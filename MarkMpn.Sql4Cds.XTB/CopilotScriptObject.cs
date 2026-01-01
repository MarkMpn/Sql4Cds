using System;
using System.ClientModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Anthropic;
using Azure.AI.OpenAI;
using Markdig;
using Markdig.Syntax;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Extensions.AI;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Web.WebView2.WinForms;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;
using OpenAI.Assistants;
using OpenAI.Chat;
using QuikGraph;
using QuikGraph.Algorithms;
using XrmToolBox.Extensibility;
using ChatFinishReason = Microsoft.Extensions.AI.ChatFinishReason;
using ChatMessage = Microsoft.Extensions.AI.ChatMessage;

#pragma warning disable OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.

namespace MarkMpn.Sql4Cds.XTB
{
    [ClassInterface(ClassInterfaceType.AutoDual)]
    [ComVisible(true)]
    public class CopilotScriptObject
    {
        private readonly SqlQueryControl _control;
        private readonly WebView2 _copilotWebView;
        private readonly MarkdownPipeline _markdownPipeline;
        private IChatClient _chatClient;
        private string _lastMessage;
        private bool _runningQuery;
        private Dictionary<string, string> _pendingQueries;
        private List<FunctionResultContent> _toolOutputs;
        private TimeSpan _toolDelay;
        private CancellationTokenSource _cts;
        private List<ChatMessage> _messages;
        private ChatOptions _options;
        private Dictionary<string, AIFunction> _tools;
        private string _toolCallId;

        internal CopilotScriptObject(SqlQueryControl control, WebView2 copilotWebView)
        {
            _markdownPipeline = new MarkdownPipelineBuilder()
                .UseAdvancedExtensions()
                .UseSqlCodeBlockHandling()
                .Build();
            _control = control;
            _copilotWebView = copilotWebView;
            _pendingQueries = new Dictionary<string, string>();
            _toolDelay = TimeSpan.FromSeconds(0.5);
            _messages = new List<ChatMessage>();

            string systemPrompt;

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.XTB.Resources.CopilotInstructions.txt"))
            using (var reader = new StreamReader(stream))
            {
                systemPrompt = reader.ReadToEnd();
            }

            _messages.Add(new ChatMessage(ChatRole.System, systemPrompt));

            _options = new ChatOptions();
            _options.RawRepresentationFactory = _ => new ChatCompletionOptions { ReasoningEffortLevel = ChatReasoningEffortLevel.Low };
            var jsonSerializerOptions = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                TypeInfoResolver = new DefaultJsonTypeInfoResolver()
            };

            _options.Tools = new List<AITool>
            {
                AIFunctionFactory.Create((Func<IDictionary<string,ListTableResult>>)ListTables, name: "list_tables", serializerOptions: jsonSerializerOptions),
                AIFunctionFactory.Create((Func<string, IDictionary<string,ColumnListResult>>)GetColumnsInTable, name: "get_columns_in_table", serializerOptions: jsonSerializerOptions),
                AIFunctionFactory.Create((Func<string>)GetCurrentQuery, name: "get_current_query", serializerOptions: jsonSerializerOptions),
                AIFunctionFactory.Create((Func<string, string, Relationship[]>)FindRelationship, name: "find_relationship", serializerOptions: jsonSerializerOptions),
                AIFunctionFactory.Create((Func<string,Task<string[]>>)ExecuteQueryAsync, name: "execute_query", serializerOptions: jsonSerializerOptions)
            };
            _options.AllowMultipleToolCalls = true;

            _tools = _options.Tools.Cast<AIFunction>().ToDictionary(t => t.Name);
        }

        public void Cancel()
        {
            if (_cts != null)
            {
                _cts.Cancel();
                if (_runningQuery && _control.Cancellable)
                    _control.Cancel();
                _pendingQueries.Clear();
            }
        }

        public async Task<string[]> SendMessage(string request)
        {
            _cts = new CancellationTokenSource();
            _messages.Add(new ChatMessage(ChatRole.User, request));

            try
            {
                if (_chatClient == null)
                {
                    if (!String.IsNullOrEmpty(Settings.Instance.OpenAIEndpoint))
                    {
                        _chatClient = new AzureOpenAIClient(new Uri(Settings.Instance.OpenAIEndpoint), new ApiKeyCredential(Settings.Instance.OpenAIKey))
                            .GetChatClient(Settings.Instance.OpenAIModel)
                            .AsIChatClient();
                    }
                    else if (Settings.Instance.OpenAIModel == "Anthropic")
                    {
                        _chatClient = new AnthropicClient(Settings.Instance.OpenAIKey);
                    }
                    else
                    {
                        _chatClient = new OpenAI.Chat.ChatClient(Settings.Instance.OpenAIModel, new ApiKeyCredential(Settings.Instance.OpenAIKey)).AsIChatClient();
                    }

                    _chatClient.AsBuilder()
                        .ConfigureOptions(options =>
                        {
                            options.ModelId = Settings.Instance.OpenAIModel;
                        })
                        .Build();
                }

                var updates = _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token);
                await DoRunAsync(updates);
            }
            catch (Exception ex)
            {
                await ShowPromptSuggestionAsync("warning", HttpUtility.HtmlEncode(ex.Message), null, null);
            }

            return null;
        }
        
        public async Task<string[]> ExecuteQuery(string id, bool executeAllowed)
        {
            if (!executeAllowed)
            {
                _toolOutputs.Add(new FunctionResultContent(id, JsonSerializer.Serialize(new { status = "Canceled", error = "User rejected running this query. Abort this process and await further input." })));
            }
            else
            {
                var results = await ExecuteInternal(id);

                if (results.Count == 1)
                    _toolOutputs.Add(new FunctionResultContent(id, results[0]));
                else
                    _toolOutputs.Add(new FunctionResultContent(id, JsonSerializer.Serialize(results)));
            }

            if (_pendingQueries.Count == 0 && !_cts.IsCancellationRequested)
            {
                // We've collected all the required query results, submit them to the assistant
                var updates = _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token);
                await DoRunAsync(updates);
            }

            return null;
        }
        
        private async Task<List<string>> ExecuteInternal(string id)
        {
            if (!_pendingQueries.TryGetValue(id, out var query))
                throw new ApplicationException("Unknown query ID");

            await RunningQuery();

            _runningQuery = true;
            var results = await Task.Run(() => _control.Execute(query));
            _runningQuery = false;
            _copilotWebView.Focus();

            _pendingQueries.Remove(id);
            return results;
        }
        
        public async Task<string[]> Retry()
        {
            var updates = _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token);
            await DoRunAsync(updates);
            return null;
        }
        
        private async Task DoRunAsync(IAsyncEnumerable<ChatResponseUpdate> updates)
        {
            Func<Task> promptSuggestion = null;

            try
            { 
                var dataSource = _control.DataSources[_control.Connection.ConnectionName];
                var messageText = new ConcurrentDictionary<string, string>();
                bool submittedToolOutputs;

                do
                {
                    _toolOutputs = new List<FunctionResultContent>();
                    var runStarted = false;
                    submittedToolOutputs = false;
                    var updateCache = new List<ChatResponseUpdate>();

                    await foreach (var update in updates)
                    {
                        if (_cts.IsCancellationRequested)
                            break;

                        if (!runStarted)
                        {
                            await RunStarted();
                            runStarted = true;
                        }

                        foreach (var content in update.Contents)
                        {
                            if (content is FunctionCallContent func)
                            {
                                try
                                {
                                    if (!_tools.TryGetValue(func.Name, out var tool))
                                    {
                                        throw new ApplicationException($"The tool '{func.Name}' is not available.");
                                    }
                                    else
                                    {
                                        _toolCallId = func.CallId;

                                        var result = await tool.InvokeAsync(new AIFunctionArguments(func.Arguments), _cts.Token);

                                        if (result != null)
                                            _toolOutputs.Add(new FunctionResultContent(func.CallId, JsonSerializer.Serialize(result, tool.JsonSerializerOptions)));
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _toolOutputs.Add(new FunctionResultContent(func.CallId, JsonSerializer.Serialize(new { success = false, error = ex.Message })));
                                }
                            }
                            else if (content is TextContent messageContentUpdate)
                            {
                                var text = messageText.AddOrUpdate(update.MessageId, messageContentUpdate.Text, (_, existing) => existing + messageContentUpdate.Text);
                                _lastMessage = text;

                                var html = Markdown.ToHtml(text, _markdownPipeline);
                                await ShowMessageAsync(update.MessageId, html);
                            }
                        }

                        updateCache.Add(update);
                        /*
                        if (update is RunUpdate runUpdate)
                        {
                            if (_run == null)
                                await RunStarted();

                            _run = runUpdate;
                        }
                        else if (update is RequiredActionUpdate func)
                        {
                            promptSuggestion = null;
                            Dictionary<string, string> args;

                            try
                            {
                                args = JsonConvert.DeserializeObject<Dictionary<string, string>>(func.FunctionArguments);
                            }
                            catch (JsonException)
                            {
                                if (func.FunctionName == "execute_query")
                                {
                                    // Query is sometimes presented directly rather than wrapped in JSON
                                    args = new Dictionary<string, string>
                                    {
                                        ["query"] = func.FunctionArguments
                                    };
                                }
                                else
                                {
                                    throw;
                                }
                            }

                            switch (func.FunctionName)
                            {
                                case "list_tables":
                                    var entities = dataSource.Metadata.GetAllEntities();
                                    var response = entities.ToDictionary(e => e.LogicalName, e => new { displayName = e.DisplayName?.UserLocalizedLabel?.Label, description = e.Description?.UserLocalizedLabel?.Label });
                                    _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(response, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })));
                                    break;

                                case "get_columns_in_table":
                                    var entity = args["table_name"];
                                    dataSource.Metadata.TryGetValue(entity, out var metadata);

                                    if (metadata == null)
                                    {
                                        _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(new { success = false, error = $"The table '{entity}' does not exist in this environment" })));
                                    }
                                    else
                                    {
                                        var columns = metadata.Attributes.ToDictionary(a => a.LogicalName, a => new { displayName = a.DisplayName?.UserLocalizedLabel?.Label, description = ShowDescription(a) ? a.Description?.UserLocalizedLabel?.Label : null, type = a.AttributeTypeName?.Value, options = (a as EnumAttributeMetadata)?.OptionSet?.Options?.ToDictionary(o => o.Value, o => o.Label?.UserLocalizedLabel?.Label), lookupTo = (a as LookupAttributeMetadata)?.Targets?.Select(target => target + "." + dataSource.Metadata[target].PrimaryIdAttribute)?.ToArray() });
                                        _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(columns, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })));
                                    }
                                    break;

                                case "get_current_query":
                                    _toolOutputs.Add(new ToolOutput(func.ToolCallId, _control.Sql));
                                    break;

                                case "execute_query":
                                    var query = args["query"];
                                    var executeAllowed = false;

                                    // Validate the query first
                                    try
                                    {
                                        _control.ValidateQuery(query);
                                    }
                                    catch (Sql4CdsException ex)
                                    {
                                        var errors = ex.Errors.Select(e => e.Message).ToList();

                                        var error = (Exception)ex;
                                        while (error.InnerException != null)
                                        {
                                            if (error is NotSupportedQueryFragmentException nsqfe &&
                                                nsqfe.Suggestion != null)
                                            {
                                                errors.Add(nsqfe.Suggestion);
                                            }

                                            error = error.InnerException;
                                        }

                                        var html = Markdown.ToHtml($"Errors found while validating a proposed query:\r\n\r\n```sql\r\n{query}\r\n```\r\n\r\n```\r\n{errors[0]}\r\n```\r\n\r\nRetrying...", _markdownPipeline);
                                        await ShowPromptSuggestionAsync("warning", html, null, null);

                                        var showListTablesHint = ex.Errors.Any(e => e.Number == 208);
                                        var showGetColumnsInTableHint = ex.Errors.Any(e => e.Number == 207);
                                        var showFindRelationshipHint = showGetColumnsInTableHint && ContainsJoin(query);
                                        var errorHints = new List<string>();

                                        if (showListTablesHint)
                                            errorHints.Add("To get a list of valid table names, use the `list_tables` function");
                                        if (showGetColumnsInTableHint)
                                            errorHints.Add("To get a list of valid column names in a table, use the `get_columns_in_table` function");
                                        if (showFindRelationshipHint)
                                            errorHints.Add("If you need to find how to join two tables together, try the `find_relationship` function");

                                        var prompt = $"This query contains the following errors:\r\n{String.Join("\r\n", errors.Select(e => $"* {e}"))}";

                                        if (errorHints.Any())
                                            prompt += "\r\n\r\nTry the following changes:\r\n" + String.Join("\r\n", errorHints.Select(h => $"* {h}"));

                                        _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(new { success = false, error = prompt })));

                                        break;
                                    }

                                    // Check if the user has seen this query in the previous message
                                    var whitespace = new Regex(@"\s+");
                                    var seenInLastMessage = _lastMessage != null && whitespace.Replace(_lastMessage, " ").Contains(whitespace.Replace(query, " "));
                                    
                                    if (!executeAllowed && Settings.Instance.AllowCopilotSelectQueries)
                                    {
                                        // Check if unprompted execution is allowed - must be a single SELECT query only
                                        var parser = new TSql160Parser(Settings.Instance.QuotedIdentifiers);
                                        var result = parser.Parse(new StringReader(query), out var errors);

                                        if (result is TSqlScript script &&
                                            script.Batches.Count == 1 &&
                                            script.Batches[0].Statements.Count == 1 &&
                                            script.Batches[0].Statements[0] is SelectStatement)
                                        {
                                            executeAllowed = true;

                                            if (!seenInLastMessage)
                                            {
                                                // Show a message to the user so they know what query is being executed
                                                var markdown = "Executing the query:\n\n```sql\n" + query + "\n```";
                                                var html = Markdown.ToHtml(markdown, _markdownPipeline);
                                                await ShowMessageAsync(func.ToolCallId, html);
                                            }
                                        }
                                    }

                                    if (!executeAllowed)
                                    {
                                        // Check if the user wants to run this query
                                        var markdown = seenInLastMessage ? "Do you want to run this query?" : $"Do you want to run the query:\n\n```sql\n{query}\n```";
                                        var html = Markdown.ToHtml(markdown, _markdownPipeline);
                                        await ShowExecutePromptAsync(html, func.ToolCallId);
                                    }

                                    _pendingQueries.Add(func.ToolCallId, query);

                                    if (executeAllowed)
                                        await ExecuteInternal(func.ToolCallId, true);
                                    else
                                        return;
                                    break;

                                case "find_relationship":
                                    var table1 = args["table1"];
                                    var table2 = args["table2"];

                                    if (!dataSource.Metadata.TryGetValue(table1, out var t1) ||
                                        !dataSource.Metadata.TryGetValue(table2, out var t2))
                                    {
                                        _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(new { success = false, error = "One or both of the tables do not exist in this environment. Use the list_tables function to get the valid table names." })));
                                    }
                                    else
                                    {
                                        // Build the graph of relationships between each entity
                                        var graph = new UndirectedGraph<string, TaggedEdge<string, string>>();

                                        foreach (var e in dataSource.Metadata.GetAllEntities())
                                            graph.AddVertex(e.LogicalName);

                                        // Some entities have links to almost everything else - ignore them unless they're one of the entities we're interested in
                                        var ignoreEntities = new[] { "organization", "systemuser", "team", "queue", "businessunit", "asyncoperation", "userentityinstancedata" };

                                        foreach (var e in dataSource.Metadata.GetAllEntities())
                                        {
                                            if (ignoreEntities.Contains(e.LogicalName) && t1.LogicalName != e.LogicalName && t2.LogicalName != e.LogicalName)
                                                continue;

                                            foreach (var a in e.Attributes.OfType<LookupAttributeMetadata>())
                                            {
                                                foreach (var target in a.Targets)
                                                {
                                                    if (ignoreEntities.Contains(target) && t1.LogicalName != target && t2.LogicalName != target)
                                                        continue;

                                                    graph.AddEdge(new TaggedEdge<string, string>(e.LogicalName, target, a.LogicalName));
                                                }
                                            }
                                        }

                                        var paths = graph.ShortestPathsDijkstra(e => 1, table1);

                                        if (paths(table2, out var path))
                                        {
                                            _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(path.Select(e => new { fromTable = e.Source, fromColumn = e.Tag, toTable = e.Target, toColumn = dataSource.Metadata[e.Target].PrimaryIdAttribute }).ToArray())));
                                        }
                                        else
                                        {
                                            _toolOutputs.Add(new ToolOutput(func.ToolCallId, JsonConvert.SerializeObject(new { success = false, error = "There is no set of relationships that link these two tables" })));
                                        }
                                    }
                                    break;

                                default:
                                    break;
                            }
                        }
                        else if (update is MessageContentUpdate messageContentUpdate)
                        {
                            var text = messageText.AddOrUpdate(messageContentUpdate.MessageId, messageContentUpdate.Text, (_, existing) => existing + messageContentUpdate.Text);
                            _lastMessage = text;

                            var html = Markdown.ToHtml(text, _markdownPipeline);
                            await ShowMessageAsync(messageContentUpdate.MessageId, html);
                        }
                        else if (update is MessageStatusUpdate messageStatusUpdate)
                        {
                            if (messageStatusUpdate.UpdateKind != StreamingUpdateReason.MessageCompleted)
                            {
                                promptSuggestion = null;
                            }
                            else
                            {
                                var text = messageText[messageStatusUpdate.Value.Id];

                                // If there are any SQL queries in this message, validate them now
                                var queries = Markdown.Parse(text)
                                    .OfType<FencedCodeBlock>()
                                    .Where(c => c.Info == "sql");

                                var errors = new List<string>();
                                var errorHints = new List<string>();

                                foreach (var query in queries)
                                {
                                    try
                                    {
                                        _control.ValidateQuery(query.Lines.ToString());
                                    }
                                    catch (Sql4CdsException ex)
                                    {
                                        errors.AddRange(ex.Errors.Select(e => e.Message));

                                        var error = (Exception)ex;
                                        while (error.InnerException != null)
                                        {
                                            if (error is NotSupportedQueryFragmentException nsqfe &&
                                                nsqfe.Suggestion != null)
                                            {
                                                errors.Add(nsqfe.Suggestion);
                                            }

                                            error = error.InnerException;
                                        }

                                        var showListTablesHint = ex.Errors.Any(e => e.Number == 208);
                                        var showGetColumnsInTableHint = ex.Errors.Any(e => e.Number == 207);
                                        var showFindRelationshipHint = showGetColumnsInTableHint && ContainsJoin(query.Lines.ToString());

                                        if (showListTablesHint)
                                            errorHints.Add("To get a list of valid table names, use the `list_tables` function");
                                        if (showGetColumnsInTableHint)
                                            errorHints.Add("To get a list of valid column names in a table, use the `get_columns_in_table` function");
                                        if (showFindRelationshipHint)
                                            errorHints.Add("If you need to find how to join two tables together, try the `find_relationship` function");
                                    }
                                }

                                if (errors.Any())
                                {
                                    var html = Markdown.ToHtml($"The suggested query contains errors:\r\n```\r\n{errors[0]}\r\n```", _markdownPipeline);
                                    var prompt = $"This query contains the following errors:\r\n{String.Join("\r\n", errors.Select(e => $"* {e}"))}";

                                    if (errorHints.Any())
                                        prompt += "\r\n\r\nTry the following changes:\r\n" + String.Join("\r\n", errorHints.Select(h => $"* {h}"));

                                    promptSuggestion = () => ShowPromptSuggestionAsync("warning", html, "Retry", prompt);
                                }
                                else if (queries.Count() == 1)
                                {
                                    promptSuggestion = () => ShowPromptSuggestionAsync("info", "", "Run this query", "Run this query");
                                }
                            }
                        }
                        else
                        {
                            //throw new NotSupportedException();
                        }

                        if (_toolOutputs.Any() && _pendingQueries.Count == 0 && !_canceled)
                        {
                            // Sleep to avoid overloading the API with sequential function calls
                            await Task.Delay(_toolDelay);

                            updates = _assistantClient.SubmitToolOutputsToRunStreamingAsync(_run, _toolOutputs);
                        }*/
                    }

                    // Add these messages to the chat history
                    await _messages.AddMessagesAsync(updateCache.ToAsyncEnumerable());

                    // Validate any new messages
                    foreach (var messageId in updateCache.Select(u => u.MessageId).Distinct())
                    {
                        if (!messageText.TryGetValue(messageId, out var text))
                            continue;

                        // If there are any SQL queries in this message, validate them now
                        var queries = Markdown.Parse(text)
                            .OfType<FencedCodeBlock>()
                            .Where(c => c.Info == "sql");

                        var errors = new List<string>();
                        var errorHints = new List<string>();

                        foreach (var query in queries)
                        {
                            try
                            {
                                _control.ValidateQuery(query.Lines.ToString());
                            }
                            catch (Sql4CdsException ex)
                            {
                                errors.AddRange(ex.Errors.Select(e => e.Message));

                                var error = (Exception)ex;
                                while (error.InnerException != null)
                                {
                                    if (error is NotSupportedQueryFragmentException nsqfe &&
                                        nsqfe.Suggestion != null)
                                    {
                                        errors.Add(nsqfe.Suggestion);
                                    }

                                    error = error.InnerException;
                                }

                                var showListTablesHint = ex.Errors.Any(e => e.Number == 208);
                                var showGetColumnsInTableHint = ex.Errors.Any(e => e.Number == 207);
                                var showFindRelationshipHint = showGetColumnsInTableHint && ContainsJoin(query.Lines.ToString());

                                if (showListTablesHint)
                                    errorHints.Add("To get a list of valid table names, use the `list_tables` function");
                                if (showGetColumnsInTableHint)
                                    errorHints.Add("To get a list of valid column names in a table, use the `get_columns_in_table` function");
                                if (showFindRelationshipHint)
                                    errorHints.Add("If you need to find how to join two tables together, try the `find_relationship` function");
                            }
                        }

                        if (errors.Any())
                        {
                            var html = Markdown.ToHtml($"The suggested query contains errors:\r\n```\r\n{errors[0]}\r\n```", _markdownPipeline);
                            var prompt = $"This query contains the following errors:\r\n{String.Join("\r\n", errors.Select(e => $"* {e}"))}";

                            if (errorHints.Any())
                                prompt += "\r\n\r\nTry the following changes:\r\n" + String.Join("\r\n", errorHints.Select(h => $"* {h}"));

                            promptSuggestion = () => ShowPromptSuggestionAsync("warning", html, "Retry", prompt);
                        }
                        else if (queries.Count() == 1)
                        {
                            promptSuggestion = () => ShowPromptSuggestionAsync("info", "", "Run this query", "Run this query");
                        }
                    }

                    if (_toolOutputs.Any() && _pendingQueries.Count == 0 && !_cts.IsCancellationRequested)
                    {
                        // Sleep to avoid overloading the API with sequential function calls
                        await Task.Delay(_toolDelay);

                        _messages.Add(new ChatMessage(ChatRole.Tool, _toolOutputs.Cast<AIContent>().ToList()));
                        updates = _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token);
                        submittedToolOutputs = true;
                    }
                } while (!_cts.IsCancellationRequested && submittedToolOutputs);

                /*
                if (_run?.LastError != null)
                {
                    if (_run.LastError.Code == RunErrorCode.RateLimitExceeded)
                        _toolDelay = TimeSpan.FromSeconds(_toolDelay.TotalSeconds * 2);

                    await ShowRetryAsync(HttpUtility.HtmlEncode(_run.LastError.Message));
                }*/
            }
            catch (Exception ex)
            {
                await ShowPromptSuggestionAsync("warning", HttpUtility.HtmlEncode(ex.Message), null, null);
            }
            finally
            {
                if (promptSuggestion != null)
                    await promptSuggestion();

                if (_pendingQueries.Count == 0)
                {
                    await Finished();
                }
            }
        }

        [Description("Get the list of tables in the current environment")]
        private IDictionary<string,ListTableResult> ListTables()
        {
            var dataSource = _control.DataSources[_control.Connection.ConnectionName];
            var entities = dataSource.Metadata.GetAllEntities();
            var response = entities.ToDictionary(e => e.LogicalName, e => new ListTableResult { displayName = e.DisplayName?.UserLocalizedLabel?.Label, description = e.Description?.UserLocalizedLabel?.Label });
            return response;
        }

        class ListTableResult
        {
            public string displayName { get; set; }
            public string description { get; set; }
        }

        [Description("Get the list of columns in a table, including the available values for optionset columns and the relationships for foreign key columns")]
        private Dictionary<string, ColumnListResult> GetColumnsInTable([Description("The name of the table, e.g. 'account'")] string table_name)
        {
            var dataSource = _control.DataSources[_control.Connection.ConnectionName];
            dataSource.Metadata.TryGetValue(table_name, out var metadata);

            if (metadata == null)
                throw new ApplicationException($"The table '{table_name}' does not exist in this environment");

            var columns = metadata.Attributes.ToDictionary(a => a.LogicalName, a => new ColumnListResult { displayName = a.DisplayName?.UserLocalizedLabel?.Label, description = ShowDescription(a) ? a.Description?.UserLocalizedLabel?.Label : null, type = a.AttributeTypeName?.Value, options = (a as EnumAttributeMetadata)?.OptionSet?.Options?.ToDictionary(o => o.Value.Value, o => o.Label?.UserLocalizedLabel?.Label), lookupTo = (a as LookupAttributeMetadata)?.Targets?.Select(target => target + "." + dataSource.Metadata[target].PrimaryIdAttribute)?.ToArray() });
            return columns;
        }

        class ColumnListResult
        {
            public string displayName { get; set; }
            public string description { get; set; }
            public string type { get; set; }
            public Dictionary<int,string> options { get; set; }
            public string[] lookupTo { get; set; }
        }

        [Description("Gets the contents of the query the user is currently editing")]
        private string GetCurrentQuery()
        {
            return _control.Sql;
        }

        [Description("Runs a SQL query and returns the result. Only queries which have previously been shown to the user will be run")]
        private async Task<string[]> ExecuteQueryAsync([Description("The SQL query to execute")] string query)
        {
            var executeAllowed = false;

            // Validate the query first
            try
            {
                _control.ValidateQuery(query);
            }
            catch (Sql4CdsException ex)
            {
                var errors = ex.Errors.Select(e => e.Message).ToList();

                var error = (Exception)ex;
                while (error.InnerException != null)
                {
                    if (error is NotSupportedQueryFragmentException nsqfe &&
                        nsqfe.Suggestion != null)
                    {
                        errors.Add(nsqfe.Suggestion);
                    }

                    error = error.InnerException;
                }

                var html = Markdown.ToHtml($"Errors found while validating a proposed query:\r\n\r\n```sql\r\n{query}\r\n```\r\n\r\n```\r\n{errors[0]}\r\n```\r\n\r\nRetrying...", _markdownPipeline);
                await ShowPromptSuggestionAsync("warning", html, null, null);

                var showListTablesHint = ex.Errors.Any(e => e.Number == 208);
                var showGetColumnsInTableHint = ex.Errors.Any(e => e.Number == 207);
                var showFindRelationshipHint = showGetColumnsInTableHint && ContainsJoin(query);
                var errorHints = new List<string>();

                if (showListTablesHint)
                    errorHints.Add("To get a list of valid table names, use the `list_tables` function");
                if (showGetColumnsInTableHint)
                    errorHints.Add("To get a list of valid column names in a table, use the `get_columns_in_table` function");
                if (showFindRelationshipHint)
                    errorHints.Add("If you need to find how to join two tables together, try the `find_relationship` function");

                var prompt = $"This query contains the following errors:\r\n{String.Join("\r\n", errors.Select(e => $"* {e}"))}";

                if (errorHints.Any())
                    prompt += "\r\n\r\nTry the following changes:\r\n" + String.Join("\r\n", errorHints.Select(h => $"* {h}"));

                throw new ApplicationException(prompt);
            }

            // Check if the user has seen this query in the previous message
            var whitespace = new Regex(@"\s+");
            var seenInLastMessage = _lastMessage != null && whitespace.Replace(_lastMessage, " ").Contains(whitespace.Replace(query, " "));

            if (!executeAllowed && Settings.Instance.AllowCopilotSelectQueries)
            {
                // Check if unprompted execution is allowed - must be a single SELECT query only
                var parser = new TSql160Parser(Settings.Instance.QuotedIdentifiers);
                var result = parser.Parse(new StringReader(query), out var errors);

                if (result is TSqlScript script &&
                    script.Batches.Count == 1 &&
                    script.Batches[0].Statements.Count == 1 &&
                    script.Batches[0].Statements[0] is SelectStatement)
                {
                    executeAllowed = true;

                    if (!seenInLastMessage)
                    {
                        // Show a message to the user so they know what query is being executed
                        var markdown = "Executing the query:\n\n```sql\n" + query + "\n```";
                        var html = Markdown.ToHtml(markdown, _markdownPipeline);
                        await ShowMessageAsync(_toolCallId, html);
                    }
                }
            }

            if (!executeAllowed)
            {
                // Check if the user wants to run this query
                var markdown = seenInLastMessage ? "Do you want to run this query?" : $"Do you want to run the query:\n\n```sql\n{query}\n```";
                var html = Markdown.ToHtml(markdown, _markdownPipeline);
                await ShowExecutePromptAsync(html, _toolCallId);
            }

            _pendingQueries.Add(_toolCallId, query);

            if (executeAllowed)
            {
                var results = await ExecuteInternal(_toolCallId);
                return results.ToArray();
            }

            return null;
        }

        [Description("Finds the path of joins that can be used to connect two tables")]
        private Relationship[] FindRelationship([Description("The table to join from")] string table1, [Description("The table to join to")] string table2)
        {
            var dataSource = _control.DataSources[_control.Connection.ConnectionName];

            if (!dataSource.Metadata.TryGetValue(table1, out var t1))
                throw new ApplicationException($"The table '{table1}' does not exist in this environment");

            if (!dataSource.Metadata.TryGetValue(table2, out var t2))
                throw new ApplicationException($"The table '{table2}' does not exist in this environment");

            // Build the graph of relationships between each entity
            var graph = new UndirectedGraph<string, TaggedEdge<string, string>>();

            foreach (var e in dataSource.Metadata.GetAllEntities())
                graph.AddVertex(e.LogicalName);

            // Some entities have links to almost everything else - ignore them unless they're one of the entities we're interested in
            var ignoreEntities = new[] { "organization", "systemuser", "team", "queue", "businessunit", "asyncoperation", "userentityinstancedata" };

            foreach (var e in dataSource.Metadata.GetAllEntities())
            {
                if (ignoreEntities.Contains(e.LogicalName) && t1.LogicalName != e.LogicalName && t2.LogicalName != e.LogicalName)
                    continue;

                foreach (var a in e.Attributes.OfType<LookupAttributeMetadata>())
                {
                    foreach (var target in a.Targets)
                    {
                        if (ignoreEntities.Contains(target) && t1.LogicalName != target && t2.LogicalName != target)
                            continue;

                        graph.AddEdge(new TaggedEdge<string, string>(e.LogicalName, target, a.LogicalName));
                    }
                }
            }

            var paths = graph.ShortestPathsDijkstra(e => 1, table1);

            if (!paths(table2, out var path))
                throw new ApplicationException("There is no set of relationships that link these two tables");
            
            return path.Select(e => new Relationship { fromTable = e.Source, fromColumn = e.Tag, toTable = e.Target, toColumn = dataSource.Metadata[e.Target].PrimaryIdAttribute }).ToArray();
        }

        class Relationship
        {
            public string fromTable { get; set; }
            public string fromColumn { get; set; }
            public string toTable { get; set; }
            public string toColumn { get; set; }
        }

        private bool ShowDescription(AttributeMetadata a)
        {
            // Check if the description should be output to the API. Some common attributes are not useful
            // or are already understood by the model and can be skipped to reduce the required tokens
            switch (a.LogicalName)
            {
                case "createdby":
                case "createdon":
                case "createdonbehalfby":
                case "modifiedby":
                case "modifiedon":
                case "modifiedonbehalfby":
                case "owningbusinessunit":
                case "owningteam":
                case "owninguser":
                case "ownerid":
                case "transactioncurrencyid":
                case "versionnumber":
                case "importsequencenumber":
                case "overriddencreatedon":
                case "statecode":
                case "statuscode":
                case "timezoneruleversionnumber":
                case "utcconversiontimezonecode":
                    return false;
            }

            return true;
        }

        private bool ContainsJoin(string sql)
        {
            var parsed = new TSql160Parser(Settings.Instance.QuotedIdentifiers).Parse(new StringReader(sql), out _);
            var visitor = new FindJoinVisitor();
            parsed.Accept(visitor);
            return visitor.ContainsJoin;
        }

        class FindJoinVisitor : TSqlFragmentVisitor
        {
            public bool ContainsJoin { get; private set; }

            public override void Visit(QualifiedJoin node)
            {
                base.Visit(node);
                ContainsJoin = true;
            }
        }

        private async Task ShowMessageAsync(string id, string html)
        {
            await _copilotWebView.ExecuteScriptAsync("updateMessage(" + JsonSerializer.Serialize(id) + "," + JsonSerializer.Serialize(html) + ")");
        }

        private async Task ShowPromptSuggestionAsync(string type, string title, string action, string message)
        {
            await _copilotWebView.ExecuteScriptAsync("showPromptSuggestion(" + JsonSerializer.Serialize(type) + "," + JsonSerializer.Serialize(title) + "," + JsonSerializer.Serialize(action) + "," + JsonSerializer.Serialize(message) + ")");
        }

        private async Task ShowExecutePromptAsync(string html, string id)
        {
            await _copilotWebView.ExecuteScriptAsync("showExecutePrompt(" + JsonSerializer.Serialize(html) + "," + JsonSerializer.Serialize(id) + ")");
        }

        private async Task ShowRetryAsync(string html)
        {
            await _copilotWebView.ExecuteScriptAsync("showRetryPrompt(" + JsonSerializer.Serialize(html) + ")");
        }

        private async Task RunStarted()
        {
            await _copilotWebView.ExecuteScriptAsync("runStarted()");
        }

        private async Task Finished()
        {
            await _copilotWebView.ExecuteScriptAsync("finished()");
        }

        private async Task RunningQuery()
        {
            await _copilotWebView.ExecuteScriptAsync("runningQuery()");
        }
    }
}

#pragma warning restore OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.