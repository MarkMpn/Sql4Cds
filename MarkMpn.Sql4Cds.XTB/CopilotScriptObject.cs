using System;
using System.ClientModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using Markdig;
using Markdig.Syntax;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Web.WebView2.WinForms;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json;
using OpenAI.Assistants;
using QuikGraph;
using QuikGraph.Algorithms;
using XrmToolBox.Extensibility;

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
        private AssistantClient _assistantClient;
        private Assistant _assistant;
        private AssistantThread _assistantThread;
        private ThreadRun _run;
        private string _lastMessage;
        private bool _canceled;
        private bool _runningQuery;
        private Dictionary<string, string> _pendingQueries;
        private List<ToolOutput> _toolOutputs;

        internal CopilotScriptObject(SqlQueryControl control, WebView2 copilotWebView)
        {
            _markdownPipeline = new MarkdownPipelineBuilder()
                .UseAdvancedExtensions()
                .UseSqlCodeBlockHandling()
                .Build();
            _control = control;
            _copilotWebView = copilotWebView;
            _pendingQueries = new Dictionary<string, string>();
        }

        public void Cancel()
        {
            if (_run != null)
            {
                _canceled = true;
                _assistantClient.CancelRun(_run);
                if (_runningQuery && _control.Cancellable)
                    _control.Cancel();
                _run = null;
                _pendingQueries.Clear();
            }
        }

        public async Task<string[]> SendMessage(string request)
        {
            _canceled = false;

            try
            {
                if (_assistantClient == null)
                {
                    var client = String.IsNullOrEmpty(Settings.Instance.OpenAIEndpoint) ? new OpenAI.OpenAIClient(new ApiKeyCredential(Settings.Instance.OpenAIKey)) : new Azure.AI.OpenAI.AzureOpenAIClient(new Uri(Settings.Instance.OpenAIEndpoint), new Azure.AzureKeyCredential(Settings.Instance.OpenAIKey));
                    _assistantClient = client.GetAssistantClient();

                    if (!Version.TryParse(Settings.Instance.AssistantVersion, out var assistantVersion) || assistantVersion < Assembly.GetExecutingAssembly().GetName().Version)
                    {
                        // Update the assistant definition before we try to use it
                        var definition = CreateCopilotAssistantForm.Definition;

                        await _assistantClient.ModifyAssistantAsync(Settings.Instance.AssistantID, new AssistantModificationOptions
                        {
                            Instructions = definition.Instructions,
                            DefaultTools = definition.Tools,
                        });

                        Settings.Instance.AssistantVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString();
                        SettingsManager.Instance.Save(typeof(PluginControl), Settings.Instance);
                    }
                }

                if (_assistant == null)
                    _assistant = await _assistantClient.GetAssistantAsync(Settings.Instance.AssistantID);

                if (_assistantThread == null)
                    _assistantThread = await _assistantClient.CreateThreadAsync();

                ThreadMessage message = await _assistantClient.CreateMessageAsync(
                    _assistantThread,
                    new[] { MessageContent.FromText(request) });

                var updates = _assistantClient.CreateRunStreamingAsync(_assistantThread, _assistant);
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
            await ExecuteInternal(id, executeAllowed);

            if (_pendingQueries.Count == 0 && !_canceled)
            {
                // We've collected all the required query results, submit them to the assistant
                var updates = _assistantClient.SubmitToolOutputsToRunStreamingAsync(_run, _toolOutputs);
                await DoRunAsync(updates);
            }

            return null;
        }

        private async Task ExecuteInternal(string id, bool executeAllowed)
        {
            if (!_pendingQueries.TryGetValue(id, out var query))
                return;

            await RunningQuery();

            if (!executeAllowed)
            {
                _toolOutputs.Add(new ToolOutput(id, JsonConvert.SerializeObject(new { status = "Canceled", error = "User rejected running this query. Abort this process and await further input." })));
            }
            else
            {
                _runningQuery = true;
                var results = await Task.Run(() => _control.Execute(query));
                _runningQuery = false;
                _copilotWebView.Focus();

                if (results.Count == 1)
                    _toolOutputs.Add(new ToolOutput(id, results[0]));
                else
                    _toolOutputs.Add(new ToolOutput(id, JsonConvert.SerializeObject(results)));
            }

            _pendingQueries.Remove(id);
        }

        private async Task DoRunAsync(AsyncResultCollection<StreamingUpdate> updates)
        {
            Func<Task> promptSuggestion = null;

            try
            { 
                var dataSource = _control.DataSources[_control.Connection.ConnectionName];
                var messageText = new ConcurrentDictionary<string, string>();
                
                do
                {
                    _toolOutputs = new List<ToolOutput>();

                    await foreach (var update in updates)
                    {
                        if (_canceled)
                            break;

                        if (update is RunUpdate runUpdate)
                        {
                            if (_run == null)
                                await RunStarted();

                            _run = runUpdate;
                        }
                        else if (update is RequiredActionUpdate func)
                        {
                            promptSuggestion = null;
                            var args = JsonConvert.DeserializeObject<Dictionary<string, string>>(func.FunctionArguments);

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
                                        var columns = metadata.Attributes.ToDictionary(a => a.LogicalName, a => new { displayName = a.DisplayName?.UserLocalizedLabel?.Label, description = a.Description?.UserLocalizedLabel?.Label, type = a.AttributeTypeName?.Value, options = (a as EnumAttributeMetadata)?.OptionSet?.Options?.ToDictionary(o => o.Value, o => o.Label?.UserLocalizedLabel?.Label), lookupTo = (a as LookupAttributeMetadata)?.Targets?.Select(target => target + "." + dataSource.Metadata[target].PrimaryIdAttribute)?.ToArray() });
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
                            updates = _assistantClient.SubmitToolOutputsToRunStreamingAsync(_run, _toolOutputs);
                    }
                } while (_run?.Status.IsTerminal == false);
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
                    _run = null;
                    await Finished();
                }
            }
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
            await _copilotWebView.ExecuteScriptAsync("updateMessage(" + JsonConvert.SerializeObject(id) + "," + JsonConvert.SerializeObject(html) + ")");
        }

        private async Task ShowPromptSuggestionAsync(string type, string title, string action, string message)
        {
            await _copilotWebView.ExecuteScriptAsync("showPromptSuggestion(" + JsonConvert.SerializeObject(type) + "," + JsonConvert.SerializeObject(title) + "," + JsonConvert.SerializeObject(action) + "," + JsonConvert.SerializeObject(message) + ")");
        }

        private async Task ShowExecutePromptAsync(string html, string id)
        {
            await _copilotWebView.ExecuteScriptAsync("showExecutePrompt(" + JsonConvert.SerializeObject(html) + "," + JsonConvert.SerializeObject(id) + ")");
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