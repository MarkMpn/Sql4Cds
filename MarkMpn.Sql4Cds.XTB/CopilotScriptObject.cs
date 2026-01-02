using System;
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
using Markdig;
using Markdig.Syntax;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Extensions.AI;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Web.WebView2.WinForms;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;
using OpenAI.Chat;
using QuikGraph;
using QuikGraph.Algorithms;
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
        private readonly Dictionary<string, string> _pendingQueries;
        private readonly List<ChatMessage> _messages;
        private readonly Dictionary<string, AIFunction> _tools;
        private readonly ChatOptions _options;
        private IChatClient _chatClient;
        private string _lastMessage;
        private bool _runningQuery;
        private List<FunctionResultContent> _toolOutputs;
        private TimeSpan _toolDelay;
        private CancellationTokenSource _cts;
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

            var aiFunctions = new AIFunctions(_control);
            _options.Tools = new List<AITool>(aiFunctions.GetTools());
            _options.Tools.Add(AIFunctionFactory.Create((Func<string>)GetCurrentQuery, name: "get_current_query", serializerOptions: aiFunctions.JsonSerializerOptions));
            _options.Tools.Add(AIFunctionFactory.Create((Func<string, Task<string[]>>)ExecuteQueryAsync, name: "execute_query", serializerOptions: aiFunctions.JsonSerializerOptions));
            _options.Tools.Add(AIFunctionFactory.Create((Func<string, QueryValidationResult>)ValidateQuery, name: "validate_query", serializerOptions: aiFunctions.JsonSerializerOptions));
            
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
            if (_cts != null)
                _cts.Dispose();

            _cts = new CancellationTokenSource();
            _messages.Add(new ChatMessage(ChatRole.User, request));

            try
            {
                if (_chatClient == null)
                    _chatClient = Settings.Instance.CreateChatClient();

                var updates = _options.ConversationId == null
                    ? _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token)
                    : _chatClient.GetStreamingResponseAsync(_messages.Last(), _options, _cts.Token);

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
                _messages.Add(new ChatMessage(ChatRole.Tool, _toolOutputs.Cast<AIContent>().ToList()));

                var updates = _options.ConversationId == null
                    ? _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token)
                    : _chatClient.GetStreamingResponseAsync(_messages.Last(), _options, _cts.Token);

                await DoRunAsync(updates);
            }

            return null;
        }
        
        private async Task<List<string>> ExecuteInternal(string id)
        {
            if (!_pendingQueries.TryGetValue(id, out var query))
                throw new CopilotException("Unknown query ID");

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
                var runStarted = false;
                var messageText = new ConcurrentDictionary<string, string>();
                bool submittedToolOutputs;

                do
                {
                    if (!runStarted)
                    {
                        await RunStarted();
                        runStarted = true;
                    }

                    _toolOutputs = new List<FunctionResultContent>();
                    submittedToolOutputs = false;
                    var updateCache = new List<ChatResponseUpdate>();

                    await foreach (var update in updates)
                    {
                        if (_cts.IsCancellationRequested)
                            break;

                        if (_options.ConversationId == null && update.ConversationId != null)
                            _options.ConversationId = update.ConversationId;

                        foreach (var content in update.Contents)
                        {
                            if (content is FunctionCallContent func)
                            {
                                try
                                {
                                    if (!_tools.TryGetValue(func.Name, out var tool))
                                    {
                                        throw new CopilotException($"The tool '{func.Name}' is not available.");
                                    }
                                    else
                                    {
                                        _toolCallId = func.CallId;

                                        if (func.Name != "execute_query")
                                            await LogToolUsage(func.CallId, func.Name, false);

                                        var result = await tool.InvokeAsync(new AIFunctionArguments(func.Arguments), _cts.Token);

                                        if (result != null)
                                            _toolOutputs.Add(new FunctionResultContent(func.CallId, JsonSerializer.Serialize(result, tool.JsonSerializerOptions)));

                                        if (func.Name != "execute_query")
                                            await LogToolUsage(func.CallId, func.Name, true);
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
                            var validationResult = ValidateQuery(query.Lines.ToString());

                            errors.AddRange(validationResult.Errors);
                            errorHints.AddRange(validationResult.Hints);
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

                        updates = _options.ConversationId == null
                            ? _chatClient.GetStreamingResponseAsync(_messages, _options, _cts.Token)
                            : _chatClient.GetStreamingResponseAsync(_messages.Last(), _options, _cts.Token);

                        submittedToolOutputs = true;

                        await RunContinuing();
                    }
                } while (!_cts.IsCancellationRequested && submittedToolOutputs);
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
            var validationResult = ValidateQuery(query);

            if (validationResult.Errors.Count > 0)
            {
                var html = Markdown.ToHtml($"Errors found while validating a proposed query:\r\n\r\n```sql\r\n{query}\r\n```\r\n\r\n```\r\n{validationResult.Errors[0]}\r\n```\r\n\r\nRetrying...", _markdownPipeline);
                await ShowPromptSuggestionAsync("warning", html, null, null);

                var prompt = $"This query contains the following errors:\r\n{String.Join("\r\n", validationResult.Errors.Select(e => $"* {e}"))}";

                if (validationResult.Hints.Count > 0)
                    prompt += "\r\n\r\nTry the following changes:\r\n" + String.Join("\r\n", validationResult.Hints.Select(h => $"* {h}"));

                throw new CopilotException(prompt);
            }

            // Check if the user has seen this query in the previous message
            var whitespace = new Regex(@"\s+");
            var seenInLastMessage = _lastMessage != null && whitespace.Replace(_lastMessage, " ").Contains(whitespace.Replace(query, " "));

            if (Settings.Instance.AllowCopilotSelectQueries)
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

        [Description("Checks if a query is valid")]
        private QueryValidationResult ValidateQuery([Description("The query to check")] string query)
        {
            var result = new QueryValidationResult();

            try
            {
                _control.ValidateQuery(query);
            }
            catch (Sql4CdsException ex)
            {
                result.Errors.AddRange(ex.Errors.Select(e => e.Message));

                var error = (Exception)ex;
                while (error.InnerException != null)
                {
                    if (error is NotSupportedQueryFragmentException nsqfe &&
                        nsqfe.Suggestion != null)
                    {
                        result.Errors.Add(nsqfe.Suggestion);
                    }

                    error = error.InnerException;
                }

                var showListTablesHint = ex.Errors.Any(e => e.Number == 208);
                var showGetColumnsInTableHint = ex.Errors.Any(e => e.Number == 207);
                var showFindRelationshipHint = showGetColumnsInTableHint && ContainsJoin(query);

                if (showListTablesHint)
                    result.Hints.Add("To get a list of valid table names, use the `list_tables` function");
                if (showGetColumnsInTableHint)
                    result.Hints.Add("To get a list of valid column names in a table, use the `get_columns_in_table` function");
                if (showFindRelationshipHint)
                    result.Hints.Add("If you need to find how to join two tables together, try the `find_relationship` function");
            }

            return result;
        }

        class QueryValidationResult
        {
            public List<string> Errors { get; } = new List<string>();

            public List<string> Hints { get; } = new List<string>();
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

        private async Task RunStarted()
        {
            await _copilotWebView.ExecuteScriptAsync("runStarted()");
        }

        private async Task RunContinuing()
        {
            await _copilotWebView.ExecuteScriptAsync("runContinuing()");
        }

        private async Task Finished()
        {
            await _copilotWebView.ExecuteScriptAsync("finished()");
        }

        private async Task RunningQuery()
        {
            await _copilotWebView.ExecuteScriptAsync("runningQuery()");
        }

        private async Task LogToolUsage(string callId, string tool, bool complete)
        {
            await _copilotWebView.ExecuteScriptAsync("updateMessage(" + JsonSerializer.Serialize(callId) + "," + JsonSerializer.Serialize($"<p>Executing tool <code>{tool}</code>{(complete ? "✅" : "⌛")}</p>") + ")");
        }
    }
}

#pragma warning restore OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.