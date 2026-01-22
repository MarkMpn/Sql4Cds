using System;
using System.ClientModel;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using AutocompleteMenuNS;
using Microsoft.Extensions.AI;
using OpenAI.Chat;
using ChatMessage = Microsoft.Extensions.AI.ChatMessage;
using ChatResponseFormat = Microsoft.Extensions.AI.ChatResponseFormat;

namespace MarkMpn.Sql4Cds.XTB
{
    internal class AIAutocomplete
    {
        private readonly SqlQueryControl _control;
        private readonly List<ChatMessage> _messages;
        private IChatClient _chatClient;
        private ChatOptions _chatOptions;

        private static readonly string _systemPrompt;
        private static ChatResponseFormat _responseFormat;

        static AIAutocomplete()
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.XTB.Resources.AIAutocompleteInstructions.txt"))
            using (var reader = new StreamReader(stream))
            {
                _systemPrompt = reader.ReadToEnd();
            }

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.XTB.Resources.AIAutocompleteResponseSchema.json"))
            using (var reader = new StreamReader(stream))
            {
                var json = reader.ReadToEnd();
                var jsonReader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
                var responseSchema = JsonElement.ParseValue(ref jsonReader);
                _responseFormat = ChatResponseFormat.ForJsonSchema(responseSchema);
            }
        }

        public AIAutocomplete(SqlQueryControl control)
        {
            _control = control;
            _messages = new List<ChatMessage>();
        }

        public IEnumerable<AutocompleteItem> GetSuggestions(string text, int pos)
        {
#pragma warning disable OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
            if (_chatClient == null)
                _chatClient = Settings.Instance.CreateChatClient(builder => builder.UseFunctionInvocation());

            if (_chatOptions == null)
            {
                _chatOptions = new ChatOptions();

                if (Settings.Instance.AIModel.StartsWith("gpt-5"))
                    _chatOptions.RawRepresentationFactory = _ => new ChatCompletionOptions { ReasoningEffortLevel = ChatReasoningEffortLevel.Low };

                var aiFunctions = new AIFunctions(_control);
                _chatOptions.Tools = new List<AITool>(aiFunctions.GetTools());
                _chatOptions.ResponseFormat = _responseFormat;
            }

            var prompt = text.Insert(pos + 1, "|");

            // Check if any of the previous messages also include this prompt
            ChatMessage response = null;

            for (var i = 0; i < _messages.Count; i++)
            {
                if (_messages[i].Role == ChatRole.User && ((TextContent)_messages[i].Contents[0]).Text == prompt)
                {
                    response = _messages[i + 1];
                    break;
                }
            }

            if (response == null)
            {
                _messages.Clear();
                _messages.Add(new ChatMessage(ChatRole.System, _systemPrompt));
                _messages.Add(new ChatMessage(ChatRole.User, prompt));
                try
                {
                    response = Task.Run(async () => _chatClient.GetResponseAsync(_messages, _chatOptions)).Unwrap().ConfigureAwait(false).GetAwaiter().GetResult().Messages.Last();
                    _messages.Add(response);
                }
                catch (Exception ex)
                {
                    _messages.RemoveAt(_messages.Count - 1);
                    return new[] { new AIAutocompleteErrorItem(ex) };
                }
            }

            var suggestion = response.Contents.OfType<TextContent>().Where(t => !String.IsNullOrWhiteSpace(t.Text)).FirstOrDefault();

            if (suggestion == null)
                return Enumerable.Empty<AutocompleteItem>();

            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(suggestion.Text));

            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return Enumerable.Empty<AutocompleteItem>();

            if (!reader.Read() || reader.TokenType != JsonTokenType.PropertyName || reader.GetString() != "completions")
                return Enumerable.Empty<AutocompleteItem>();

            if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray)
                return Enumerable.Empty<AutocompleteItem>();

            var items = new List<AutocompleteItem>();

            while (true)
            {
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    return items;

                if (!reader.Read() || reader.TokenType != JsonTokenType.PropertyName || reader.GetString() != "sql")
                    return items;

                if (!reader.Read() || reader.TokenType != JsonTokenType.String)
                    return items;

                items.Add(new AIAutocompleteItem(reader.GetString()));

                if (!reader.Read() || reader.TokenType != JsonTokenType.EndObject)
                    return items;
            }

            return items;
#pragma warning restore OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        }

        class AIAutocompleteItem : AutocompleteItem
        {
            public AIAutocompleteItem(string text) : base(text, 27)
            {
            }

            public override string MenuText
            {
                get => Text.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ");
                set { }
            }

            public override string ToolTipTitle
            {
                get => "AI Suggestion";
                set { }
            }

            public override string ToolTipText
            {
                get => "This suggestion was generated by AI, review it carefully before use";
                set { }
            }
        }

        class AIAutocompleteErrorItem : AutocompleteItem
        {
            public AIAutocompleteErrorItem(Exception ex) : base(GetMessage(ex), 28)
            {
            }

            private static string GetMessage(Exception ex)
            {
                if (ex is ClientResultException cre)
                {
                    var firstLineStripped = cre.Message.IndexOf("\r\n\r\n");
                    if (firstLineStripped != -1)
                        return cre.Message.Substring(firstLineStripped + 4).Trim();
                }

                return ex.Message;
            }

            public override string MenuText
            {
                get => "AI Error: " + Text.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ");
                set { }
            }

            public override string ToolTipTitle
            {
                get => "AI Error";
                set { }
            }

            public override string ToolTipText
            {
                get => "An error was encountered generating AI autocomplete suggestions";
                set { }
            }

            public override string GetTextForReplace()
            {
                return string.Empty;
            }
        }
    }
}
