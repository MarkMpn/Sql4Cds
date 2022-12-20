using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Admin;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;
using Range = Microsoft.VisualStudio.LanguageServer.Protocol.Range;

namespace MarkMpn.Sql4Cds.LanguageServer.Autocomplete
{
    class AutocompleteHandler : IJsonRpcMethodHandler
    {
        private readonly TextDocumentManager _doc;
        private readonly ConnectionManager _con;

        public AutocompleteHandler(TextDocumentManager doc, ConnectionManager con)
        {
            _doc = doc;
            _con = con;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(Methods.TextDocumentCompletion, HandleCompletion);
            lsp.AddHandler(Methods.TextDocumentHover, HandleHover);
        }

        public SumType<CompletionItem[], CompletionList>? HandleCompletion(CompletionParams request)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri.ToString());
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character - 1;
            var con = _con.GetConnection(request.TextDocument.Uri.ToString());
            var cons = _con.GetAllConnections();
            var acds = new Dictionary<string, AutocompleteDataSource>();

            foreach (var c in cons)
            {
                EntityCache.TryGetEntities(c.Value.Connection, out var entities);

                acds.Add(c.Key, new AutocompleteDataSource
                {
                    Name = c.Value.Name,
                    Entities = entities,
                    Metadata = c.Value.Metadata,
                    Messages = c.Value.MessageCache
                });
            }
            var ac = new Autocomplete(acds, con.DataSource.Name);
            var suggestions = ac.GetSuggestions(doc, pos);
            return suggestions
                .Select(s => new CompletionItem
                {
                    Label = s.MenuText,
                    InsertText = s.GetTextForReplace(),
                    Documentation = s.ToolTipText,
                    Detail = s.ToolTipTitle,
                    Kind = s.ImageIndex
                })
                .ToArray();
        }

        public Hover HandleHover(TextDocumentPositionParams request)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri.ToString());
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character;
            var wordEnd = new Regex("\\b").Match(doc, pos + 1);

            if (!wordEnd.Success)
                return null;

            var con = _con.GetConnection(request.TextDocument.Uri.ToString());
            var cons = _con.GetAllConnections();
            var acds = new Dictionary<string, AutocompleteDataSource>();

            foreach (var c in cons)
            {
                EntityCache.TryGetEntities(c.Value.Connection, out var entities);

                acds.Add(c.Key, new AutocompleteDataSource
                {
                    Name = c.Value.Name,
                    Entities = entities,
                    Metadata = c.Value.Metadata,
                    Messages = c.Value.MessageCache
                });
            }
            var ac = new Autocomplete(acds, con.DataSource.Name);
            var suggestions = ac.GetSuggestions(doc, pos);
            var exactSuggestions = suggestions.Where(suggestion => suggestion.Text.Length <= wordEnd.Index && doc.Substring(wordEnd.Index - suggestion.CompareText.Length, suggestion.CompareText.Length).Equals(suggestion.CompareText, StringComparison.OrdinalIgnoreCase)).ToList();

            if (exactSuggestions.Count != 1)
                return null;

            return new Hover
            {
                Range = new Range
                {
                    Start = new Position
                    {
                        Line = request.Position.Line,
                        Character = request.Position.Character + wordEnd.Index - pos - exactSuggestions[0].Text.Length
                    },
                    End = new Position
                    {
                        Line = request.Position.Line,
                        Character = request.Position.Character + wordEnd.Index - pos
                    }
                },
                Contents = new[]
                {
                    (SumType<string, MarkedString>)new MarkedString { Value = "**" + exactSuggestions[0].ToolTipTitle + "**" },
                    (SumType<string, MarkedString>)new MarkedString { Value = exactSuggestions[0].ToolTipText }
                }
            };
        }
    }
}
