using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Models;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class AutocompleteHandler : IRequestHandler<TextDocumentPosition, CompletionItem[]>, IRequestHandler<TextDocumentPositionHover, Hover>, IJsonRpcHandler
    {
        private readonly TextDocumentManager _doc;
        private readonly ConnectionManager _con;

        public AutocompleteHandler(TextDocumentManager doc, ConnectionManager con)
        {
            _doc = doc;
            _con = con;
        }

        public Task<CompletionItem[]> Handle(TextDocumentPosition request, CancellationToken cancellationToken)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri);
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character - 1;
            var con = _con.GetConnection(request.TextDocument.Uri);
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
            return Task.FromResult(suggestions.Select(s => new CompletionItem
            {
                Label = s.MenuText,
                InsertText = s.GetTextForReplace(),
                Documentation = s.ToolTipText,
                Detail = s.ToolTipTitle,
                Kind = s.ImageIndex
            }).ToArray());
        }

        public Task<Hover> Handle(TextDocumentPositionHover request, CancellationToken cancellationToken)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri);
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character;
            var wordEnd = new Regex("\\b").Match(doc, pos + 1);

            if (!wordEnd.Success)
                return Task.FromResult<Hover>(null);

            var con = _con.GetConnection(request.TextDocument.Uri);
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
                return Task.FromResult<Hover>(null);

            return Task.FromResult(new Hover
            {
                Range = new OmniSharp.Extensions.LanguageServer.Protocol.Models.Range
                {
                    Start = new OmniSharp.Extensions.LanguageServer.Protocol.Models.Position
                    {
                        Line = request.Position.Line,
                        Character = request.Position.Character + wordEnd.Index - pos - exactSuggestions[0].Text.Length
                    },
                    End = new OmniSharp.Extensions.LanguageServer.Protocol.Models.Position
                    {
                        Line = request.Position.Line,
                        Character = request.Position.Character + wordEnd.Index - pos
                    }
                },
                Contents = new MarkedStringsOrMarkupContent(
                    new OmniSharp.Extensions.LanguageServer.Protocol.Models.MarkedString("**" + exactSuggestions[0].ToolTipTitle + "**"),
                    new OmniSharp.Extensions.LanguageServer.Protocol.Models.MarkedString(exactSuggestions[0].ToolTipText))
            });
        }
    }

    [Method("textDocument/hover", Direction.ClientToServer)]
    [Serial]
    class TextDocumentPositionHover : IRequest<Hover>
    {
        /// <summary>
        /// Gets or sets the document identifier.
        /// </summary>
        public TextDocumentIdentifier TextDocument { get; set; }

        /// <summary>
        /// Gets or sets the position in the document.
        /// </summary>
        public Position Position { get; set; }
    }

    [Method("textDocument/completion", Direction.ClientToServer)]
    [Serial]
    class TextDocumentPosition : IRequest<CompletionItem[]>
    {
        /// <summary>
        /// Gets or sets the document identifier.
        /// </summary>
        public TextDocumentIdentifier TextDocument { get; set; }

        /// <summary>
        /// Gets or sets the position in the document.
        /// </summary>
        public Position Position { get; set; }
    }

    /// <summary>
    /// Defines a base parameter class for identifying a text document.
    /// </summary>
    [DebuggerDisplay("TextDocumentIdentifier = {Uri}")]
    public class TextDocumentIdentifier
    {
        /// <summary>
        /// Gets or sets the URI which identifies the path of the
        /// text document.
        /// </summary>
        public string Uri { get; set; }
    }

    [DebuggerDisplay("Position = {Line}:{Character}")]
    public class Position
    {
        /// <summary>
        /// Gets or sets the zero-based line number.
        /// </summary>
        public int Line { get; set; }

        /// <summary>
        /// Gets or sets the zero-based column number.
        /// </summary>
        public int Character { get; set; }

        /// <summary>
        /// Overrides the base equality method
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == null || (obj as Position == null))
            {
                return false;
            }
            Position p = (Position)obj;
            bool result = (Line == p.Line) && (Character == p.Character);
            return result;
        }


        /// <summary>
        /// Overrides the base GetHashCode method
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + Line.GetHashCode();
            hash = hash * 23 + Character.GetHashCode();
            return hash;
        }
    }

    [DebuggerDisplay("Start = {Start.Line}:{Start.Character}, End = {End.Line}:{End.Character}")]
    public struct Range
    {
        /// <summary>
        /// Gets or sets the starting position of the range.
        /// </summary>
        public Position Start { get; set; }

        /// <summary>
        /// Gets or sets the ending position of the range.
        /// </summary>
        public Position End { get; set; }

        /// <summary>
        /// Overrides the base equality method
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {


            if (obj == null || !(obj is Range))
            {
                return false;
            }
            Range range = (Range)obj;
            bool sameStart = range.Start.Equals(Start);
            bool sameEnd = range.End.Equals(End);
            return (sameStart && sameEnd);
        }

        /// <summary>
        /// Overrides the base GetHashCode method
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + Start.GetHashCode();
            hash = hash * 23 + End.GetHashCode();
            return hash;
        }
    }
}
