using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Admin;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Microsoft.Xrm.Sdk;
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
            lsp.AddHandler(Methods.TextDocumentSignatureHelp, HandleSignatureHelp);
        }

        public SumType<CompletionItem[], CompletionList>? HandleCompletion(CompletionParams request)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri.ToString());
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character - 1;
            var con = _con.GetConnection(request.TextDocument.Uri.ToString());
            var cons = _con.GetAllConnections();
            var acds = new Dictionary<string, AutocompleteDataSource>();

            if (con == null)
                return Array.Empty<CompletionItem>();

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

            if (con == null)
                return null;

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

        private SignatureHelp HandleSignatureHelp(SignatureHelpParams request)
        {
            var doc = _doc.GetContent(request.TextDocument.Uri.ToString());
            var lines = doc.Split('\n');
            var pos = lines.Take(request.Position.Line).Sum(line => line.Length + 1) + request.Position.Character;

            var depth = 1;
            var paramIndex = 0;

            for (var i = pos - 1; i >= 0; i--)
            {
                if (doc[i] == '(')
                {
                    depth--;

                    if (depth == 0)
                    {
                        // Find method name
                        var wordLength = new Regex("\\b").Match(new string(doc.Substring(0, i).Reverse().ToArray()), 1).Index;
                        var wordStart = i - wordLength;
                        var functionName = doc.Substring(wordStart, wordLength);
                        var con = _con.GetConnection(request.TextDocument.Uri.ToString());

                        if (con == null)
                            return null;

                        if (con.DataSource.MessageCache.TryGetValue(functionName, out var message) &&
                            message.IsValidAsTableValuedFunction())
                        {
                            var availableParameters = message.InputParameters
                                .OrderBy(p => p.Position)
                                .Select(p => new ParameterInformation
                                {
                                    Label = p.Name,
                                    Documentation = p.Name + " " + CLRToSQLTypeName(p.Type)
                                })
                                .ToArray();

                            return new SignatureHelp
                            {
                                ActiveSignature = 0,
                                ActiveParameter = paramIndex,
                                Signatures = new[]
                                {
                                    new SignatureInformation
                                    {
                                        Label = message.Name,
                                        Documentation = "Dataverse Message",
                                        Parameters = availableParameters
                                    }
                                }
                            };
                        }
                        else
                        {
                            var function = typeof(FunctionMetadata.SqlFunctions)
                                .GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public)
                                .FirstOrDefault(m => m.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase));

                            if (function != null)
                            {
                                var availableParameters = function
                                    .GetParameters()
                                    .Select(p => new ParameterInformation
                                    {
                                        Label = p.Name,
                                        Documentation = (p.Name + " " + CLRToSQLTypeName(p.ParameterType)).Trim()
                                    })
                                    .ToArray();

                                return new SignatureHelp
                                {
                                    ActiveSignature = 0,
                                    ActiveParameter = paramIndex,
                                    Signatures = new[]
                                    {
                                        new SignatureInformation
                                        {
                                            Label = CLRToSQLTypeName(function.ReturnType) + " " + function.Name.ToUpperInvariant() + "(" + String.Join(", ", availableParameters.Select(p => (string)p.Documentation)) + ")",
                                            Documentation = function.GetCustomAttribute<DescriptionAttribute>()?.Description ?? "SQL Function",
                                            Parameters = availableParameters
                                        }
                                    }
                                };
                            }
                        }
                    }
                }
                else if (doc[i] == ')')
                {
                    depth++;
                }
                else if (doc[i] == ',' && depth == 1)
                {
                    paramIndex++;
                }
            }

            return null;
        }

        private string CLRToSQLTypeName(Type type)
        {
            if (type == typeof(string))
                return "nvarchar";

            if (type == typeof(int))
                return "int";

            if (type == typeof(long))
                return "bigint";

            if (type == typeof(DateTime))
                return "datetime";

            if (type == typeof(Guid))
                return "uniqueidentifier";

            if (type == typeof(EntityReference))
                return "[MarkMpn.Sql4Cds.Engine.SqlEntityReference]";

            if (type == typeof(double))
                return "double";

            if (type == typeof(object))
                return "";

            return type.Name.ToLowerInvariant();
        }
    }
}
