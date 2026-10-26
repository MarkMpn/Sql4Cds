using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using MarkMpn.Sql4Cds.XTB;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Scripting
{
    class QueryTransformHandler : IJsonRpcMethodHandler
    {
        private readonly ConnectionManager _connections;
        private readonly TextDocumentManager _documents;

        public QueryTransformHandler(ConnectionManager connections, TextDocumentManager documents)
        {
            _connections = connections;
            _documents = documents;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddAsyncHandler(new LspRequest<QueryTransformParams, string[]>("sql4cds/transform"),
                request => Task.Run(() => Transform(request)));
            lsp.AddAsyncHandler(Methods.TextDocumentFormatting,
                request => Task.Run(() => FormatDocument(request)));
        }

        private TextEdit[] FormatDocument(DocumentFormattingParams request)
        {
            var query = _documents.GetContent(request.TextDocument.Uri.ToString());
            var addDisplayNames = false;
            if (request.Options.OtherOptions?.TryGetValue("addDisplayNames", out var option) == true)
                addDisplayNames = option is bool value ? value : throw new ArgumentException("options.addDisplayNames must be a boolean");
            if (String.IsNullOrWhiteSpace(query)) return Array.Empty<TextEdit>();

            string formatted;
            if (!addDisplayNames)
            {
                formatted = Format(query, null);
            }
            else
            {
                var session = _connections.GetConnection(request.TextDocument.Uri.ToString())
                    ?? throw new InvalidOperationException("Select a connection first");
                var dataSources = _connections.GetAllConnections();
                using var connection = new Sql4CdsConnection(dataSources);
                connection.ChangeDatabase(session.DataSource.Name);
                connection.UseTDSEndpoint = false;
                formatted = Format(query, new FormatterOptions
                {
                    AddDisplayNames = true,
                    ThrowOnError = true,
                    Connection = connection,
                    DataSources = dataSources
                });
            }

            if (formatted == query) return Array.Empty<TextEdit>();
            var lines = query.Split('\n');
            return new[]
            {
                new TextEdit
                {
                    Range = new Microsoft.VisualStudio.LanguageServer.Protocol.Range
                    {
                        Start = new Position(0, 0),
                        End = new Position(lines.Length - 1, lines[lines.Length - 1].Length)
                    },
                    NewText = formatted
                }
            };
        }

        private string[] Transform(QueryTransformParams request)
        {
            if (String.IsNullOrWhiteSpace(request.Query))
                throw new ArgumentException("Select or enter a query first");
            var session = _connections.GetConnection(request.OwnerUri)
                ?? throw new InvalidOperationException("Select a connection first");
            var dataSources = _connections.GetAllConnections();
            using var connection = new Sql4CdsConnection(dataSources);
            connection.ChangeDatabase(session.DataSource.Name);
            connection.UseTDSEndpoint = false;

            if (request.Target == "m")
                return new[] { ConvertToM(request.Query, session.DataSource.ServerName) };

            if (request.Target != "fetchxml")
                throw new ArgumentException("Unknown conversion target");

            return ConvertToFetchXml(request.Query, connection);
        }

        internal static string[] ConvertToFetchXml(string query, Sql4CdsConnection connection)
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            var results = new List<string>();
            foreach (var plan in command.GeneratePlan(false))
            {
                var nodes = GetNodes(plan).ToList();
                var fetchXml = nodes.OfType<IFetchXmlExecutionPlanNode>().ToList();
                if (fetchXml.Count == 0)
                    throw new NotSupportedException("This query cannot be represented as FetchXML. Use the estimated execution plan to inspect it.");

                // A plan can require client-side processing even when it contains FetchXML scans.
                var warning = nodes.Any(node => !(node is IFetchXmlExecutionPlanNode) && !(node == plan && node is SelectNode))
                    ? "<!-- This query requires additional processing by SQL 4 CDS. These FetchXML queries retrieve the data, but are not equivalent to the SQL query on their own. See the estimated execution plan. -->\n\n"
                    : "";
                results.Add(warning + String.Join("\n\n", fetchXml.Select(node => node.FetchXmlString)));
            }

            if (results.Count == 0)
                throw new NotSupportedException("The query did not produce any FetchXML");
            return results.ToArray();
        }

        internal static string Format(string query, FormatterOptions options)
        {
            var fragment = new TSql160Parser(true).Parse(new StringReader(query), out var errors);
            
            if (errors.Count != 0)
                throw new ArgumentException(String.Join("\n", errors.Select(error => $"Line {error.Line}: {error.Message}")));
            
            return Formatter.Format(query, options);
        }

        internal static string ConvertToM(string query, string server)
        {
            // Escape M string literals without collapsing SQL newlines (which would extend -- comments).
            var escapedQuery = EscapeMString(query);
            return "let\n" +
                $"    Source = CommonDataService.Database(\"{EscapeMString(server)}\"),\n" +
                $"    DataverseSQL = Value.NativeQuery(Source, \"{escapedQuery}\", null, [EnableFolding=true])\n" +
                "in\n    DataverseSQL";
        }

        private static string EscapeMString(string text)
        {
            return text.Replace("#(", "#(#)(").Replace("\"", "\"\"")
                .Replace("\r", "#(cr)").Replace("\n", "#(lf)").Replace("\t", "#(tab)");
        }

        private static IEnumerable<IExecutionPlanNode> GetNodes(IExecutionPlanNode node)
        {
            yield return node;
            foreach (var source in node.GetSources())
            {
                foreach (var child in GetNodes(source))
                    yield return child;
            }
        }
    }

    class QueryTransformParams
    {
        public string OwnerUri { get; set; }
        public string Query { get; set; }
        public string Target { get; set; }
    }
}
