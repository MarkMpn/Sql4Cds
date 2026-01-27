using System;
using System.ClientModel;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using Anthropic;
using Azure.AI.OpenAI;
using Microsoft.Extensions.AI;
using Microsoft.Xrm.Sdk.Metadata;
using OpenAI.Chat;
using QuikGraph;
using QuikGraph.Algorithms;

namespace MarkMpn.Sql4Cds.XTB
{
    static class AIExtensions
    {
        public static IChatClient CreateChatClient(this Settings settings, Func<ChatClientBuilder, ChatClientBuilder> builderConfig = null)
        {
            IChatClient chatClient;

            switch (settings.AIProvider)
            {
                case AIProvider.Sponsorship:
                    chatClient = new ChatClient(settings.AIModel, new ApiKeyCredential(settings.AIAPIKey), new OpenAI.OpenAIClientOptions { Endpoint = new Uri("https://localhost:7140/api/ai") }).AsIChatClient();
                    break;

                case AIProvider.Anthropic:
                    chatClient = new AnthropicClient(settings.AIAPIKey);
                    break;

                case AIProvider.OpenAI:
                    chatClient = new ChatClient(settings.AIModel, new ApiKeyCredential(settings.AIAPIKey)).AsIChatClient();
                    break;

                case AIProvider.AzureOpenAI:
                    chatClient = new AzureOpenAIClient(new Uri(settings.AIEndpoint), new ApiKeyCredential(settings.AIAPIKey))
                        .GetChatClient(settings.AIModel)
                        .AsIChatClient();
                    break;

                default:
                    throw new CopilotException("Unknown AI Provider");
            }

            var builder = chatClient.AsBuilder()
                .ConfigureOptions(options =>
                {
                    options.ModelId = settings.AIModel;
                });

            if (builderConfig != null)
                builder = builderConfig(builder);

            return builder.Build();
        }
    }

    class CopilotException : Exception
    {
        public CopilotException(string message) : base(message)
        {
        }
    }

    class AIFunctions
    {
        private readonly SqlQueryControl _control;

        public AIFunctions(SqlQueryControl control)
        {
            _control = control;
        }

        public JsonSerializerOptions JsonSerializerOptions { get; } = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver()
        };

        public List<AITool> GetTools()
        {
            return new List<AITool>
            {
                AIFunctionFactory.Create((Func<IDictionary<string,ListTableResult>>)ListTables, name: "list_tables", serializerOptions: JsonSerializerOptions),
                AIFunctionFactory.Create((Func<string, IDictionary<string,ColumnListResult>>)GetColumnsInTable, name: "get_columns_in_table", serializerOptions: JsonSerializerOptions),
                AIFunctionFactory.Create((Func<string, string, Relationship[]>)FindRelationship, name: "find_relationship", serializerOptions: JsonSerializerOptions),
            };
        }

        [Description("Get the list of tables in the current environment")]
        private IDictionary<string, ListTableResult> ListTables()
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
                throw new CopilotException($"The table '{table_name}' does not exist in this environment");

            var columns = metadata.Attributes.ToDictionary(a => a.LogicalName, a => new ColumnListResult { displayName = a.DisplayName?.UserLocalizedLabel?.Label, description = ShowDescription(a) ? a.Description?.UserLocalizedLabel?.Label : null, type = a.AttributeTypeName?.Value, options = (a as EnumAttributeMetadata)?.OptionSet?.Options?.ToDictionary(o => o.Value.Value, o => o.Label?.UserLocalizedLabel?.Label), lookupTo = (a as LookupAttributeMetadata)?.Targets?.Select(target => target + "." + dataSource.Metadata[target].PrimaryIdAttribute)?.ToArray() });
            return columns;
        }

        class ColumnListResult
        {
            public string displayName { get; set; }
            public string description { get; set; }
            public string type { get; set; }
            public Dictionary<int, string> options { get; set; }
            public string[] lookupTo { get; set; }
        }


        [Description("Finds the path of joins that can be used to connect two tables")]
        private Relationship[] FindRelationship([Description("The table to join from")] string table1, [Description("The table to join to")] string table2)
        {
            var dataSource = _control.DataSources[_control.Connection.ConnectionName];

            if (!dataSource.Metadata.TryGetValue(table1, out var t1))
                throw new CopilotException($"The table '{table1}' does not exist in this environment");

            if (!dataSource.Metadata.TryGetValue(table2, out var t2))
                throw new CopilotException($"The table '{table2}' does not exist in this environment");

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
                throw new CopilotException("There is no set of relationships that link these two tables");

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
    }
}
