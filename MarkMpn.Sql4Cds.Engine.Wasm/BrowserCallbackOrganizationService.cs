using System;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization.Json;
using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Wasm;

[SupportedOSPlatform("browser")]
internal sealed class BrowserCallbackOrganizationService : IOrganizationService, IDisposable
{
    private readonly string _callbackSetId;

    public BrowserCallbackOrganizationService(string callbackSetId)
    {
        _callbackSetId = callbackSetId ?? throw new ArgumentNullException(nameof(callbackSetId));
    }

    public Guid Create(Entity entity)
    {
        var result = InvokeRequired("create", Serialize(entity));
        return Guid.Parse(result);
    }

    public void Update(Entity entity)
    {
        InvokeRequired("update", Serialize(entity));
    }

    public void Delete(string entityName, Guid id)
    {
        InvokeRequired("delete", entityName, id.ToString("D"));
    }

    public OrganizationResponse Execute(OrganizationRequest request)
    {
        var result = InvokeRequired("execute", request.RequestName, request.GetType().FullName ?? request.GetType().Name, Serialize(request));
        return (OrganizationResponse)Deserialize(GetResponseType(request), result);
    }

    public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet)
    {
        var result = InvokeRequired("retrieve", entityName, id.ToString("D"), Serialize(columnSet));
        return Deserialize<Entity>(result);
    }

    public EntityCollection RetrieveMultiple(QueryBase query)
    {
        var result = InvokeRequired("retrieveMultiple", query.GetType().FullName ?? query.GetType().Name, Serialize(query));
        return Deserialize<EntityCollection>(result);
    }

    public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
    {
        InvokeRequired("associate", entityName, entityId.ToString("D"), Serialize(relationship), Serialize(relatedEntities));
    }

    public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
    {
        InvokeRequired("disassociate", entityName, entityId.ToString("D"), Serialize(relationship), Serialize(relatedEntities));
    }

    public void Dispose()
    {
    }

    private string InvokeRequired(string callbackName, params string?[] args)
    {
        return BrowserInterop.InvokeCallback(
            _callbackSetId,
            callbackName,
            args.Length > 0 ? args[0] : null,
            args.Length > 1 ? args[1] : null,
            args.Length > 2 ? args[2] : null,
            args.Length > 3 ? args[3] : null);
    }

    private static string Serialize<T>(T value)
    {
        var serializer = new DataContractJsonSerializer(value?.GetType() ?? typeof(T));

        using var stream = new MemoryStream();
        serializer.WriteObject(stream, value);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static T Deserialize<T>(string json)
    {
        return (T)Deserialize(typeof(T), json);
    }

    private static object Deserialize(Type type, string json)
    {
        var serializer = new DataContractJsonSerializer(type);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return serializer.ReadObject(stream);
    }

    private static Type GetResponseType(OrganizationRequest request)
    {
        return request switch
        {
            RetrieveMetadataChangesRequest => typeof(RetrieveMetadataChangesResponse),
            RetrieveAllOptionSetsRequest => typeof(RetrieveAllOptionSetsResponse),
            RetrieveVersionRequest => typeof(RetrieveVersionResponse),
            WhoAmIRequest => typeof(WhoAmIResponse),
            ExecuteMultipleRequest => typeof(ExecuteMultipleResponse),
            _ => typeof(OrganizationResponse)
        };
    }
}

[SupportedOSPlatform("browser")]
internal static partial class BrowserInterop
{
    [JSImport("globalThis.sql4cdsInvokeCallback")]
    internal static partial string InvokeCallback(string callbackSetId, string callbackName, string? arg1, string? arg2, string? arg3, string? arg4);
}

[SupportedOSPlatform("browser")]
internal sealed class Sql4CdsWasmSession : IDisposable
{
    private static readonly PropertyInfo DefaultCollationProperty = typeof(DataSource).GetProperty("DefaultCollation", BindingFlags.Instance | BindingFlags.NonPublic);
    private static readonly PropertyInfo EnglishCollationProperty = typeof(Collation).GetProperty("USEnglish", BindingFlags.Static | BindingFlags.NonPublic);

    private readonly Sql4CdsConnection _connection;
    private readonly BrowserCallbackOrganizationService? _service;

    private Sql4CdsWasmSession(Sql4CdsConnection connection, BrowserCallbackOrganizationService? service = null)
    {
        _connection = connection;
        _service = service;
        _connection.UseTDSEndpoint = false;
        _connection.ApplicationName = "SQL 4 CDS WASM";
    }

    public static Sql4CdsWasmSession CreateLocal()
    {
        var metadata = new EmptyAttributeMetadataCache();
        var dataSource = new DataSource
        {
            Name = "local",
            Metadata = metadata,
            TableSizeCache = new EmptyTableSizeCache(),
            MessageCache = new EmptyMessageCache()
        };

        DefaultCollationProperty?.SetValue(dataSource, EnglishCollationProperty?.GetValue(null));

        var connection = new Sql4CdsConnection(new System.Collections.Generic.Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase)
        {
            ["local"] = dataSource
        });

        return new Sql4CdsWasmSession(connection);
    }

    public static Sql4CdsWasmSession CreateRemote(string callbackSetId, string dataSourceName)
    {
        var service = new BrowserCallbackOrganizationService(callbackSetId);
        var metadata = new AttributeMetadataCache(service);
        var dataSource = new DataSource
        {
            Name = String.IsNullOrWhiteSpace(dataSourceName) ? "js" : dataSourceName,
            Connection = service,
            Metadata = metadata,
            TableSizeCache = new TableSizeCache(service, metadata),
            MessageCache = new MessageCache(service, metadata)
        };

        DefaultCollationProperty?.SetValue(dataSource, EnglishCollationProperty?.GetValue(null));

        var connection = new Sql4CdsConnection(new System.Collections.Generic.Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase)
        {
            [dataSource.Name] = dataSource
        });

        return new Sql4CdsWasmSession(connection, service);
    }

    public Sql4CdsLocalExecutionResult Execute(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var result = new Sql4CdsLocalExecutionResult();

        do
        {
            if (reader.FieldCount == 0)
                continue;

            var columns = new string[reader.FieldCount];

            for (var i = 0; i < reader.FieldCount; i++)
                columns[i] = reader.GetName(i);

            var rows = new System.Collections.Generic.List<System.Collections.Generic.Dictionary<string, object>>();

            while (reader.Read())
            {
                var row = new System.Collections.Generic.Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                for (var i = 0; i < reader.FieldCount; i++)
                    row[columns[i]] = ConvertValue(reader.GetValue(i));

                rows.Add(row);
            }

            result.ResultSets.Add(new Sql4CdsLocalResultSet
            {
                Columns = columns,
                Rows = rows
            });
        }
        while (reader.NextResult());

        result.RecordsAffected = reader.RecordsAffected;
        return result;
    }

    public Sql4CdsLocalPlan Explain(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;

        var statements = new System.Collections.Generic.List<Sql4CdsLocalPlanNode>();

        foreach (var statement in cmd.GeneratePlan(false))
            statements.Add(DescribeNode(statement));

        return new Sql4CdsLocalPlan
        {
            Statements = statements
        };
    }

    public void Dispose()
    {
        _connection.Dispose();
        _service?.Dispose();
    }

    private static object? ConvertValue(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;

        return Sql4CdsLocalEngineValueConverter.Convert(value);
    }

    private static Sql4CdsLocalPlanNode DescribeNode(MarkMpn.Sql4Cds.Engine.ExecutionPlan.IExecutionPlanNode node)
    {
        var children = new System.Collections.Generic.List<Sql4CdsLocalPlanNode>();

        foreach (var child in node.GetSources())
            children.Add(DescribeNode(child));

        return new Sql4CdsLocalPlanNode
        {
            Type = node.GetType().Name,
            Sql = (node as MarkMpn.Sql4Cds.Engine.ExecutionPlan.IRootExecutionPlanNode)?.Sql,
            Children = children
        };
    }

    private sealed class EmptyAttributeMetadataCache : IAttributeMetadataCache
    {
        public EntityMetadata this[string name] => throw new System.Collections.Generic.KeyNotFoundException("Unknown entity " + name);

        public EntityMetadata this[int otc] => throw new System.Collections.Generic.KeyNotFoundException("Unknown entity object type code " + otc);

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            metadata = null;
            return false;
        }

        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            metadata = null;
            return false;
        }

        public string[] RecycleBinEntities => Array.Empty<string>();

        public System.Collections.Generic.IEnumerable<EntityMetadata> GetAllEntities() => Array.Empty<EntityMetadata>();

        public string[] TryGetRecycleBinEntities() => Array.Empty<string>();
    }

    private sealed class EmptyTableSizeCache : MarkMpn.Sql4Cds.Engine.ExecutionPlan.ITableSizeCache
    {
        public int this[string logicalName] => 0;
    }

    private sealed class EmptyMessageCache : IMessageCache
    {
        public System.Collections.Generic.IEnumerable<Message> GetAllMessages(bool lazy) => Array.Empty<Message>();

        public bool TryGetValue(string name, out Message message)
        {
            message = null;
            return false;
        }

        public bool IsMessageAvailable(string entityLogicalName, string messageName) => false;
    }
}
