using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides a local, in-memory SQL 4 CDS execution surface without a Dataverse connection.
    /// </summary>
    public sealed class Sql4CdsLocalEngine : IDisposable
    {
        private readonly Sql4CdsConnection _connection;

        public Sql4CdsLocalEngine()
        {
            var metadata = new EmptyAttributeMetadataCache();
            var dataSource = new DataSource
            {
                Name = "local",
                Metadata = metadata,
                TableSizeCache = new EmptyTableSizeCache(),
                MessageCache = new EmptyMessageCache(),
                DefaultCollation = Collation.USEnglish
            };

            _connection = new Sql4CdsConnection(new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase)
            {
                ["local"] = dataSource
            })
            {
                ApplicationName = "SQL 4 CDS Local Engine"
            };
            _connection.UseTDSEndpoint = false;
        }

        public Sql4CdsLocalExecutionResult Execute(string sql)
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;

                using (var reader = cmd.ExecuteReader())
                {
                    var result = new Sql4CdsLocalExecutionResult();

                    do
                    {
                        if (reader.FieldCount == 0)
                            continue;

                        var columns = Enumerable.Range(0, reader.FieldCount)
                            .Select(reader.GetName)
                            .ToArray();
                        var rows = new List<Dictionary<string, object>>();

                        while (reader.Read())
                        {
                            var row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

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
            }
        }

        public Sql4CdsLocalPlan Explain(string sql)
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;

                return new Sql4CdsLocalPlan
                {
                    Statements = cmd.GeneratePlan(false)
                        .Select(DescribeNode)
                        .ToList()
                };
            }
        }

        private static object ConvertValue(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            switch (value)
            {
                case SqlString sqlString:
                    return sqlString.IsNull ? null : sqlString.Value;
                case SqlInt16 sqlInt16:
                    return sqlInt16.IsNull ? null : sqlInt16.Value;
                case SqlInt32 sqlInt32:
                    return sqlInt32.IsNull ? null : sqlInt32.Value;
                case SqlInt64 sqlInt64:
                    return sqlInt64.IsNull ? null : sqlInt64.Value;
                case SqlDecimal sqlDecimal:
                    return sqlDecimal.IsNull ? null : sqlDecimal.Value;
                case SqlDouble sqlDouble:
                    return sqlDouble.IsNull ? null : sqlDouble.Value;
                case SqlSingle sqlSingle:
                    return sqlSingle.IsNull ? null : sqlSingle.Value;
                case SqlBoolean sqlBoolean:
                    return sqlBoolean.IsNull ? null : sqlBoolean.Value;
                case SqlGuid sqlGuid:
                    return sqlGuid.IsNull ? null : sqlGuid.Value;
                case SqlDateTime sqlDateTime:
                    return sqlDateTime.IsNull ? null : sqlDateTime.Value;
                case SqlMoney sqlMoney:
                    return sqlMoney.IsNull ? null : sqlMoney.Value;
                case SqlBytes sqlBytes:
                    return sqlBytes.IsNull ? null : sqlBytes.Value;
                case SqlBinary sqlBinary:
                    return sqlBinary.IsNull ? null : sqlBinary.Value;
                case SqlXml sqlXml:
                    return sqlXml.IsNull ? null : sqlXml.Value;
                case SqlEntityReference sqlEntityReference:
                    return sqlEntityReference.IsNull
                        ? null
                        : new Dictionary<string, object>
                        {
                            ["logicalName"] = sqlEntityReference.LogicalName,
                            ["id"] = sqlEntityReference.Id
                        };
                default:
                    return value;
            }
        }

        private static Sql4CdsLocalPlanNode DescribeNode(IExecutionPlanNode node)
        {
            return new Sql4CdsLocalPlanNode
            {
                Type = node.GetType().Name,
                Sql = (node as IRootExecutionPlanNode)?.Sql,
                Children = node.GetSources()
                    .Select(DescribeNode)
                    .ToList()
            };
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        class EmptyAttributeMetadataCache : IAttributeMetadataCache
        {
            public EntityMetadata this[string name] => throw new KeyNotFoundException("Unknown entity " + name);

            public EntityMetadata this[int otc] => throw new KeyNotFoundException("Unknown entity object type code " + otc);

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

            public IEnumerable<EntityMetadata> GetAllEntities()
            {
                return Enumerable.Empty<EntityMetadata>();
            }

            public string[] TryGetRecycleBinEntities()
            {
                return Array.Empty<string>();
            }
        }

        class EmptyTableSizeCache : ITableSizeCache
        {
            public int this[string logicalName] => 0;
        }

        class EmptyMessageCache : IMessageCache
        {
            public IEnumerable<Message> GetAllMessages(bool lazy)
            {
                return Enumerable.Empty<Message>();
            }

            public bool TryGetValue(string name, out Message message)
            {
                message = null;
                return false;
            }

            public bool IsMessageAvailable(string entityLogicalName, string messageName)
            {
                return false;
            }
        }
    }

    public sealed class Sql4CdsLocalExecutionResult
    {
        public int RecordsAffected { get; set; }

        public List<Sql4CdsLocalResultSet> ResultSets { get; } = new List<Sql4CdsLocalResultSet>();
    }

    public sealed class Sql4CdsLocalResultSet
    {
        public string[] Columns { get; set; }

        public List<Dictionary<string, object>> Rows { get; set; }
    }

    public sealed class Sql4CdsLocalPlan
    {
        public List<Sql4CdsLocalPlanNode> Statements { get; set; }
    }

    public sealed class Sql4CdsLocalPlanNode
    {
        public string Type { get; set; }

        public string Sql { get; set; }

        public List<Sql4CdsLocalPlanNode> Children { get; set; }
    }
}
