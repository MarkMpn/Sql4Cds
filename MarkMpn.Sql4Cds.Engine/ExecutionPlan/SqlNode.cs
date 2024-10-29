using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;

#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Executes SQL using the TDS endpoint
    /// </summary>
    class SqlNode : BaseNode, IDataReaderExecutionPlanNode
    {
        private readonly Timer _timer = new Timer();
        private int _executionCount;
        private INodeSchema _schema;

        private readonly static Dictionary<Type, INullable> _sampleValues = new Dictionary<Type, INullable>
        {
            [typeof(SqlBinary)] = new SqlBinary(new byte[] { 0 }),
            [typeof(SqlBoolean)] = SqlBoolean.False,
            [typeof(SqlByte)] = SqlByte.Zero,
            [typeof(SqlDateTime)] = SqlDateTime.MinValue,
            [typeof(SqlDecimal)] = SqlDecimal.MinValue,
            [typeof(SqlDouble)] = SqlDouble.Zero,
            [typeof(SqlGuid)] = (SqlGuid)Guid.NewGuid(),
            [typeof(SqlEntityReference)] = (SqlGuid)Guid.NewGuid(),
            [typeof(SqlInt16)] = SqlInt16.Zero,
            [typeof(SqlInt32)] = SqlInt32.Zero,
            [typeof(SqlInt64)] = SqlInt64.Zero,
            [typeof(SqlMoney)] = SqlDecimal.MinValue,
            [typeof(SqlSingle)] = SqlSingle.Zero,
            [typeof(SqlString)] = (SqlString)"test",
            [typeof(SqlDate)] = SqlDateTime.MinValue,
            [typeof(SqlDateTime2)] = SqlDateTime.MinValue,
            [typeof(SqlDateTimeOffset)] = SqlDateTime.MinValue,
            [typeof(SqlSmallDateTime)] = SqlDateTime.MinValue,
            [typeof(SqlTime)] = (SqlTime)TimeSpan.Zero,
            [typeof(SqlXml)] = new SqlXml(new MemoryStream(System.Text.Encoding.UTF8.GetBytes("<x></x>"))),
            [typeof(SqlVariant)] = (SqlString)"test"
        };

        public SqlNode() { }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        [Category("TDS Endpoint")]
        [Description("The SQL query to execute")]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        [Browsable(false)]
        public HashSet<string> Parameters { get; private set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        internal SelectStatement SelectStatement { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public DbDataReader Execute(NodeExecutionContext context, CommandBehavior behavior)
        {
            _executionCount++;

            using (_timer.Run())
            {
                try
                {
                    if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                        throw new QueryExecutionException("Missing datasource " + DataSource);

                    if (context.Options.UseLocalTimeZone)
                        throw new QueryExecutionException("Cannot use automatic local time zone conversion with the TDS Endpoint");

#if NETCOREAPP
                    if (!(dataSource.Connection is ServiceClient svc))
                        throw new QueryExecutionException($"IOrganizationService implementation needs to be ServiceClient for use with the TDS Endpoint, got {dataSource.Connection.GetType()}");
#else
                    if (!(dataSource.Connection is CrmServiceClient svc))
                        throw new QueryExecutionException($"IOrganizationService implementation needs to be CrmServiceClient for use with the TDS Endpoint, got {dataSource.Connection.GetType()}");
#endif

                    if (svc.CallerId != Guid.Empty)
                        throw new QueryExecutionException("Cannot use impersonation with the TDS Endpoint");

                    if (String.IsNullOrEmpty(svc.CurrentAccessToken))
                        throw new QueryExecutionException("OAuth must be used to authenticate with the TDS Endpoint");

                    var con = TDSEndpoint.Connect(svc);

                    var cmd = con.CreateCommand();
                    cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;
                    cmd.CommandText = ApplyCommandBehavior(Sql, behavior, context);

                    foreach (var paramValue in context.ParameterValues)
                    {
                        if (paramValue.Key.StartsWith("@@"))
                            continue;

                        if (!Parameters.Contains(paramValue.Key))
                            continue;

                        var param = cmd.CreateParameter();
                        param.ParameterName = paramValue.Key;

                        if (paramValue.Value is SqlEntityReference er)
                            param.Value = (SqlGuid)er;
                        else
                            param.Value = paramValue.Value;

                        if (paramValue.Value.IsNull)
                            param.Value = "";

                        cmd.Parameters.Add(param);
                    }

                    context.Options.CancellationToken.Register(() => cmd.Cancel());
                    if (Parent == null)
                    {
                        cmd.StatementCompleted += (s, e) =>
                        {
                            context.ParameterValues["@@ROWCOUNT"] = (SqlInt32)e.RecordCount;
                        };
                    }

                    return new SqlDataReaderWrapper(con, cmd, behavior | CommandBehavior.CloseConnection, this, context.Options.CancellationToken);
                }
                catch (SqlException ex)
                {
                    throw new QueryExecutionException(new Sql4CdsError(ex.Class, ex.LineNumber + LineNumber - 1, ex.Number, String.IsNullOrEmpty(ex.Procedure) ? null : ex.Procedure, ex.Server, ex.State, ex.Message), ex)
                    {
                        Node = this
                    };
                }
                catch (QueryExecutionException ex)
                {
                    if (ex.Node == null)
                        ex.Node = this;

                    throw;
                }
                catch (Exception ex)
                {
                    throw new QueryExecutionException(ex.Message, ex)
                    {
                        Node = this
                    };
                }
            }
        }

        internal static string ApplyCommandBehavior(string sql, CommandBehavior behavior, NodeExecutionContext context)
        {
            if (context.Session.DateFormat != DateFormat.mdy)
            {
                // mdy is the default format for the TDS Endpoint, so we need to switch it as necessary
                sql = "SET DATEFORMAT " + context.Session.DateFormat.ToString() + ";\r\n" + sql;
            }

            return sql;
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        internal IExecutionPlanNodeInternal FoldDmlSource(NodeCompilationContext context, IList<OptimizerHint> hints, string logicalName, string[] requiredColumns, string[] keyAttributes)
        {
            if (!(SelectStatement?.QueryExpression is QuerySpecification querySpec))
                return this;

            if (querySpec.FromClause == null || querySpec.FromClause.TableReferences.Count != 1 || !(querySpec.FromClause.TableReferences[0] is NamedTableReference table))
                return this;

            if (table.SchemaObject.BaseIdentifier.Value != logicalName)
                return this;

            if (querySpec.WhereClause == null || querySpec.WhereClause.SearchCondition == null)
                return this;

            var filterVisitor = new SimpleFilterVisitor();
            querySpec.WhereClause.SearchCondition.Accept(filterVisitor);

            if (filterVisitor.BinaryType == null)
                return this;

            var dataSource = context.Session.DataSources[DataSource];
            var metadata = dataSource.Metadata[logicalName];
            var conditions = filterVisitor.Conditions.ToList();
            var ecc = new ExpressionCompilationContext(context, null, null);

            if (!TryGetDmlSchema(dataSource, metadata, querySpec, new ExpressionCompilationContext(context, null, null), out var literalValues))
                return this;

            // Every column must either be a literal or a key attribute
            if (requiredColumns.Except(literalValues.Keys).Except(keyAttributes).Any())
                return this;

            var schema = GetSchema(context);
            var constantScan = new ConstantScanNode();

            foreach (var col in requiredColumns)
                constantScan.Schema[col.SplitMultiPartIdentifier().Last()] = schema.Schema[col];

            // We can handle compound keys, but only if they are all ANDed together
            if (keyAttributes.Length > 1 && filterVisitor.BinaryType == BooleanBinaryExpressionType.And)
            {
                var values = new Dictionary<string, ScalarExpression>();

                foreach (var col in requiredColumns)
                {
                    if (literalValues.ContainsKey(col))
                        continue;

                    var condition = conditions.FirstOrDefault(c => c.attribute == col.SplitMultiPartIdentifier().Last());
                    if (condition == null)
                        return this;

                    if (condition.@operator != @operator.eq)
                        return this;

                    var attribute = metadata.Attributes.Single(a => a.LogicalName == condition.attribute);
                    values[condition.attribute] = attribute.GetDmlValue(condition.value, condition.IsVariable, ecc, schema.Schema[col].Type);
                }

                constantScan.Values.Add(values);

                AddLiteralValues(constantScan, literalValues, ecc, schema);
                return constantScan;
            }

            // We can also handle multiple values for a single key being ORed together
            else if (keyAttributes.Length == 1 &&
                conditions.All(c => c.attribute == metadata.PrimaryIdAttribute) &&
                conditions.All(c => c.@operator == @operator.eq || c.@operator == @operator.@in) &&
                (conditions.Count == 1 || filterVisitor.BinaryType == BooleanBinaryExpressionType.Or))
            {
                foreach (var condition in conditions)
                {
                    var attribute = metadata.Attributes.Single(a => a.LogicalName == condition.attribute);

                    if (condition.@operator == @operator.eq)
                    {
                        constantScan.Values.Add(new Dictionary<string, ScalarExpression> { [condition.attribute] = attribute.GetDmlValue(condition.value, condition.IsVariable, ecc, schema.Schema[condition.attribute].Type) });
                    }
                    else if (condition.@operator == @operator.@in)
                    {
                        foreach (var value in condition.Items)
                            constantScan.Values.Add(new Dictionary<string, ScalarExpression> { [condition.attribute] = attribute.GetDmlValue(value.Value, value.IsVariable, ecc, schema.Schema[condition.attribute].Type) });
                    }
                }

                AddLiteralValues(constantScan, literalValues, ecc, schema);
                return constantScan;
            }

            return this;
        }

        private void AddLiteralValues(ConstantScanNode constantScan, Dictionary<string, ScalarExpression> literalValues, ExpressionCompilationContext ecc, INodeSchema schema)
        {
            foreach (var row in constantScan.Values)
            {
                foreach (var value in literalValues)
                {
                    var expr = value.Value;
                    expr.GetType(ecc, out var type);
                    if (!type.IsSameAs(schema.Schema[value.Key].Type))
                        expr = new CastCall { Parameter = expr, DataType = schema.Schema[value.Key].Type };

                    row[value.Key] = expr;
                }
            }
        }

        private bool TryGetDmlSchema(DataSource dataSource, EntityMetadata metadata, QuerySpecification querySpec, ExpressionCompilationContext context, out Dictionary<string, ScalarExpression> literalValues)
        {
            literalValues = new Dictionary<string, ScalarExpression>(StringComparer.OrdinalIgnoreCase);

            foreach (var select in querySpec.SelectElements)
            {
                if (!(select is SelectScalarExpression scalar))
                    return false;

                if (!scalar.Expression.GetColumns().Any() && scalar.ColumnName?.Value != null)
                {
                    literalValues[scalar.ColumnName.Value] = scalar.Expression;
                    continue;
                }

                if (!(scalar.Expression is ColumnReferenceExpression col))
                    return false;

                var attribute = metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(col.MultiPartIdentifier.Identifiers.Last().Value, StringComparison.OrdinalIgnoreCase));

                if (attribute == null)
                    return false;
            }

            return true;
        }

        public INodeSchema GetSchema(NodeCompilationContext context)
        {
            if (_schema != null)
                return _schema;

            // Parse the SQL that will be executed by the node so we can identify NULL literals
            var parser = new TSql160Parser(context.Options.QuotedIdentifiers);
            var script = (TSqlScript)parser.Parse(new StringReader(Sql), out _);

            var querySpec = (script.Batches[0].Statements[0] as SelectStatement).QueryExpression as QuerySpecification;

            // Get the ADO.NET schema from the data reader
            // Parameter values need non-null value for compatibility with TDS Endpoint
            var parameterValues = context.ParameterTypes.ToDictionary(p => p.Key, p => _sampleValues[p.Value.ToNetType(out _)]);
            using (var reader = Execute(new NodeExecutionContext(context, parameterValues), CommandBehavior.SchemaOnly))
            {
                var schemaTable = reader.GetSchemaTable();
                _schema = SchemaConverter.ConvertSchema(schemaTable, context.Session.DataSources[DataSource], querySpec);
                return _schema;
            }
        }

        public override string ToString()
        {
            return "TDS Endpoint";
        }

        public object Clone()
        {
            return new SqlNode
            {
                DataSource = DataSource,
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Parameters = Parameters,
                SelectStatement = SelectStatement,
                _schema = _schema
            };
        }
    }
}
