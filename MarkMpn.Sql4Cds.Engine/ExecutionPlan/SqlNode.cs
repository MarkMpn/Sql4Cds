using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
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
                    if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
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
                    cmd.CommandText = ApplyCommandBehavior(Sql, behavior, context.Options);

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

        internal static string ApplyCommandBehavior(string sql, CommandBehavior behavior, IQueryExecutionOptions options)
        {
            if (behavior == CommandBehavior.Default)
                return sql;

            // TDS Endpoint doesn't support command behavior flags, so fake them by modifying the SQL query
            var dom = new TSql160Parser(options.QuotedIdentifiers);
            var script = (TSqlScript) dom.Parse(new StringReader(sql), out _);

            if (behavior.HasFlag(CommandBehavior.SchemaOnly))
            {
                // Add an impossible WHERE clause to prevent any data being returned
                foreach (var batch in script.Batches)
                {
                    foreach (var select in batch.Statements.OfType<SelectStatement>())
                    {
                        if (select.QueryExpression is QuerySpecification querySpec)
                        {
                            var contradiction = new BooleanComparisonExpression
                            {
                                FirstExpression = new IntegerLiteral { Value = "0" },
                                ComparisonType = BooleanComparisonType.Equals,
                                SecondExpression = new IntegerLiteral { Value = "1" },
                            };

                            if (querySpec.WhereClause == null)
                                querySpec.WhereClause = new WhereClause { SearchCondition = contradiction };
                            else
                                querySpec.WhereClause.SearchCondition = querySpec.WhereClause.SearchCondition.And(contradiction);
                        }
                    }
                }
            }

            if (behavior.HasFlag(CommandBehavior.SingleRow) || behavior.HasFlag(CommandBehavior.SingleResult))
            {
                // Remove all SELECT statements after the first one
                var foundFirstSelect = false;

                foreach (var batch in script.Batches)
                {
                    foreach (var select in batch.Statements.OfType<SelectStatement>().ToArray())
                    {
                        if (!foundFirstSelect)
                            foundFirstSelect = true;
                        else
                            batch.Statements.Remove(select);
                    }
                }
            }

            if (behavior.HasFlag(CommandBehavior.SingleRow))
            {
                // Add a TOP 1 clause to the first SELECT statement
                foreach (var batch in script.Batches)
                {
                    foreach (var select in batch.Statements.OfType<SelectStatement>())
                    {
                        if (select.QueryExpression is QuerySpecification querySpec)
                        {
                            querySpec.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = "1" } };
                        }
                    }
                }
            }

            script.ScriptTokenStream = null;
            return script.ToSql();
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        internal IExecutionPlanNodeInternal FoldDmlSource(NodeCompilationContext context, IList<OptimizerHint> hints, string logicalName, Dictionary<string, string> mappings)
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

            var dataSource = context.DataSources[DataSource];
            var metadata = dataSource.Metadata[logicalName];
            var conditions = filterVisitor.Conditions.ToList();

            if (!TryGetDmlSchema(dataSource, metadata, querySpec, out var schema))
                return this;

            var constantScan = new ConstantScanNode();

            foreach (var col in mappings)
                constantScan.Schema[col.Value.SplitMultiPartIdentifier().Last()] = new ColumnDefinition(schema[col.Value], true, false);

            // We can handle compound keys, but only if they are all ANDed together
            if (mappings.Count > 1 && filterVisitor.BinaryType == BooleanBinaryExpressionType.And)
            {
                var values = new Dictionary<string, ScalarExpression>();

                foreach (var mapping in mappings)
                {
                    var condition = conditions.FirstOrDefault(c => c.attribute == mapping.Value.SplitMultiPartIdentifier()[1]);
                    if (condition == null)
                        return this;

                    if (condition.@operator != @operator.eq)
                        return this;

                    var attribute = metadata.Attributes.Single(a => a.LogicalName == condition.attribute);
                    values[condition.attribute] = attribute.GetDmlValue(condition.value, dataSource);
                }

                constantScan.Values.Add(values);
                return constantScan;
            }

            // We can also handle multiple values for a single key being ORed together
            else if (mappings.Count == 1 &&
                conditions.All(c => c.attribute == metadata.PrimaryIdAttribute) &&
                conditions.All(c => c.@operator == @operator.eq || c.@operator == @operator.@in) &&
                (conditions.Count == 1 || filterVisitor.BinaryType == BooleanBinaryExpressionType.Or))
            {
                foreach (var condition in conditions)
                {
                    var attribute = metadata.Attributes.Single(a => a.LogicalName == condition.attribute);

                    if (condition.@operator == @operator.eq)
                    {
                        constantScan.Values.Add(new Dictionary<string, ScalarExpression> { [condition.attribute] = attribute.GetDmlValue(condition.value, dataSource) });
                    }
                    else if (condition.@operator == @operator.@in)
                    {
                        foreach (var value in condition.Items)
                            constantScan.Values.Add(new Dictionary<string, ScalarExpression> { [condition.attribute] = attribute.GetDmlValue(condition.value, dataSource) });
                    }
                }

                return constantScan;
            }

            return this;
        }

        private bool TryGetDmlSchema(DataSource dataSource, EntityMetadata metadata, QuerySpecification querySpec, out Dictionary<string, DataTypeReference> schema)
        {
            schema = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);

            foreach (var select in querySpec.SelectElements)
            {
                if (!(select is SelectScalarExpression scalar))
                    return false;

                if (!(scalar.Expression is ColumnReferenceExpression col))
                    return false;

                var attribute = metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(col.MultiPartIdentifier.Identifiers.Last().Value, StringComparison.OrdinalIgnoreCase));

                if (attribute == null)
                    return false;

                var type = attribute.GetAttributeSqlType(dataSource, false);
                var name = scalar.ColumnName?.Value ?? attribute.LogicalName;

                schema[name] = type;
            }

            return true;
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
                SelectStatement = SelectStatement
            };
        }
    }
}
