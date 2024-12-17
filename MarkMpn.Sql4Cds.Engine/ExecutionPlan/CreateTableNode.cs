using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class CreateTableNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        /// <summary>
        /// The number of the first line of the statement
        /// </summary>
        [Browsable(false)]
        public int LineNumber { get; set; }

        /// <summary>
        /// The table that will be created
        /// </summary>
        [Browsable(false)]
        public DataTable TableDefinition { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            recordsAffected = -1;
            message = null;

            try
            {
                try
                {
                    context.Session.TempDb.Tables.Add(TableDefinition.Clone());
                }
                catch (DuplicateNameException)
                {
                    throw new QueryExecutionException(Sql4CdsError.DuplicateObjectName(null, TableDefinition.TableName));
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public object Clone()
        {
            return new CreateTableNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                TableDefinition = TableDefinition
            };
        }

        public static CreateTableNode FromStatement(CreateTableStatement createTable)
        {
            // Check for a whole range of CREATE TABLE options we don't support
            var errors = new List<Sql4CdsError>();
            var suggestions = new HashSet<string>();

            if (createTable.AsEdge)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "AS EDGE"));

            if (createTable.AsFileTable)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "AS FILETABLE"));

            if (createTable.AsNode)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "AS NODE"));

            if (createTable.CtasColumns != null && createTable.CtasColumns.Count > 0)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "CREATE TABLE AS SELECT"));

            if (createTable.FederationScheme != null)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "FEDERATED"));

            if (createTable.FileStreamOn != null)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "FILESTREAM_ON"));

            if (createTable.OnFileGroupOrPartitionScheme != null)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "ON FILEGROUP"));

            if (createTable.SelectStatement != null)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "CREATE TABLE AS SELECT"));

            if (createTable.TextImageOn != null)
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "TEXTIMAGE_ON"));

            foreach (var option in createTable.Options)
                errors.Add(Sql4CdsError.UnsupportedStatement(option, option.ToNormalizedSql()));

            // We can create tables in tempdb. Don't try to create tables in Dataverse itself for now.
            if (createTable.SchemaObjectName.DatabaseIdentifier != null)
            {
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "Database name"));
                suggestions.Add("Only temporary tables are supported");
            }
            else if (createTable.SchemaObjectName.SchemaIdentifier != null)
            {
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "Schema name"));
                suggestions.Add("Only temporary tables are supported");
            }
            else if (!createTable.SchemaObjectName.BaseIdentifier.Value.StartsWith("#"))
            {
                errors.Add(Sql4CdsError.UnsupportedStatement(createTable, "Non-temporary table"));
                suggestions.Add("Only temporary tables are supported");
            }

            // Build the corresponding DataTable
            var table = new DataTable(createTable.SchemaObjectName.BaseIdentifier.Value);

            foreach (var col in createTable.Definition.ColumnDefinitions)
            {
                var dataCol = table.Columns.Add(col.ColumnIdentifier.Value);
                dataCol.DataType = col.DataType.ToNetType(out var sqlType);
                dataCol.AllowDBNull = !col.Constraints.Any(c => c is NullableConstraintDefinition n && !n.Nullable);
                dataCol.MaxLength = sqlType.SqlDataTypeOption.IsStringType() ? sqlType.GetSize() : dataCol.MaxLength;
                
                // TODO: More validation
            }

            // Create a hidden primary key column
            var pk = table.Columns.Add($"PK_{Guid.NewGuid():N}");
            pk.DataType = typeof(long);
            pk.AutoIncrement = true;
            table.PrimaryKey = new[] { pk };

            if (errors.Count > 0)
                throw new NotSupportedQueryFragmentException(errors.ToArray(), null) { Suggestion = String.Join(Environment.NewLine, suggestions) };

            return new CreateTableNode
            {
                TableDefinition = table
            };
        }
    }
}
