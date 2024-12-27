using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class StaticCursorNode : CursorBaseNode
    {
        public override object Clone()
        {
            throw new NotImplementedException();
        }

        public static IRootExecutionPlanNodeInternal[] FromQuery(NodeCompilationContext context, IRootExecutionPlanNodeInternal query)
        {
            var nodes = new List<IRootExecutionPlanNodeInternal>();

            // Cache the results of the query in a temporary table, indexed by the row number
            var tempTable = new DataTable("#" + Guid.NewGuid().ToString("N"));
            tempTable.PrimaryKey = new[] { tempTable.Columns.Add("RowNumber", typeof(int)) };

            // Track how the fetch query should name each of the columns
            List<SelectColumn> columns;

            INodeSchema schema;

            if (query is SelectNode select)
            {
                schema = select.Source.GetSchema(context);
                columns = select.ColumnSet;

                foreach (var column in select.ColumnSet)
                {
                    var sourceCol = schema.Schema[column.SourceColumn];
                    var dataCol = tempTable.Columns.Add(column.SourceColumn, sourceCol.Type.ToNetType(out _));
                    dataCol.AllowDBNull = sourceCol.IsNullable;
                }
            }
            else if (query is SqlNode sql)
            {
                // SQL schema can have repeated or blank column names, so create new ones based on simple column indexes
                var sqlSchema = sql.GetSchema(context);
                columns = new List<SelectColumn>();
                var index = 0;

                foreach (var column in sqlSchema.Schema)
                {
                    var sourceCol = column.Value;
                    var colName = $"Col{index++}";
                    var dataCol = tempTable.Columns.Add(colName, sourceCol.Type.ToNetType(out _));
                    dataCol.AllowDBNull = sourceCol.IsNullable;
                    columns.Add(new SelectColumn { SourceColumn = colName, OutputColumn = column.Key });
                }
            }
            else
            {
                throw new NotSupportedException();
            }

            // Define the temporary table
            nodes.Add(new CreateTableNode { TableDefinition = tempTable });

            // TODO: Requires new operators for calculating the row number
            var populationQuery = new InsertNode
            {
                LogicalName = tempTable.TableName,
            };

            // Fetch query filters the results from the temporary table by the row number
            var rowNumberVariable = $"@{context.GetExpressionName()}";
            var fetchQuery = new SelectNode
            {
                Source = new FilterNode
                {
                    Source = new TableScanNode
                    {
                        TableName = tempTable.TableName
                    },
                    Filter = new BooleanComparisonExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = tempTable.TableName },
                                    new Identifier { Value = tempTable.PrimaryKey[0].ColumnName }
                                }
                            }
                        },
                        ComparisonType = BooleanComparisonType.Equals,
                        SecondExpression = new VariableReference { Name = rowNumberVariable }
                    }
                }
            };

            foreach (var col in columns)
                fetchQuery.ColumnSet.Add(col);

            var cursor = new StaticCursorNode
            {
                PopulationQuery = populationQuery,
                FetchQuery = fetchQuery
            };
            nodes.Add(cursor);

            return nodes.ToArray();
        }
    }
}
