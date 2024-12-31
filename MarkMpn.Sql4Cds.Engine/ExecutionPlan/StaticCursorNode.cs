using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class StaticCursorNode : CursorBaseNode
    {
        [Browsable(false)]
        public DataTable TempTable { get; set; }

        public override object Clone()
        {
            throw new NotImplementedException();
        }

        public static StaticCursorNode FromQuery(NodeCompilationContext context, IRootExecutionPlanNodeInternal query)
        {
            // Cache the results of the query in a temporary table, indexed by the row number
            var tempTable = new DataTable("#" + Guid.NewGuid().ToString("N"));
            tempTable.PrimaryKey = new[] { tempTable.Columns.Add("RowNumber", typeof(long)) };

            // Track how the fetch query should name each of the columns
            List<SelectColumn> columns;

            INodeSchema schema;
            IDataExecutionPlanNodeInternal sourceNode;

            if (query is SelectNode select)
            {
                sourceNode = select.Source;
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
                // TODO: Make SqlNode implement IDataExecutionPlanNodeInternal and refactor BaseDmlNode accordingly
                throw new NotImplementedException();
                //sourceNode = sql;

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

            // Define the temporary table in the current context so we can continue building the rest of the query
            context.Session.TempDb.Tables.Add(tempTable.Clone());

            // Add the operators to calculate the row number and set up the insert statement
            var segmentColumn = context.GetExpressionName("Segment");
            var rowNumberColumn = context.GetExpressionName("RowNumber");
            var populationQuery = new InsertNode
            {
                LogicalName = tempTable.TableName,
                Source = new SequenceProjectNode
                {
                    Source = new SegmentNode
                    {
                        Source = sourceNode,
                        GroupBy = new List<string>(),
                        SegmentColumn = segmentColumn
                    },
                    SegmentColumn = segmentColumn,
                    DefinedValues =
                    {
                        [rowNumberColumn] = new Aggregate
                        {
                            AggregateType = AggregateType.RowNumber
                        }
                    }
                },
                Accessors = new List<AttributeAccessor>()
            };

            // Set up the accessors to populate the temporary table
            populationQuery.Accessors.Add(new AttributeAccessor
            {
                TargetAttribute = tempTable.PrimaryKey[0].ColumnName,
                Accessor = eec => eec.Entity[rowNumberColumn],
                SourceAttributes = new[] { rowNumberColumn }
            });

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
                FetchQuery = fetchQuery,
                TempTable = tempTable
            };

            return cursor;
        }
    }
}
