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
    class StaticCursorNode : CursorDeclarationBaseNode
    {
        private SqlInt64 _rowNumber;

        [Browsable(false)]
        public DataTable TempTable { get; set; }

        [Browsable(false)]
        public string RowNumberVariable { get; set; }

        public override IDmlQueryExecutionPlanNode Open(NodeExecutionContext context)
        {
            // Create the temp table
            if (!context.Session.TempDb.Tables.Contains(TempTable.TableName))
                context.Session.TempDb.Tables.Add(TempTable.Clone());

            // Position the cursor before the first row
            _rowNumber = 0;

            return base.Open(context);
        }

        public override IDataReaderExecutionPlanNode Fetch(NodeExecutionContext context, FetchOrientation orientation, Func<ExpressionExecutionContext, object> rowOffset)
        {
            // Increment the row number
            switch (orientation)
            {
                case FetchOrientation.Next:
                    _rowNumber += 1;
                    break;

                case FetchOrientation.Prior:
                    if (_rowNumber > 0)
                        _rowNumber -= 1;
                    break;

                case FetchOrientation.First:
                    _rowNumber = 1;
                    break;

                case FetchOrientation.Last:
                    _rowNumber = context.Session.TempDb.Tables[TempTable.TableName].Rows.Count;
                    break;

                case FetchOrientation.Absolute:
                    _rowNumber = (SqlInt32)rowOffset(new ExpressionExecutionContext(context));

                    if (_rowNumber < 0)
                        _rowNumber = context.Session.TempDb.Tables[TempTable.TableName].Rows.Count + _rowNumber;

                    if (_rowNumber < 0)
                        _rowNumber = 0;
                    break;

                case FetchOrientation.Relative:
                    _rowNumber += (SqlInt32)rowOffset(new ExpressionExecutionContext(context));

                    if (_rowNumber < 0)
                        _rowNumber = 0;
                    break;

                default:
                    throw new NotSupportedException($"Invalid fetch orientation '{orientation}'");
            }

            context.ParameterTypes[RowNumberVariable] = DataTypeHelpers.BigInt;
            context.ParameterValues[RowNumberVariable] = _rowNumber;

            return base.Fetch(context, orientation, rowOffset);
        }

        public override void Close(NodeExecutionContext context)
        {
            base.Close(context);

            // Remove the temp table
            context.Session.TempDb.Tables.Remove(TempTable.TableName);

            // Remove the row number variable
            context.ParameterTypes.Remove(RowNumberVariable);
            context.ParameterValues.Remove(RowNumberVariable);
        }

        public override object Clone()
        {
            return new StaticCursorNode
            {
                CursorName = CursorName,
                TempTable = TempTable.Clone(),
                RowNumberVariable = RowNumberVariable,
                PopulationQuery = (IDmlQueryExecutionPlanNode)PopulationQuery.Clone(),
                FetchQuery = (IDataReaderExecutionPlanNode)FetchQuery.Clone(),
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Scope = Scope,
                Sql = Sql
            };
        }

        public static StaticCursorNode FromQuery(NodeCompilationContext context, IRootExecutionPlanNodeInternal query)
        {
            // Cache the results of the query in a temporary table, indexed by the row number
            var tempTable = new DataTable("#" + Guid.NewGuid().ToString("N"));
            tempTable.PrimaryKey = new[] { tempTable.Columns.Add("RowNumber", typeof(long)) };

            // Track how the fetch query should name each of the columns
            var columns = new List<SelectColumn>();

            INodeSchema schema;
            IDataExecutionPlanNodeInternal sourceNode;

            if (query is SelectNode select)
            {
                sourceNode = select.Source;
                schema = select.Source.GetSchema(context);

                foreach (var column in select.ColumnSet)
                {
                    var sourceCol = schema.Schema[column.SourceColumn];
                    var colName = $"Col{tempTable.Columns.Count}";
                    var dataCol = tempTable.Columns.Add(colName, sourceCol.Type.ToNetType(out _));
                    dataCol.AllowDBNull = sourceCol.IsNullable;
                    columns.Add(new SelectColumn { SourceColumn = colName, OutputColumn = column.OutputColumn });
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
                DataSource = context.Options.PrimaryDataSource,
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
                Accessor = eec => (long)(SqlInt64)eec.Entity[rowNumberColumn],
                SourceAttributes = new[] { rowNumberColumn }
            });

            foreach (var col in columns)
            {
                populationQuery.Accessors.Add(new AttributeAccessor
                {
                    TargetAttribute = col.SourceColumn,
                    Accessor = eec => eec.Entity[col.OutputColumn],
                    SourceAttributes = new[] { col.OutputColumn }
                });
            }

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
                fetchQuery.ColumnSet.Add(new SelectColumn { SourceColumn = tempTable.TableName + "." + col.SourceColumn, OutputColumn = col.OutputColumn });

            var cursor = new StaticCursorNode
            {
                PopulationQuery = populationQuery,
                FetchQuery = fetchQuery,
                TempTable = tempTable,
                RowNumberVariable = rowNumberVariable
            };

            return cursor;
        }
    }
}
