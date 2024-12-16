using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class TableScanNode : BaseDataNode
    {
        public string TableName { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var table = context.Session.TempDb.Tables[TableName];

            if (table == null)
                throw new QueryExecutionException(Sql4CdsError.InvalidObjectName(new Identifier { Value = TableName }));

            var cols = new ColumnList();
            var primaryKey = table.PrimaryKey.Length == 1 ? table.PrimaryKey[0].ColumnName : null;
            
            foreach (DataColumn col in table.Columns)
            {
                var netType = col.DataType;
                var sqlType = netType.ToSqlType(context.PrimaryDataSource);
                cols.Add(col.ColumnName, new ColumnDefinition(sqlType, col.AllowDBNull, false));
            }

            return new NodeSchema(
                cols,
                null,
                primaryKey,
                null);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var table = context.Session.TempDb.Tables[TableName];

            if (table == null)
                throw new QueryExecutionException(Sql4CdsError.InvalidObjectName(new Identifier { Value = TableName }));

            return new RowCountEstimate(table.Rows.Count);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var table = context.Session.TempDb.Tables[TableName];

            if (table == null)
                throw new QueryExecutionException(Sql4CdsError.InvalidObjectName(new Identifier { Value = TableName }));

            foreach (DataRow row in table.Rows)
            {
                var entity = new Entity();

                foreach (DataColumn col in table.Columns)
                {
                    var value = row[col];

                    if (value == DBNull.Value)
                        entity[col.ColumnName] = SqlTypeConverter.GetNullValue(col.DataType);
                    else
                        entity[col.ColumnName] = value;
                }

                yield return entity;
            }
        }

        public override object Clone()
        {
            return new TableScanNode
            {
                TableName = TableName
            };
        }
    }
}
