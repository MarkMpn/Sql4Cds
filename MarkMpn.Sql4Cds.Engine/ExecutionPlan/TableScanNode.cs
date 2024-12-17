﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
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
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (DataColumn col in table.Columns)
            {
                var netType = col.DataType;

                if (!typeof(INullable).IsAssignableFrom(netType))
                    netType = SqlTypeConverter.NetToSqlType(netType);

                var sqlType = netType.ToSqlType(context.PrimaryDataSource);
                var colDefinition = (IColumnDefinition)new ColumnDefinition(sqlType, col.AllowDBNull, false);

                if (col.ColumnName == primaryKey)
                    colDefinition = colDefinition.Invisible();

                var baseColName = col.ColumnName.EscapeIdentifier();
                var qualifiedColName = table.TableName.EscapeIdentifier() + "." + baseColName;
                cols.Add(qualifiedColName, colDefinition);

                if (!aliases.TryGetValue(baseColName, out var a))
                {
                    a = new List<string>();
                    aliases[baseColName] = a;
                }

                ((List<string>)a).Add(qualifiedColName);
            }

            return new NodeSchema(
                cols,
                aliases,
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
                return new RowCountEstimate(0);

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
                    var colName = table.TableName.EscapeIdentifier() + "." + col.ColumnName.EscapeIdentifier();

                    if (value == DBNull.Value)
                        entity[colName] = SqlTypeConverter.GetNullValue(col.DataType);
                    else if (col == table.PrimaryKey.Single())
                        entity[colName] = (SqlInt64)(long)value;
                    else
                        entity[colName] = value;
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

        public override string ToString()
        {
            return "Table Scan\r\n" + TableName.EscapeIdentifier();
        }
    }
}