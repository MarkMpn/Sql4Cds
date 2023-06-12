using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class SystemFunctionNode : BaseDataNode
    {
        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The alias for the data source
        /// </summary>
        [Category("System Function")]
        [Description("The alias for the data source")]
        public string Alias { get; set; }

        /// <summary>
        /// The name of the function to execute
        /// </summary>
        [Category("System Function")]
        [Description("The name of the function to execute")]
        public SystemFunction SystemFunction { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override object Clone()
        {
            return new SystemFunctionNode
            {
                DataSource = DataSource,
                Alias = Alias,
                SystemFunction = SystemFunction
            };
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var dataSource = context.DataSources[DataSource];

            switch (SystemFunction)
            {
                case SystemFunction.fn_helpcollations:
                    return new NodeSchema(
                        schema: new ColumnList
                        {
                            [PrefixWithAlias("name")] = DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault),
                            [PrefixWithAlias("description")] = DataTypeHelpers.NVarChar(1000, dataSource.DefaultCollation, CollationLabel.CoercibleDefault),
                        },
                        aliases: null,
                        primaryKey: null,
                        sortOrder: null,
                        notNullColumns: new[] { "name", "description" });
            }

            throw new NotSupportedException("Unsupported function " + SystemFunction);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return new RowCountEstimate(100);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var dataSource = context.DataSources[DataSource];

            switch (SystemFunction)
            {
                case SystemFunction.fn_helpcollations:
                    foreach (var coll in Collation.GetAllCollations())
                    {
                        yield return new Entity
                        {
                            [PrefixWithAlias("name")] = dataSource.DefaultCollation.ToSqlString(coll.Name),
                            [PrefixWithAlias("description")] = dataSource.DefaultCollation.ToSqlString(coll.Description)
                        };
                    }
                    break;

                default:
                    throw new NotSupportedException("Unsupported function " + SystemFunction);
            }
        }

        private string PrefixWithAlias(string columnName)
        {
            if (String.IsNullOrEmpty(Alias))
                return columnName;

            return Alias + "." + columnName;
        }
    }

    enum SystemFunction
    {
        fn_helpcollations
    }
}
