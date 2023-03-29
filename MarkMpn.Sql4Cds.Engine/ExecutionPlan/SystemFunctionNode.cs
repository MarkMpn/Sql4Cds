using System;
using System.Collections.Generic;
using System.ComponentModel;
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
                        schema: new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase)
                        {
                            ["name"] = DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault),
                            ["description"] = DataTypeHelpers.NVarChar(1000, dataSource.DefaultCollation, CollationLabel.CoercibleDefault),
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
                    break;
            }

            throw new NotSupportedException("Unsupported function " + SystemFunction);
        }
    }

    enum SystemFunction
    {
        fn_helpcollations
    }
}
