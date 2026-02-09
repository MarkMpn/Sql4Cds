using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenRowsetBulkNode : BaseDataNode
    {
        /// <summary>
        /// The alias to apply to the results of the OPENROWSET function
        /// </summary>
        [Category("OpenRowset")]
        [Description("The alias to apply to the results of the OPENROWSET function")]
        public string Alias { get; set; }

        /// <summary>
        /// The format of the file to load
        /// </summary>
        [Category("OpenRowset")]
        [Description("The format of the file to load")]
        public string Format { get; set; }

        /// <summary>
        /// Indicates if the file is to be read as a single value
        /// </summary>
        [Category("OpenRowset")]
        [Description("Indicates if the file is to be read as a single value")]
        public BulkInsertOptionKind? SingleOption { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            throw new NotImplementedException();
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            throw new NotImplementedException();
        }

        public override object Clone()
        {
            var clone = new OpenRowsetBulkNode();
            clone.Alias = Alias;
            clone.Format = Format;
            clone.SingleOption = SingleOption;
            return clone;
        }
    }
}
