using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class AdaptiveIndexSpoolNode : BaseDataNode
    {
        private IndexSpoolNode _indexSpool;

        public AdaptiveIndexSpoolNode()
        {
            _indexSpool = new IndexSpoolNode();
        }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal SpooledSource
        {
            get => _indexSpool.Source;
            set => _indexSpool.Source = value;
        }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal UnspooledSource { get; set; }

        /// <summary>
        /// The column in the data source to create an index on
        /// </summary>
        [Category("Index Spool")]
        [DisplayName("Key Column")]
        [Description("The column in the data source to create an index on")]
        public string KeyColumn
        {
            get => _indexSpool.KeyColumn;
            set => _indexSpool.KeyColumn = value;
        }

        /// <summary>
        /// The name of the parameter to use for seeking in the index
        /// </summary>
        [Category("Index Spool")]
        [DisplayName("Seek Value")]
        [Description("The name of the parameter to use for seeking in the index")]
        public string SeekValue
        {
            get => _indexSpool.SeekValue;
            set => _indexSpool.SeekValue = value;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            UnspooledSource.AddRequiredColumns(context, requiredColumns);
            _indexSpool.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            _indexSpool.FoldQuery(context, hints);
            UnspooledSource = UnspooledSource.FoldQuery(context, hints);
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return UnspooledSource.GetSchema(context);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return new[] { UnspooledSource, SpooledSource };
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return _indexSpool.EstimateRowsOut(context);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            IDataExecutionPlanNodeInternal source;

            if (ExecutionCount < 10)
                source = UnspooledSource;
            else
                source = _indexSpool;

            foreach (var result in source.Execute(context))
                yield return result;
        }

        public override string ToString()
        {
            return "Index Spool\r\n(Adaptive)";
        }

        public override object Clone()
        {
            return new AdaptiveIndexSpoolNode
            {
                _indexSpool = (IndexSpoolNode)_indexSpool.Clone(),
                UnspooledSource = (IDataExecutionPlanNodeInternal)UnspooledSource.Clone(),
                KeyColumn = KeyColumn,
                SeekValue = SeekValue
            };
        }
    }
}
