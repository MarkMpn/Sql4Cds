using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an OFFSET/FETCH clause
    /// </summary>
    class OffsetFetchNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The number of records to skip
        /// </summary>
        [Category("Offset")]
        [Description("The number of records to skip")]
        public ScalarExpression Offset { get; set; }

        /// <summary>
        /// The number of records to retrieve
        /// </summary>
        [Category("Offset")]
        [Description("The number of records to retrieve")]
        public ScalarExpression Fetch { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);
            var offset = SqlTypeConverter.ChangeType<int>(Offset.Compile(expressionCompilationContext)(expressionExecutionContext));
            var fetch = SqlTypeConverter.ChangeType<int>(Fetch.Compile(expressionCompilationContext)(expressionExecutionContext));

            if (offset < 0)
                throw new QueryExecutionException(Sql4CdsError.Create(10742, null));

            if (fetch <= 0)
                throw new QueryExecutionException(Sql4CdsError.Create(10744, null));

            return Source.Execute(context)
                .Skip(offset)
                .Take(fetch);
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return Source.GetSchema(context);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            var expressionCompilationContext = new ExpressionCompilationContext(context.DataSources, context.Options, null, null, null);
            
            if (!Offset.IsConstantValueExpression(expressionCompilationContext, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(expressionCompilationContext, out var fetchLiteral))
                return this;

            if (Source is FetchXmlScan fetchXml)
            {
                var expressionExecutionContext = new ExpressionExecutionContext(expressionCompilationContext);
                var offset = SqlTypeConverter.ChangeType<int>(offsetLiteral.Compile(expressionCompilationContext)(expressionExecutionContext));
                var count = SqlTypeConverter.ChangeType<int>(fetchLiteral.Compile(expressionCompilationContext)(expressionExecutionContext));
                var page = offset / count;

                if (page * count == offset && count <= 5000)
                {
                    fetchXml.FetchXml.count = count.ToString();
                    fetchXml.FetchXml.page = (page + 1).ToString();
                    fetchXml.AllPages = false;
                    return fetchXml;
                }
            }

            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var sourceCount = Source.EstimateRowsOut(context);
            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);

            if (!Offset.IsConstantValueExpression(expressionCompilationContext, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(expressionCompilationContext, out var fetchLiteral))
                return sourceCount;

            var offset = Int32.Parse(offsetLiteral.Value, CultureInfo.InvariantCulture);
            var fetch = Int32.Parse(fetchLiteral.Value, CultureInfo.InvariantCulture);

            return new RowCountEstimateDefiniteRange(0, Math.Max(0, Math.Min(fetch, sourceCount.Value - offset)));
        }

        public override string ToString()
        {
            return "Offset";
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Offset.GetVariables().Union(Fetch.GetVariables());
        }

        public override object Clone()
        {
            var clone = new OffsetFetchNode
            {
                Fetch = Fetch,
                Offset = Offset,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
