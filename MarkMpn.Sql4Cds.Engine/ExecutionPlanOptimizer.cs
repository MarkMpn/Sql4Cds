using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Optimizes an execution plan once it has been built
    /// </summary>
    class ExecutionPlanOptimizer
    {
        public ExecutionPlanOptimizer(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, bool compileConditions)
        {
            DataSources = dataSources;
            Options = options;
            ParameterTypes = parameterTypes;
            CompileConditions = compileConditions;
        }

        public IDictionary<string, DataSource> DataSources { get; }

        public IQueryExecutionOptions Options { get; }

        public IDictionary<string, DataTypeReference> ParameterTypes { get; }

        public bool CompileConditions { get; }

        /// <summary>
        /// Optimizes an execution plan
        /// </summary>
        /// <param name="node">The root node of the execution plan</param>
        /// <param name="hints">Any optimizer hints to apply</param>
        /// <returns>A new execution plan node</returns>
        public IRootExecutionPlanNodeInternal[] Optimize(IRootExecutionPlanNodeInternal node, IList<OptimizerHint> hints)
        {
            if (!CompileConditions)
            {
                if (hints == null)
                    hints = new List<OptimizerHint>();

                hints.Add(new ConditionalNode.DoNotCompileConditionsHint());
            }

            var context = new NodeCompilationContext(DataSources, Options, ParameterTypes);

            // Move any additional operators down to the FetchXml
            var bypassOptimization = hints != null && hints.OfType<UseHintList>().Any(list => list.Hints.Any(h => h.Value.Equals("DEBUG_BYPASS_OPTIMIZATION", StringComparison.OrdinalIgnoreCase)));
            var nodes = bypassOptimization ? new[] { node } : node.FoldQuery(context, hints);

            foreach (var n in nodes)
            {
                // Ensure all required columns are added to the FetchXML
                n.AddRequiredColumns(context, new List<string>());

                // Sort the items in the FetchXml nodes to match examples in documentation
                SortFetchXmlElements(n);

                // Let the nodes know that folding is now finished so they can do any internal tidy-up
                if (!bypassOptimization)
                    MarkComplete(n);
            }

            return nodes;
        }

        private void MarkComplete(IExecutionPlanNodeInternal node)
        {
            foreach (var child in node.GetSources())
                MarkComplete((IExecutionPlanNodeInternal)child);

            node.FinishedFolding();
        }

        private void SortFetchXmlElements(IExecutionPlanNode node)
        {
            if (node is FetchXmlScan fetchXml)
                SortFetchXmlElements(fetchXml.FetchXml.Items);

            foreach (var source in node.GetSources())
                SortFetchXmlElements(source);
        }

        private void SortFetchXmlElements(object[] items)
        {
            if (items == null)
                return;

            items.StableSort(new FetchXmlElementComparer());

            foreach (var entity in items.OfType<FetchEntityType>())
                SortFetchXmlElements(entity.Items);

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                SortFetchXmlElements(linkEntity.Items);
        }
    }
}
