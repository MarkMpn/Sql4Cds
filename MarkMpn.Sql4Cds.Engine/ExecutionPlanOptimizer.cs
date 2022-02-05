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
        public ExecutionPlanOptimizer(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            DataSources = dataSources;
            Options = options;
            ParameterTypes = parameterTypes;
        }

        public IDictionary<string, DataSource> DataSources { get; }

        public IQueryExecutionOptions Options { get; }

        public IDictionary<string, DataTypeReference> ParameterTypes { get; }

        /// <summary>
        /// Optimizes an execution plan
        /// </summary>
        /// <param name="node">The root node of the execution plan</param>
        /// <param name="hints">Any optimizer hints to apply</param>
        /// <returns>A new execution plan node</returns>
        public IRootExecutionPlanNodeInternal Optimize(IRootExecutionPlanNodeInternal node, IList<OptimizerHint> hints)
        {
            // Move any additional operators down to the FetchXml
            node = node.FoldQuery(DataSources, Options, ParameterTypes, hints);

            // Ensure all required columns are added to the FetchXML
            node.AddRequiredColumns(DataSources, ParameterTypes, new List<string>());

            //Sort the items in the FetchXml nodes to match examples in documentation
            SortFetchXmlElements(node);

            return node;
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
