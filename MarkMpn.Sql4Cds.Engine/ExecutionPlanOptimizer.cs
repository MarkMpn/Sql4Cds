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
        public ExecutionPlanOptimizer(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Metadata = metadata;
            Options = options;
        }

        public IAttributeMetadataCache Metadata { get; }

        public IQueryExecutionOptions Options { get; }

        /// <summary>
        /// Optimizes an execution plan
        /// </summary>
        /// <param name="node">The root node of the execution plan</param>
        /// <returns>A new execution plan node</returns>
        public IExecutionPlanNode Optimize(IExecutionPlanNode node)
        {
            // Move any additional operators down to the FetchXml
            node = node.FoldQuery(Metadata, Options, null);

            // Ensure all required columns are added to the FetchXML
            node.AddRequiredColumns(Metadata, null, new List<string>());

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
