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
            node = node.MergeNodeDown(Metadata, Options);

            // Push required column names down to leaf node data sources so only the required data is exported
            PushColumnsDown(node, new List<string>());

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

        private void PushColumnsDown(IExecutionPlanNode node, List<string> columns)
        {
            if (node is FetchXmlScan fetchXmlNode)
            {
                // Add columns to FetchXml
                var entity = fetchXmlNode.FetchXml.Items.OfType<FetchEntityType>().Single();

                foreach (var col in columns)
                {
                    var parts = col.Split('.');

                    if (parts.Length != 2)
                        continue;

                    var attr = parts[1] == "*" ? (object) new allattributes() : new FetchAttributeType { name = parts[1] };

                    if (fetchXmlNode.Alias.Equals(parts[0], StringComparison.OrdinalIgnoreCase))
                    {
                        if (entity.Items == null || !entity.Items.OfType<FetchAttributeType>().Any(a => (a.alias ?? a.name) == parts[1]))
                            entity.AddItem(attr);
                    }
                    else
                    {
                        var linkEntity = entity.FindLinkEntity(parts[0]);

                        if (linkEntity != null && (linkEntity.Items == null || !linkEntity.Items.OfType<FetchAttributeType>().Any(a => (a.alias ?? a.name) == parts[1])))
                            linkEntity.AddItem(attr);
                    }
                }
                
                fetchXmlNode.FetchXml = fetchXmlNode.FetchXml;
            }

            var schema = node.GetSchema(Metadata);
            var sourceRequiredColumns = new List<string>(columns);

            foreach (var col in node.GetRequiredColumns())
            {
                if (schema.Aliases.TryGetValue(col, out var aliasedCols))
                {
                    foreach (var aliasedCol in aliasedCols)
                    {
                        if (!sourceRequiredColumns.Contains(aliasedCol))
                            sourceRequiredColumns.Add(aliasedCol);
                    }
                }
                else if (!sourceRequiredColumns.Contains(col))
                {
                    sourceRequiredColumns.Add(col);
                }
            }

            foreach (var source in node.GetSources())
                PushColumnsDown(source, sourceRequiredColumns);
        }
    }
}
