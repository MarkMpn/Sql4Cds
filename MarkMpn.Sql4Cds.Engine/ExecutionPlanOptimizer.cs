using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Optimizes an execution plan once it has been built
    /// </summary>
    public static class ExecutionPlanOptimizer
    {
        /// <summary>
        /// Optimizes an execution plan
        /// </summary>
        /// <param name="node">The root node of the execution plan</param>
        /// <returns>A new execution plan node</returns>
        public static IExecutionPlanNode Optimize(IAttributeMetadataCache metadata, IExecutionPlanNode node)
        {
            // Push required column names down to leaf node data sources so only the required data is exported
            PushColumnsDown(metadata, node, new List<string>());

            // Move any additional operators down to the FetchXml
            node = MergeNodeDown(node);

            return node;
        }

        private static void PushColumnsDown(IAttributeMetadataCache metadata, IExecutionPlanNode node, List<string> columns)
        {
            if (node is FetchXmlScan fetchXmlNode)
            {
                // Add columns to FetchXml
                var entity = fetchXmlNode.FetchXml.Items.OfType<FetchEntityType>().Single();

                var items = new List<object>();
                if (entity.Items != null)
                    items.AddRange(entity.Items);

                foreach (var col in columns.Where(c => c.StartsWith(fetchXmlNode.Alias + ".")).Select(c => c.Split('.')[1]))
                {
                    if (col == "*")
                        items.Add(new allattributes());
                    else
                        items.Add(new FetchAttributeType { name = col });
                }

                if (items.OfType<allattributes>().Any())
                {
                    items.Clear();
                    items.Add(new allattributes());
                }

                entity.Items = items.ToArray();
                fetchXmlNode.FetchXml = fetchXmlNode.FetchXml;
            }

            var schema = node.GetSchema(metadata);
            var sourceRequiredColumns = new List<string>(columns);

            foreach (var col in node.GetRequiredColumns())
            {
                if (schema.Aliases.TryGetValue(col, out var aliasedCols))
                    sourceRequiredColumns.AddRange(aliasedCols);
                else
                    sourceRequiredColumns.Add(col);
            }

            foreach (var source in node.GetSources())
                PushColumnsDown(metadata, source, sourceRequiredColumns);
        }

        private static IExecutionPlanNode MergeNodeDown(IExecutionPlanNode node)
        {
            if (node is SelectNode select)
            {
                select.Source = MergeNodeDown(select.Source);
            }
            else if (node is MergeJoinNode mergeJoin)
            {
                mergeJoin.LeftSource = MergeNodeDown(mergeJoin.LeftSource);
                mergeJoin.RightSource = MergeNodeDown(mergeJoin.RightSource);

                var left = mergeJoin.LeftSource;
                if (left is SortNode leftSort && leftSort.IgnoreForFetchXmlFolding)
                    left = leftSort.Source;

                var right = mergeJoin.RightSource;
                if (right is SortNode rightSort && rightSort.IgnoreForFetchXmlFolding)
                    right = rightSort.Source;

                if (left is FetchXmlScan leftFetch && right is FetchXmlScan rightFetch)
                {
                    // TODO: Find where the two FetchXml documents should be merged together and return the merged version
                }
            }

            return node;
        }
    }
}
