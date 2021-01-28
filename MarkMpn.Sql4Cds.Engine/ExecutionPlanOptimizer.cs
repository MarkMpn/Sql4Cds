using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;

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
            // Move any additional operators down to the FetchXml
            node = MergeNodeDown(metadata, node);

            // Push required column names down to leaf node data sources so only the required data is exported
            PushColumnsDown(metadata, node, new List<string>());

            //Sort the items in the FetchXml nodes to match examples in documentation
            SortFetchXmlElements(node);

            return node;
        }

        private static void SortFetchXmlElements(IExecutionPlanNode node)
        {
            if (node is FetchXmlScan fetchXml)
                SortFetchXmlElements(fetchXml.FetchXml.Items);

            foreach (var source in node.GetSources())
                SortFetchXmlElements(source);
        }

        private static void SortFetchXmlElements(object[] items)
        {
            if (items == null)
                return;

            items.StableSort(new FetchXmlElementComparer());

            foreach (var entity in items.OfType<FetchEntityType>())
                SortFetchXmlElements(entity.Items);

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                SortFetchXmlElements(linkEntity.Items);
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

        private static IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IExecutionPlanNode node)
        {
            if (node is SelectNode select)
            {
                select.Source = MergeNodeDown(metadata, select.Source);
            }
            else if (node is MergeJoinNode mergeJoin && (mergeJoin.JoinType == QualifiedJoinType.Inner || mergeJoin.JoinType == QualifiedJoinType.LeftOuter))
            {
                mergeJoin.LeftSource = MergeNodeDown(metadata, mergeJoin.LeftSource);
                mergeJoin.RightSource = MergeNodeDown(metadata, mergeJoin.RightSource);

                var left = mergeJoin.LeftSource;
                if (left is SortNode leftSort && leftSort.IgnoreForFetchXmlFolding)
                    left = leftSort.Source;

                var right = mergeJoin.RightSource;
                if (right is SortNode rightSort && rightSort.IgnoreForFetchXmlFolding)
                    right = rightSort.Source;

                if (left is FetchXmlScan leftFetch && right is FetchXmlScan rightFetch)
                {
                    var leftEntity = leftFetch.FetchXml.Items.OfType<FetchEntityType>().Single();
                    var rightEntity = rightFetch.FetchXml.Items.OfType<FetchEntityType>().Single();

                    var leftSchema = left.GetSchema(metadata);
                    var rightSchema = right.GetSchema(metadata);
                    var leftAttribute = mergeJoin.LeftAttribute.GetColumnName();
                    if (!leftSchema.Schema.ContainsKey(leftAttribute) && leftSchema.Aliases.TryGetValue(leftAttribute, out var leftAlias) && leftAlias.Count == 1)
                        leftAttribute = leftAlias[0];
                    var rightAttribute = mergeJoin.RightAttribute.GetColumnName();
                    if (!rightSchema.Schema.ContainsKey(rightAttribute) && rightSchema.Aliases.TryGetValue(rightAttribute, out var rightAlias) && rightAlias.Count == 1)
                        rightAttribute = rightAlias[0];
                    var leftAttributeParts = leftAttribute.Split('.');
                    var rightAttributeParts = rightAttribute.Split('.');
                    if (leftAttributeParts.Length != 2)
                        return node;
                    if (rightAttributeParts.Length != 2)
                        return node;
                    if (!rightAttributeParts[0].Equals(rightFetch.Alias))
                        return node;

                    var rightLinkEntity = new FetchLinkEntityType
                    {
                        alias = rightFetch.Alias,
                        name = rightEntity.name,
                        linktype = mergeJoin.JoinType == QualifiedJoinType.Inner ? "inner" : "outer",
                        from = rightAttributeParts[1],
                        to = leftAttributeParts[1],
                        Items = rightEntity.Items
                    };

                    // Find where the two FetchXml documents should be merged together and return the merged version
                    if (leftAttributeParts[0].Equals(leftFetch.Alias))
                    {
                        if (leftEntity.Items == null)
                            leftEntity.Items = new object[] { rightLinkEntity };
                        else
                            leftEntity.Items = leftEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                    }
                    else
                    {
                        var leftLinkEntity = FindLinkEntity(leftFetch.FetchXml.Items.OfType<FetchEntityType>().Single().Items, leftAttributeParts[0]);

                        if (leftLinkEntity == null)
                            return node;

                        if (leftLinkEntity.Items == null)
                            leftLinkEntity.Items = new object[] { rightLinkEntity };
                        else
                            leftLinkEntity.Items = leftLinkEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                    }

                    return leftFetch;
                }
            }

            return node;
        }

        private static FetchLinkEntityType FindLinkEntity(object[] items, string alias)
        {
            if (items == null)
                return null;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (linkEntity.alias.Equals(alias, StringComparison.OrdinalIgnoreCase))
                    return linkEntity;

                var childMatch = FindLinkEntity(linkEntity.Items, alias);

                if (childMatch != null)
                    return childMatch;
            }

            return null;
        }
    }
}
