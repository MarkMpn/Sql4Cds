﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns only one entity per unique combinatioh of values in specified columns
    /// </summary>
    class DistinctNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The columns to consider
        /// </summary>
        [Category("Distinct")]
        [Description("The columns to consider")]
        public List<string> Columns { get; } = new List<string>();

        /// <summary>
        /// The data source to take the values from
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var distinct = new HashSet<Entity>(new DistinctEqualityComparer(Columns));

            foreach (var entity in Source.Execute(context))
            {
                if (distinct.Add(entity))
                    yield return entity;
            }
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = Source.GetSchema(context);

            // If this is a distinct list of one column we know the values in that column will be unique
            if (Columns.Count == 1)
                schema = new NodeSchema(
                    primaryKey: Columns[0],
                    schema: schema.Schema,
                    aliases: schema.Aliases,
                    sortOrder: schema.SortOrder);

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // We can have a sequence of Distinct - Concatenate - Distinct - Concatenate when we have multiple UNION statements
            // We can collapse this to a single Distinct - Concatenate with all the sources from the various Concatenate nodes
            CombineConcatenateSources();

            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            // Remove any duplicated column names
            for (var i = Columns.Count - 1; i >= 0; i--)
            {
                if (Columns.IndexOf(Columns[i]) < i)
                    Columns.RemoveAt(i);
            }

            // If one of the fields to include in the DISTINCT calculation is the primary key, there is no possibility of duplicate
            // rows so we can discard the distinct node
            var schema = Source.GetSchema(context);

            if (!String.IsNullOrEmpty(schema.PrimaryKey) && Columns.Contains(schema.PrimaryKey, StringComparer.OrdinalIgnoreCase))
                return Source;

            // If we know the source doesn't have more than one record, there is no possibility of duplicate
            // rows so we can discard the distinct node
            if (Source.EstimateRowsOut(context) is RowCountEstimateDefiniteRange range && range.Maximum <= 1)
                return Source;

            if (Source is FetchXmlScan fetch)
            {
                // Can't apply DISTINCT to aggregate queries
                if (fetch.FetchXml.aggregate)
                    return this;

                // Can't apply DISTINCT to TOP with an order by
                if (!String.IsNullOrEmpty(fetch.FetchXml.top) && fetch.Entity.Items?.OfType<FetchOrderType>().Any() == true)
                    return this;

                // Can't apply DISTINCT to audit.objectid
                // https://github.com/MarkMpn/Sql4Cds/issues/519
                if (fetch.Entity.name == "audit" && Columns.Any(col => col.StartsWith(fetch.Alias.EscapeIdentifier() + ".objectid")))
                    return this;

                var metadata = context.Session.DataSources[fetch.DataSource].Metadata;

                // Can't apply DISTINCT to partylist attributes
                // https://github.com/MarkMpn/Sql4Cds/issues/528
                foreach (var column in Columns)
                {
                    if (!schema.ContainsColumn(column, out var normalized))
                        continue;

                    fetch.AddAttribute(normalized, null, metadata, out _, out var linkEntity, out var attrMetadata, out var isVirtual);

                    if (attrMetadata?.AttributeType == AttributeTypeCode.PartyList)
                        return this;
                }

                fetch.FetchXml.distinct = true;
                fetch.FetchXml.distinctSpecified = true;
                var virtualAttr = false;

                // Remove any existing <all-attributes /> listed in the FetchXML so the DISTINCT doesn't get accidentially applied
                // across more columns than expected
                fetch.RemoveAllAttributes();

                // Keep track of all other <attributes /> so we can remove any that haven't been referenced by the DISTINCT
                var existingAttributes = fetch.GetAttributes();

                // Ensure there is a sort order applied to avoid paging issues
                if (fetch.Entity.Items == null || !fetch.Entity.Items.OfType<FetchOrderType>().Any())
                {
                    // Sort by each attribute. Make sure we only add one sort per attribute, taking virtual attributes
                    // into account (i.e. don't attempt to sort on statecode and statecodename)
                    var sortedAttributes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var column in Columns)
                    {
                        if (!schema.ContainsColumn(column, out var normalized))
                            continue;

                        var attr = fetch.AddAttribute(normalized, null, metadata, out _, out var linkEntity, out _, out var isVirtual);

                        existingAttributes.Remove(attr);

                        var nameParts = normalized.SplitMultiPartIdentifier();

                        virtualAttr |= isVirtual;

                        if (!sortedAttributes.Add(linkEntity?.alias + "." + attr.name))
                            continue;

                        if (linkEntity == null)
                            fetch.Entity.AddItem(new FetchOrderType { attribute = attr.name });
                        else
                            linkEntity.AddItem(new FetchOrderType { attribute = attr.name });
                    }
                }

                foreach (var attr in existingAttributes)
                    fetch.RemoveAttribute(attr);

                // Virtual entity providers are unreliable - still fold the DISTINCT to the fetch but keep
                // this node to ensure the DISTINCT is applied if the provider doesn't support it.
                if (!virtualAttr && !fetch.IsUnreliableVirtualEntityProvider)
                    return fetch;

                schema = Source.GetSchema(context);
            }

            if (Source is ConcatenateNode concat)
            {
                // We can try to DISTINCT the sources going into the concat node to reduce the amount of data being retrieved
                // Only worth doing this if that DistinctNode can be folded into its sources, as we've still got to distinct
                // the results of the concat in this node too
                for (var i = 0; i < concat.Sources.Count; i++)
                {
                    var sourceDistinct = new DistinctNode { Source = concat.Sources[i] };
                    var canFold = true;

                    foreach (var col in Columns)
                    {
                        var concatCol = concat.ColumnSet.SingleOrDefault(c => c.OutputColumn == col);

                        if (concatCol == null)
                        {
                            canFold = false;
                            break;
                        }

                        sourceDistinct.Columns.Add(concatCol.SourceColumns[i]);
                    }

                    if (canFold)
                    {
                        // Fold the DISTINCT operation into the source. If it can't be folded, leave the original source
                        // as-is rather than inserting the additional DistinctNode here.
                        sourceDistinct.FoldQuery(context, hints);
                    }
                }
            }

            // If the data is already sorted by all the distinct columns we can use a stream aggregate instead.
            // We don't mind what order the columns are sorted in though, so long as the distinct columns form a
            // prefix of the sort order.
            var requiredSorts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var col in Columns)
            {
                if (!schema.ContainsColumn(col, out var column))
                    return this;

                requiredSorts.Add(column);
            }

            if (!schema.IsSortedBy(requiredSorts))
                return this;

            var aggregate = new StreamAggregateNode { Source = Source };
            Source.Parent = aggregate;

            for (var i = 0; i < requiredSorts.Count; i++)
                aggregate.GroupBy.Add(schema.SortOrder[i].ToColumnReference());

            return aggregate;
        }

        private void CombineConcatenateSources()
        {
            if (!(Source is ConcatenateNode concat))
                return;

            for (var i = 0; i < concat.Sources.Count; i++)
            {
                bool folded;

                do
                {
                    folded = false;

                    if (concat.Sources[i] is DistinctNode distinct)
                    {
                        concat.Sources[i] = distinct.Source;
                        folded = true;
                    }

                    if (concat.Sources[i] is ConcatenateNode subConcat)
                    {
                        for (var j = 0; j < subConcat.Sources.Count; j++)
                        {
                            concat.Sources.Insert(i + j + 1, subConcat.Sources[j]);

                            foreach (var col in concat.ColumnSet)
                            {
                                foreach (var subCol in subConcat.ColumnSet)
                                {
                                    if (col.SourceColumns[i] == subCol.OutputColumn)
                                    {
                                        col.SourceColumns.Insert(i + j + 1, subCol.SourceColumns[j]);
                                        col.SourceExpressions.Insert(i + j + 1, subCol.SourceExpressions[j]);
                                        break;
                                    }
                                }
                            }
                        }

                        concat.Sources.RemoveAt(i);

                        foreach (var col in concat.ColumnSet)
                        {
                            col.SourceColumns.RemoveAt(i);
                            col.SourceExpressions.RemoveAt(i);
                        }

                        i--;
                        folded = false;
                    }
                } while (folded);
            }
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            foreach (var col in Columns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            // TODO: Is there any metadata available that could help give a better estimate for this?
            // Maybe get the schema and check if any of the columns included in the DISTINCT list are the
            // primary key and if so return the entire count, if some are optionset then there's a known list
            var totalCount = Source.EstimateRowsOut(context);

            if (totalCount is RowCountEstimateDefiniteRange range && range.Maximum == 1)
                return totalCount;

            return new RowCountEstimate(totalCount.Value * 8 / 10);
        }

        public override object Clone()
        {
            var clone = new DistinctNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;
            clone.Columns.AddRange(Columns);

            return clone;
        }
    }
}
