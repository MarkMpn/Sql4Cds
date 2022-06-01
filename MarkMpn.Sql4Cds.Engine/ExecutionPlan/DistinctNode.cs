using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

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

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var distinct = new HashSet<Entity>(new DistinctEqualityComparer(Columns));

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                if (distinct.Add(entity))
                    yield return entity;
            }
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);

            // If this is a distinct list of one column we know the values in that column will be unique
            if (Columns.Count == 1)
                schema = new NodeSchema(schema) { PrimaryKey = Columns[0] };

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;

            // Remove any duplicated column names
            for (var i = Columns.Count - 1; i >= 0; i--)
            {
                if (Columns.IndexOf(Columns[i]) < i)
                    Columns.RemoveAt(i);
            }

            // If one of the fields to include in the DISTINCT calculation is the primary key, there is no possibility of duplicate
            // rows so we can discard the distinct node
            var schema = Source.GetSchema(dataSources, parameterTypes);

            if (!String.IsNullOrEmpty(schema.PrimaryKey) && Columns.Contains(schema.PrimaryKey, StringComparer.OrdinalIgnoreCase))
                return Source;

            if (Source is FetchXmlScan fetch)
            {
                fetch.FetchXml.distinct = true;
                fetch.FetchXml.distinctSpecified = true;
                var metadata = dataSources[fetch.DataSource].Metadata;
                var virtualAttr = false;

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

                        var attr = fetch.AddAttribute(normalized, null, metadata, out _, out var linkEntity);

                        if (attr.name != normalized.Split('.')[1])
                            virtualAttr = true;

                        if (!sortedAttributes.Add(linkEntity?.alias + "." + attr.name))
                            continue;

                        if (linkEntity == null)
                            fetch.Entity.AddItem(new FetchOrderType { attribute = attr.name });
                        else
                            linkEntity.AddItem(new FetchOrderType { attribute = attr.name });
                    }
                }

                if (!virtualAttr)
                    return fetch;

                schema = Source.GetSchema(dataSources, parameterTypes);
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

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in Columns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            // TODO: Is there any metadata available that could help give a better estimate for this?
            // Maybe get the schema and check if any of the columns included in the DISTINCT list are the
            // primary key and if so return the entire count, if some are optionset then there's a known list
            var totalCount = Source.EstimateRowsOut(dataSources, options, parameterTypes);

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
