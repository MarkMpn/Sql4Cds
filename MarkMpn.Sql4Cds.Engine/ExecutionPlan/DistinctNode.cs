using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns only one entity per unique combinatioh of values in specified columns
    /// </summary>
    class DistinctNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        class DistinctKey
        {
            private List<object> _values;
            private readonly int _hashCode;

            public DistinctKey(Entity entity, List<string> columns)
            {
                _values = columns.Select(col => entity[col]).ToList();

                _hashCode = 0;

                foreach (var val in _values)
                {
                    if (val == null)
                        continue;

                    _hashCode ^= StringComparer.CurrentCultureIgnoreCase.GetHashCode(val);
                }
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            public override bool Equals(object obj)
            {
                var other = (DistinctKey)obj;

                for (var i = 0; i < _values.Count; i++)
                {
                    if (_values[i] == null && other._values[i] == null)
                        continue;

                    if (_values[i] == null || other._values[i] == null)
                        return false;

                    if (StringComparer.CurrentCultureIgnoreCase.Compare(_values[i], other._values[i]) != 0)
                        return false;
                }

                return true;
            }
        }

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
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var distinct = new HashSet<DistinctKey>();

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                var key = new DistinctKey(entity, Columns);

                if (distinct.Add(key))
                    yield return entity;
            }
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);

            // If this is a distinct list of one column we know the values in that column will be unique
            if (Columns.Count == 1)
                schema.PrimaryKey = Columns[0];

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
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

                        if (!sortedAttributes.Add(normalized))
                            continue;

                        var parts = normalized.Split('.');
                        if (parts.Length != 2)
                            continue;

                        if (parts[0].Equals(fetch.Alias, StringComparison.OrdinalIgnoreCase))
                        {
                            var attr = dataSources[fetch.DataSource].Metadata[fetch.Entity.name].Attributes.SingleOrDefault(a => a.LogicalName.Equals(parts[1], StringComparison.OrdinalIgnoreCase));

                            if (attr == null)
                                continue;

                            if (attr.AttributeOf != null && !sortedAttributes.Add(parts[0] + "." + attr.AttributeOf))
                                continue;

                            fetch.Entity.AddItem(new FetchOrderType { attribute = parts[1] });
                        }
                        else
                        {
                            var linkEntity = fetch.Entity.FindLinkEntity(parts[0]);
                            var attr = dataSources[fetch.DataSource].Metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName.Equals(parts[1], StringComparison.OrdinalIgnoreCase));

                            if (attr == null)
                                continue;

                            if (attr.AttributeOf != null && !sortedAttributes.Add(parts[0] + "." + attr.AttributeOf))
                                continue;

                            linkEntity.AddItem(new FetchOrderType { attribute = parts[1] });
                        }
                    }
                }

                return fetch;
            }

            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in Columns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            // TODO: Is there any metadata available that could help give a better estimate for this?
            // Maybe get the schema and check if any of the columns included in the DISTINCT list are the
            // primary key and if so return the entire count, if some are optionset then there's a known list
            var totalCount = Source.EstimateRowsOut(dataSources, options, parameterTypes);
            return totalCount * 8 / 10;
        }
    }
}
