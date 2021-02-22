using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Sorts the data in the data stream
    /// </summary>
    public class SortNode : BaseNode
    {
        /// <summary>
        /// The sorts to apply
        /// </summary>
        public List<ExpressionWithSortOrder> Sorts { get; } = new List<ExpressionWithSortOrder>();

        /// <summary>
        /// The data source to sort
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            var source = Source.Execute(org, metadata, options, parameterValues);
            var schema = GetSchema(metadata);
            IOrderedEnumerable<Entity> sortedSource;

            if (Sorts[0].SortOrder == SortOrder.Descending)
                sortedSource = source.OrderByDescending(e => Sorts[0].Expression.GetValue(e, schema), CaseInsensitiveObjectComparer.Instance);
            else
                sortedSource = source.OrderBy(e => Sorts[0].Expression.GetValue(e, schema), CaseInsensitiveObjectComparer.Instance);

            for (var i = 1; i < Sorts.Count; i++)
            {
                if (Sorts[i].SortOrder == SortOrder.Descending)
                    sortedSource = sortedSource.ThenByDescending(e => Sorts[i].Expression.GetValue(e, schema), CaseInsensitiveObjectComparer.Instance);
                else
                    sortedSource = sortedSource.ThenBy(e => Sorts[i].Expression.GetValue(e, schema), CaseInsensitiveObjectComparer.Instance);
            }

            return sortedSource;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return Source.GetSchema(metadata);
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return Sorts
                .SelectMany(s => s.GetColumns())
                .Distinct();
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source = Source.MergeNodeDown(metadata, options);

            if (Source is FetchXmlScan fetchXml)
            {
                var schema = fetchXml.GetSchema(metadata);
                var entity = fetchXml.Entity;
                var items = entity.Items;

                foreach (var sortOrder in Sorts)
                {
                    if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                        return this;

                    if (!schema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                        return this;

                    var parts = sortCol.Split('.');
                    var entityName = parts[0];
                    var attrName = parts[1];

                    var fetchSort = new FetchOrderType { attribute = attrName, descending = sortOrder.SortOrder == SortOrder.Descending };
                    if (entityName == fetchXml.Alias)
                    {
                        if (items != entity.Items)
                            return this;

                        entity.AddItem(fetchSort);
                        items = entity.Items;
                    }
                    else
                    {
                        var linkEntity = FetchXmlExtensions.FindLinkEntity(items, entityName);
                        if (linkEntity == null)
                            return this;

                        linkEntity.AddItem(fetchSort);
                        items = linkEntity.Items;
                    }
                }

                return Source;
            }

            return this;
        }
    }
}
