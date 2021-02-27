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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var source = Source.Execute(org, metadata, options, parameterTypes, parameterValues);
            var schema = GetSchema(metadata, parameterTypes);
            IOrderedEnumerable<Entity> sortedSource;

            if (Sorts[0].SortOrder == SortOrder.Descending)
                sortedSource = source.OrderByDescending(e => Sorts[0].Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
            else
                sortedSource = source.OrderBy(e => Sorts[0].Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);

            for (var i = 1; i < Sorts.Count; i++)
            {
                if (Sorts[i].SortOrder == SortOrder.Descending)
                    sortedSource = sortedSource.ThenByDescending(e => Sorts[i].Expression.GetValue(e, schema,parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
                else
                    sortedSource = sortedSource.ThenBy(e => Sorts[i].Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
            }

            return sortedSource;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);

            if (Source is FetchXmlScan fetchXml)
            {
                var schema = Source.GetSchema(metadata, parameterTypes);
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

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var sortColumns = Sorts.SelectMany(s => s.Expression.GetColumns()).Distinct();

            foreach (var col in sortColumns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }
    }
}
