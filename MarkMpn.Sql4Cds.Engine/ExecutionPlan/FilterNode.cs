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
    /// Applies a filter to the data stream
    /// </summary>
    public class FilterNode : BaseNode
    {
        /// <summary>
        /// The filter to apply
        /// </summary>
        public BooleanExpression Filter { get; set; }

        /// <summary>
        /// The data source to select from
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata, parameterTypes);

            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                if (Filter.GetValue(entity, schema, parameterTypes, parameterValues))
                    yield return entity;
            }
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
            Source.Parent = this;
            var schema = Source.GetSchema(metadata, parameterTypes);

            if (Source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
            {
                if (TranslateCriteria(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, out var fetchFilter))
                {
                    fetchXml.Entity.AddItem(fetchFilter);
                    return fetchXml;
                }

                // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                Filter = ExtractFetchXMLFilters(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, out fetchFilter);

                if (fetchFilter != null)
                    fetchXml.Entity.AddItem(fetchFilter);
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in Filter.GetColumns())
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        private BooleanExpression ExtractFetchXMLFilters(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, object[] items, out filter filter)
        {
            filter = null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            filter lhsFilter;
            if (TranslateCriteria(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out lhsFilter))
                bin.FirstExpression = null;
            else
                bin.FirstExpression = ExtractFetchXMLFilters(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out lhsFilter);

            filter rhsFilter;
            if (TranslateCriteria(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out rhsFilter))
                bin.SecondExpression = null;
            else
                bin.SecondExpression = ExtractFetchXMLFilters(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out rhsFilter);

            if (lhsFilter != null && rhsFilter != null)
            {
                filter = new filter
                {
                    Items = new object[]
                    {
                        lhsFilter,
                        rhsFilter
                    }
                };
            }
            else
            {
                filter = lhsFilter ?? rhsFilter;
            }

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize) * 8 / 10;
        }
    }
}
