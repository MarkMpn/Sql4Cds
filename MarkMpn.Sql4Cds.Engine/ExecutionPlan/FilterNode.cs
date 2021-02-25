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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata);

            foreach (var entity in Source.Execute(org, metadata, options, parameterValues))
            {
                if (Filter.GetValue(entity, schema))
                    yield return entity;
            }
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
            return Filter.GetColumns();
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source = Source.MergeNodeDown(metadata, options);

            if (Source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
            {
                var schema = fetchXml.GetSchema(metadata);
                if (TranslateCriteria(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, out var fetchFilter))
                {
                    fetchXml.Entity.AddItem(fetchFilter);
                    return fetchXml;
                }

                // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                Filter = ExtractFetchXMLFilters(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, out fetchFilter);

                if (fetchFilter != null)
                    fetchXml.Entity.AddItem(fetchFilter);
            }

            return this;
        }

        private BooleanExpression ExtractFetchXMLFilters(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, out filter filter)
        {
            filter = null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            filter lhsFilter;
            if (TranslateCriteria(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out lhsFilter))
                bin.FirstExpression = null;
            else
                bin.FirstExpression = ExtractFetchXMLFilters(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out lhsFilter);

            filter rhsFilter;
            if (TranslateCriteria(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out rhsFilter))
                bin.SecondExpression = null;
            else
                bin.SecondExpression = ExtractFetchXMLFilters(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out rhsFilter);

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
    }
}
