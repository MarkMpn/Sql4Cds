using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Applies a filter to the data stream
    /// </summary>
    public class FilterNode : BaseNode, ISingleSourceExecutionPlanNode
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

            if (Source is MetadataQueryNode meta)
            {
                if (TranslateCriteria(Filter, meta, out var entityFilter, out var attributeFilter))
                {
                    meta.Query.AddFilter(entityFilter);

                    if (attributeFilter != null && meta.Query.AttributeQuery == null)
                        meta.Query.AttributeQuery = new AttributeQueryExpression();

                    meta.Query.AttributeQuery.AddFilter(attributeFilter);

                    return meta;
                }

                // If the criteria are ANDed, see if any of the individual conditions can be translated to the metadata query
                Filter = ExtractMetadataFilters(Filter, meta, out entityFilter, out attributeFilter);

                meta.Query.AddFilter(entityFilter);

                if (attributeFilter != null && meta.Query.AttributeQuery == null)
                    meta.Query.AttributeQuery = new AttributeQueryExpression();

                meta.Query.AttributeQuery.AddFilter(attributeFilter);
            }

            // If we've got a filter matching a column and a variable (key lookup in a nested loop) from a table spool, replace it with a index spool
            if (Source is TableSpoolNode tableSpool)
            {
                if (ExtractKeyLookupFilter(Filter, out var filter, out var indexColumn, out var seekVariable) && schema.ContainsColumn(indexColumn, out indexColumn))
                {
                    var spoolSource = tableSpool.Source;

                    // Index spool requires non-null key values
                    if (indexColumn != schema.PrimaryKey)
                    {
                        spoolSource = new FilterNode
                        {
                            Source = tableSpool.Source,
                            Filter = new BooleanIsNullExpression
                            {
                                Expression = indexColumn.ToColumnReference(),
                                IsNot = true
                            }
                        }.FoldQuery(metadata, options, parameterTypes);
                    }

                    Source = new IndexSpoolNode
                    {
                        Source = spoolSource,
                        KeyColumn = indexColumn,
                        SeekValue = seekVariable
                    };

                    if (filter == null)
                        return Source;

                    Filter = filter;
                }
            }

            return this;
        }

        private bool ExtractKeyLookupFilter(BooleanExpression filter, out BooleanExpression remainingFilter, out string indexColumn, out string seekVariable)
        {
            remainingFilter = null;
            indexColumn = null;
            seekVariable = null;

            if (filter is BooleanComparisonExpression cmp && cmp.ComparisonType == BooleanComparisonType.Equals)
            {
                if (cmp.FirstExpression is ColumnReferenceExpression col1 && 
                    cmp.SecondExpression is VariableReference var2)
                {
                    indexColumn = col1.GetColumnName();
                    seekVariable = var2.Name;
                    return true;
                }
                else if (cmp.FirstExpression is VariableReference var1 &&
                    cmp.SecondExpression is ColumnReferenceExpression col2)
                {
                    indexColumn = col2.GetColumnName();
                    seekVariable = var1.Name;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                if (ExtractKeyLookupFilter(bin.FirstExpression, out remainingFilter, out indexColumn, out seekVariable))
                {
                    if (remainingFilter == null)
                    {
                        remainingFilter = bin.SecondExpression;
                    }
                    else
                    {
                        bin.FirstExpression = remainingFilter;
                        remainingFilter = bin;
                    }

                    return true;
                }
                else if (ExtractKeyLookupFilter(bin.SecondExpression, out remainingFilter, out indexColumn, out seekVariable))
                {
                    if (remainingFilter == null)
                    {
                        remainingFilter = bin.FirstExpression;
                    }
                    else
                    {
                        bin.SecondExpression = remainingFilter;
                        remainingFilter = bin;
                    }

                    return true;
                }
            }

            return false;
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
