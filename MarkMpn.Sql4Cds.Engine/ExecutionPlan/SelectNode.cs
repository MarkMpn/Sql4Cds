using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Converts a data stream to a full data table
    /// </summary>
    class SelectNode : BaseNode, ISingleSourceExecutionPlanNode, IDataReaderExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        [Category("Select")]
        [Description("The columns that should be included in the query results")]
        [DisplayName("Column Set")]
        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        /// <summary>
        /// The data source to select from
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public override TimeSpan Duration => _timer.Duration;

        public override int ExecutionCount => _executionCount;

        public IDataReader Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            var schema = Source.GetSchema(dataSources, parameterTypes);
            var source = Source.Execute(dataSources, options, parameterTypes, parameterValues);
            return new SelectDataReader(ColumnSet, _timer, schema, source, parameterValues);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;

            FoldFetchXmlColumns(Source, ColumnSet, dataSources, parameterTypes);
            FoldMetadataColumns(Source, ColumnSet);

            ExpandWildcardColumns(dataSources, parameterTypes);

            if (Source is AliasNode alias)
            {
                var aliasColumns = alias.ColumnSet.ToDictionary(col => alias.Alias + "." + col.OutputColumn, col => col.SourceColumn);

                foreach (var col in ColumnSet)
                    col.SourceColumn = aliasColumns[col.SourceColumn];

                Source = alias.Source;
                Source.Parent = this;
            }

            return new[] { this };
        }

        internal static void FoldFetchXmlColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet, IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (source is FetchXmlScan fetchXml)
            {
                if (!dataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                    throw new NotSupportedQueryFragmentException("Missing datasource " + fetchXml.DataSource);

                // Check if there are any aliases we can apply to the source FetchXml
                var schema = fetchXml.GetSchema(dataSources, parameterTypes);
                var hasStar = columnSet.Any(col => col.AllColumns && col.SourceColumn == null);
                var aliasStars = new HashSet<string>(columnSet.Where(col => col.AllColumns && col.SourceColumn != null).Select(col => col.SourceColumn.Replace(".*", "")).Distinct(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

                foreach (var col in columnSet)
                {
                    if (col.AllColumns)
                    {
                        if (col.SourceColumn == null)
                        {
                            // Add an all-attributes to the main entity and all link-entities
                            fetchXml.Entity.AddItem(new allattributes());

                            foreach (var link in fetchXml.Entity.GetLinkEntities())
                            {
                                if (link.SemiJoin)
                                    continue;

                                link.AddItem(new allattributes());
                            }
                        }
                        else if (!hasStar)
                        {
                            // Only add an all-attributes to the appropriate entity/link-entity
                            if (col.SourceColumn.Replace(".*", "").Equals(fetchXml.Alias, StringComparison.OrdinalIgnoreCase))
                            {
                                fetchXml.Entity.AddItem(new allattributes());
                            }
                            else
                            {
                                var link = fetchXml.Entity.FindLinkEntity(col.SourceColumn.Replace(".*", ""));
                                link.AddItem(new allattributes());
                            }
                        }
                    }
                    else if (!hasStar)
                    {
                        // Only fold individual columns down to the FetchXML if there is no corresponding all-attributes
                        var parts = col.SourceColumn.Split('.');

                        if (parts.Length == 1 || !aliasStars.Contains(parts[0]))
                        {
                            var sourceCol = col.SourceColumn;
                            schema.ContainsColumn(sourceCol, out sourceCol);
                            fetchXml.AddAttribute(sourceCol, null, dataSource.Metadata, out _, out _);
                        }
                    }
                }

                // Finally, check what aliases we can fold down to the FetchXML.
                // Ignore:
                // 1. columns that have more than 1 alias
                // 2. aliases that are invalid for FetchXML
                // 3. attributes that are included via an <all-attributes/>
                if (!hasStar)
                {
                    var aliasedColumns = columnSet
                        .Select(c =>
                        {
                            var sourceCol = c.SourceColumn;
                            schema.ContainsColumn(sourceCol, out sourceCol);

                            return new { Mapping = c, SourceColumn = sourceCol, Alias = c.OutputColumn };
                        })
                        .GroupBy(c => c.SourceColumn, StringComparer.OrdinalIgnoreCase)
                        .Where(g => g.Count() == 1) // Don't fold aliases if there are multiple aliases for the same source column
                        .Select(g => g.Single())
                        .GroupBy(c => c.Alias, StringComparer.OrdinalIgnoreCase)
                        .Where(g => g.Count() == 1) // Don't fold aliases if there are multiple columns using the same alias
                        .Select(g => g.Single())
                        .Where(c =>
                        {
                            var parts = c.SourceColumn.Split('.');

                            if (parts.Length > 1 && aliasStars.Contains(parts[0]))
                                return false; // Don't fold aliases if we're using an <all-attributes/>

                            if (c.Alias.Equals(parts.Last(), StringComparison.OrdinalIgnoreCase))
                                return false; // Don't fold aliases if we're using the original source name

                            if (!FetchXmlScan.IsValidAlias(c.Alias))
                                return false; // Don't fold aliases if they contain invalid characters

                            return true;
                        })
                        .Select(c =>
                        {
                            var attr = fetchXml.AddAttribute(c.SourceColumn, null, dataSource.Metadata, out _, out var linkEntity);
                            return new { Mapping = c.Mapping, SourceColumn = c.SourceColumn, Alias = c.Alias, Attr = attr, LinkEntity = linkEntity };
                        })
                        .Where(c =>
                        {
                            var items = c.LinkEntity?.Items ?? fetchXml.Entity.Items;

                            // Don't fold the alias if there's also a sort on the same attribute, as it breaks paging
                            // https://markcarrington.dev/2019/12/10/inside-fetchxml-pt-4-order/#sorting_&_aliases
                            if (items != null && items.OfType<FetchOrderType>().Any(order => order.attribute == c.Attr.name) && fetchXml.AllPages)
                                return false;

                            return true;
                        })
                        .ToList();

                    foreach (var aliasedColumn in aliasedColumns)
                    {
                        aliasedColumn.Attr.alias = aliasedColumn.Alias;
                        aliasedColumn.Mapping.SourceColumn = aliasedColumn.SourceColumn.Split('.')[0] + "." + aliasedColumn.Alias;
                    }
                }
            }
        }

        private void FoldMetadataColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet)
        {
            if (source is MetadataQueryNode metadata)
            {
                // Check if there are any wildcard columns we can apply to the source metadata query
                var hasStar = columnSet.Any(col => col.AllColumns && col.SourceColumn == null);
                var aliasStars = new HashSet<string>(columnSet.Where(col => col.AllColumns && col.SourceColumn != null).Select(col => col.SourceColumn.Replace(".*", "")).Distinct(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

                if (metadata.MetadataSource.HasFlag(MetadataSource.Entity) && (hasStar || aliasStars.Contains(metadata.EntityAlias)))
                {
                    if (metadata.Query.Properties == null)
                        metadata.Query.Properties = new MetadataPropertiesExpression();

                    metadata.Query.Properties.AllProperties = true;
                }

                if (metadata.MetadataSource.HasFlag(MetadataSource.Attribute) && (hasStar || aliasStars.Contains(metadata.AttributeAlias)))
                {
                    if (metadata.Query.AttributeQuery == null)
                        metadata.Query.AttributeQuery = new AttributeQueryExpression();

                    if (metadata.Query.AttributeQuery.Properties == null)
                        metadata.Query.AttributeQuery.Properties = new MetadataPropertiesExpression();

                    metadata.Query.AttributeQuery.Properties.AllProperties = true;
                }

                if ((metadata.MetadataSource.HasFlag(MetadataSource.OneToManyRelationship) && (hasStar || aliasStars.Contains(metadata.OneToManyRelationshipAlias))) ||
                    (metadata.MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship) && (hasStar || aliasStars.Contains(metadata.ManyToOneRelationshipAlias))) ||
                    (metadata.MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship) && (hasStar || aliasStars.Contains(metadata.ManyToManyRelationshipAlias))))
                {
                    if (metadata.Query.RelationshipQuery == null)
                        metadata.Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (metadata.Query.RelationshipQuery.Properties == null)
                        metadata.Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    metadata.Query.RelationshipQuery.Properties.AllProperties = true;
                }
            }
        }

        public void ExpandWildcardColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            ExpandWildcardColumns(Source, ColumnSet, dataSources, parameterTypes);
        }

        internal static void ExpandWildcardColumns(IDataExecutionPlanNodeInternal source, List<SelectColumn> columnSet, IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            // Expand any AllColumns
            if (columnSet.Any(col => col.AllColumns))
            {
                var schema = source.GetSchema(dataSources, parameterTypes);
                var expanded = new List<SelectColumn>();

                foreach (var col in columnSet)
                {
                    if (!col.AllColumns)
                    {
                        expanded.Add(col);
                        continue;
                    }

                    foreach (var src in schema.Schema.Keys.Where(k => col.SourceColumn == null || k.StartsWith(col.SourceColumn.Replace("*", ""), StringComparison.OrdinalIgnoreCase)).OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
                    {
                        expanded.Add(new SelectColumn
                        {
                            SourceColumn = src,
                            OutputColumn = src.Split('.').Last()
                        });
                    }
                }

                columnSet.Clear();
                columnSet.AddRange(expanded);
            }
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in ColumnSet.Select(c => c.SourceColumn + (c.AllColumns ? ".*" : "")))
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override string ToString()
        {
            return "SELECT";
        }

        public object Clone()
        {
            var clone = new SelectNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql,
                Index = Index,
                Length = Length
            };

            clone.ColumnSet.AddRange(ColumnSet);
            clone.Source.Parent = clone;

            return clone;
        }
    }

    /// <summary>
    /// Describes a column generated by a <see cref="SelectNode"/>
    /// </summary>
    class SelectColumn
    {
        /// <summary>
        /// The name of the column in the source data
        /// </summary>
        public string SourceColumn { get; set; }

        /// <summary>
        /// The expression that provides the value for the column
        /// </summary>
        /// <remarks>
        /// Used for error reporting only
        /// </remarks>
        [Browsable(false)]
        public TSqlFragment SourceExpression { get; set; }

        /// <summary>
        /// The requested name of the column in the output data
        /// </summary>
        public string OutputColumn { get; set; }

        /// <summary>
        /// Indicates this is a placeholder for all columns from the source data (SELECT *)
        /// </summary>
        [Browsable(false)]
        public bool AllColumns { get; set; }
    }
}
