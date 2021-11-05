using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Converts a data stream to a full data table
    /// </summary>
    class SelectNode : BaseNode, ISingleSourceExecutionPlanNode, IDataSetExecutionPlanNode
    {
        private TimeSpan _duration;
        private int _executionCount;

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
        public IDataExecutionPlanNode Source { get; set; }

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public override TimeSpan Duration => _duration;

        public override int ExecutionCount => _executionCount;

        public DataTable Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;
            var startTime = DateTime.Now;

            try
            {
                var schema = Source.GetSchema(dataSources, parameterTypes);
                var dataTable = new DataTable();

                foreach (var col in ColumnSet)
                {
                    var sourceName = col.SourceColumn;
                    if (!schema.ContainsColumn(sourceName, out sourceName))
                        throw new QueryExecutionException($"Missing column {col.SourceColumn}") { Node = this };

                    var dataCol = dataTable.Columns.Add(col.PhysicalOutputColumn, schema.Schema[sourceName]);
                    dataCol.Caption = col.OutputColumn;
                }

                foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
                {
                    var row = dataTable.NewRow();

                    foreach (var col in ColumnSet)
                    {
                        if (!entity.Contains(col.SourceColumn))
                            throw new QueryExecutionException($"Missing column {col.SourceColumn}") { Node = this };

                        row[col.PhysicalOutputColumn] = entity[col.SourceColumn];
                    }

                    dataTable.Rows.Add(row);
                }

                return dataTable;
            }
            finally
            {
                var endTime = DateTime.Now;
                _duration += (endTime - startTime);
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public IRootExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;

            FoldFetchXmlColumns(Source, ColumnSet, dataSources, parameterTypes);
            FoldMetadataColumns(Source, ColumnSet);

            ExpandWildcardColumns(dataSources, parameterTypes);

            // Ensure column names are unique
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var col in ColumnSet)
            {
                col.PhysicalOutputColumn = col.OutputColumn;

                if (!names.Add(col.PhysicalOutputColumn))
                {
                    var suffix = 1;

                    while (!names.Add(col.OutputColumn + suffix))
                        suffix++;

                    col.PhysicalOutputColumn += suffix;
                }
            }

            return this;
        }

        internal static void FoldFetchXmlColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet, IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            if (source is FetchXmlScan fetchXml)
            {
                if (!dataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                    throw new NotSupportedQueryFragmentException("Missing datasource " + fetchXml.DataSource);

                // Check if there are any aliases we can apply to the source FetchXml
                var schema = fetchXml.GetSchema(dataSources, parameterTypes);
                var processedSourceColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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
                            var attr = fetchXml.AddAttribute(sourceCol, null, dataSource.Metadata, out var added, out var linkEntity);

                            // Check if we can fold the alias down to the FetchXML too. Don't do this if the name isn't valid for FetchXML
                            if (sourceCol != col.SourceColumn)
                                parts = col.SourceColumn.Split('.');

                            if (!col.OutputColumn.Equals(parts.Last(), StringComparison.OrdinalIgnoreCase) && FetchXmlScan.IsValidAlias(col.OutputColumn))
                            {
                                if (added || (!processedSourceColumns.Contains(sourceCol) && !fetchXml.IsAliasReferenced(attr.alias)))
                                {
                                    // Don't fold the alias if there's also a sort on the same attribute, as it breaks paging
                                    // https://markcarrington.dev/2019/12/10/inside-fetchxml-pt-4-order/#sorting_&_aliases
                                    var items = linkEntity?.Items ?? fetchXml.Entity.Items;

                                    if (items == null || !items.OfType<FetchOrderType>().Any(order => order.attribute == attr.name) || !fetchXml.AllPages)
                                        attr.alias = col.OutputColumn;
                                }

                                col.SourceColumn = sourceCol.Split('.')[0] + "." + (attr.alias ?? attr.name);
                            }

                            processedSourceColumns.Add(sourceCol);
                        }
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

        public void ExpandWildcardColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            ExpandWildcardColumns(Source, ColumnSet, dataSources, parameterTypes);
        }

        internal static void ExpandWildcardColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet, IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
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

        IRootExecutionPlanNode IRootExecutionPlanNode.FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this.FoldQuery(dataSources, options, parameterTypes);
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
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
        /// The requested name of the column in the output data
        /// </summary>
        public string OutputColumn { get; set; }

        /// <summary>
        /// A unique name for the column in the output data
        /// </summary>
        [Browsable(false)]
        public string PhysicalOutputColumn { get; set; }

        /// <summary>
        /// Indicates this is a placeholder for all columns from the source data (SELECT *)
        /// </summary>
        [Browsable(false)]
        public bool AllColumns { get; set; }
    }
}
