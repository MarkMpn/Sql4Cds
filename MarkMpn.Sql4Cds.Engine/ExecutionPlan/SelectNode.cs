using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class SelectNode : BaseNode, ISingleSourceExecutionPlanNode, IDataSetExecutionPlanNode
    {
        private TimeSpan _duration;
        private int _executionCount;

        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        /// <summary>
        /// The data source to select from
        /// </summary>
        public IDataExecutionPlanNode Source { get; set; }

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public override TimeSpan Duration => _duration;

        public override int ExecutionCount => _executionCount;

        public DataTable Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;
            var startTime = DateTime.Now;

            try
            {
                var dataTable = new DataTable();

                foreach (var col in ColumnSet)
                    dataTable.Columns.Add(col.OutputColumn);

                foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
                {
                    var row = dataTable.NewRow();

                    foreach (var col in ColumnSet)
                    {
                        if (!entity.Contains(col.SourceColumn))
                            throw new QueryExecutionException($"Missing column {col.SourceColumn}");

                        row[col.OutputColumn] = entity[col.SourceColumn];
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

        public override IEnumerable<IDataExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            FoldFetchXmlColumns(Source, ColumnSet, metadata, parameterTypes);

            ExpandWildcardColumns(metadata, parameterTypes);

            // Ensure column names are unique
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var col in ColumnSet)
            {
                if (!names.Add(col.OutputColumn))
                {
                    var suffix = 1;

                    while (!names.Add(col.OutputColumn + suffix))
                        suffix++;

                    col.OutputColumn += suffix;
                }
            }

            return this;
        }

        internal static void FoldFetchXmlColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet, IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            if (source is FetchXmlScan fetchXml)
            {
                // Check if there are any aliases we can apply to the source FetchXml
                var schema = fetchXml.GetSchema(metadata, parameterTypes);
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
                                link.AddItem(new allattributes());
                        }
                        else if (!hasStar)
                        {
                            // Only add an all-attributes to the appropriate entity/link-entity
                            var link = fetchXml.Entity.FindLinkEntity(col.SourceColumn.Replace(".*", ""));
                            link.AddItem(new allattributes());
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
                            var attr = fetchXml.AddAttribute(sourceCol, null, metadata, out var added);

                            // Check if we can fold the alias down to the FetchXML too. Don't do this if 
                            if (col.OutputColumn != parts.Last())
                            {
                                if (added || (!processedSourceColumns.Contains(col.SourceColumn) && !fetchXml.IsAliasReferenced(attr.alias)))
                                {
                                    attr.alias = col.OutputColumn;
                                    col.SourceColumn = col.OutputColumn;
                                }
                                else
                                {
                                    col.SourceColumn = attr.alias ?? (sourceCol.Split('.')[0] + "." + attr.name);
                                }
                            }

                            processedSourceColumns.Add(col.SourceColumn);
                        }
                    }
                }
            }
        }

        public void ExpandWildcardColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            ExpandWildcardColumns(Source, ColumnSet, metadata, parameterTypes);
        }

        internal static void ExpandWildcardColumns(IDataExecutionPlanNode source, List<SelectColumn> columnSet, IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            // Expand any AllColumns
            if (columnSet.Any(col => col.AllColumns))
            {
                var schema = source.GetSchema(metadata, parameterTypes);
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

        IRootExecutionPlanNode IRootExecutionPlanNode.FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this.FoldQuery(metadata, options, parameterTypes);
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in ColumnSet.Select(c => c.SourceColumn + (c.AllColumns ? ".*" : "")))
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override string ToString()
        {
            return "SELECT";
        }
    }

    public class SelectColumn
    {
        public string SourceColumn { get; set; }

        public string OutputColumn { get; set; }

        [Browsable(false)]
        public bool AllColumns { get; set; }
    }
}
