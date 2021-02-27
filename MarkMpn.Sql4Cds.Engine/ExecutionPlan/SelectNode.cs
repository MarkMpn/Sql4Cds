using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class SelectNode : BaseNode
    {
        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        /// <summary>
        /// The data source to select from
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                foreach (var col in ColumnSet)
                    entity[col.OutputColumn] = entity[col.SourceColumn];

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

            if (Source is FetchXmlScan fetchXml)
            {
                // Check if there are any aliases we can apply to the source FetchXml
                var schema = fetchXml.GetSchema(metadata, parameterTypes);
                var processedSourceColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var hasStar = ColumnSet.Any(col => col.AllColumns && col.SourceColumn == null);
                var aliasStars = new HashSet<string>(ColumnSet.Where(col => col.AllColumns && col.SourceColumn != null).Select(col => col.SourceColumn.Replace(".*", "")).Distinct(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

                foreach (var col in ColumnSet)
                {
                    if (col.AllColumns)
                    {
                        if (col.SourceColumn == null)
                        {
                            // Add an allattributes to the main entity and all link-entities
                            fetchXml.Entity.AddItem(new allattributes());

                            foreach (var link in GetLinkEntities(fetchXml.Entity.Items))
                                link.AddItem(new allattributes());
                        }
                        else if (!hasStar)
                        {
                            // Only add an allattributes to the appropriate entity/link-entity
                            var link = fetchXml.Entity.FindLinkEntity(col.SourceColumn.Replace(".*", ""));
                            link.AddItem(new allattributes());
                        }
                    }
                    else if (!hasStar)
                    {
                        // Only fold individual columns down to the FetchXML if there is no corresponding allatributes
                        var parts = col.SourceColumn.Split('.');

                        if (parts.Length == 1 || !aliasStars.Contains(parts[0]))
                        {
                            var sourceCol = col.SourceColumn;
                            schema.ContainsColumn(sourceCol, out sourceCol);
                            var attr = AddAttribute(fetchXml, sourceCol, null, metadata, out var added);

                            // Check if we can fold the alias down to the FetchXML too. Don't do this if 
                            if (col.OutputColumn != parts.Last())
                            {
                                if (added || (!processedSourceColumns.Contains(col.SourceColumn) && !IsAliasReferenced(fetchXml, attr.alias)))
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

            // Expand any AllColumns
            if (ColumnSet.Any(col => col.AllColumns))
            {
                var schema = Source.GetSchema(metadata, parameterTypes);
                var expanded = new List<SelectColumn>();

                foreach (var col in ColumnSet)
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

                ColumnSet.Clear();
                ColumnSet.AddRange(expanded);
            }

            return this;
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

        private IEnumerable<FetchLinkEntityType> GetLinkEntities(object[] items)
        {
            if (items == null)
                yield break;

            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                yield return link;

                foreach (var subLink in GetLinkEntities(link.Items))
                    yield return subLink;
            }
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
        public bool AllColumns { get; set; }
    }
}
