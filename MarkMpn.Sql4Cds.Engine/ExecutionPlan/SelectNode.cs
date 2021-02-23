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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            foreach (var entity in Source.Execute(org, metadata, options, parameterValues))
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

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return Source.GetSchema(metadata);
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return ColumnSet
                .Select(col => col.SourceColumn + (col.AllColumns ? ".*" : ""))
                .Distinct();
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source = Source.MergeNodeDown(metadata, options);

            if (Source is FetchXmlScan fetchXml)
            {
                // Check if there are any aliases we can apply to the source FetchXml
                var schema = fetchXml.GetSchema(metadata);
                var processedSourceColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var col in ColumnSet)
                {
                    var sourceCol = col.SourceColumn;
                    schema.ContainsColumn(sourceCol, out sourceCol);
                    var attr = AddAttribute(fetchXml, sourceCol, null, metadata, out var added);

                    if (col.OutputColumn != col.SourceColumn.Split('.').Last())
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

            return this;
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
