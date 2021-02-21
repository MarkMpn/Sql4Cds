using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Concatenates the results from multiple queries
    /// </summary>
    public class ConcatenateNode : BaseNode
    {
        /// <summary>
        /// The data sources to concatenate
        /// </summary>
        public List<IExecutionPlanNode> Sources { get; } = new List<IExecutionPlanNode>();

        /// <summary>
        /// The columns to produce in the result and the source columns from each data source
        /// </summary>
        public List<ConcatenateColumn> ColumnSet { get; } = new List<ConcatenateColumn>();

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var source = Sources[i];

                foreach (var entity in source.Execute(org, metadata, options, parameterValues))
                {
                    var result = new Entity(entity.LogicalName, entity.Id);

                    foreach (var col in ColumnSet)
                        result[col.OutputColumn] = entity[col.SourceColumns[i]];

                    yield return result;
                }
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return ColumnSet.SelectMany(col => col.SourceColumns);
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            var schema = new NodeSchema();
            var sourceSchema = Sources[0].GetSchema(metadata);

            foreach (var col in ColumnSet)
                schema.Schema[col.OutputColumn] = sourceSchema.Schema[col.SourceColumns[0]];

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Sources;
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            for (var i = 0; i < Sources.Count; i++)
                Sources[i] = Sources[i].MergeNodeDown(metadata, options);

            return this;
        }
    }

    public class ConcatenateColumn
    {
        public string OutputColumn { get; set; }
        public List<string> SourceColumns { get; } = new List<string>();
    }
}
