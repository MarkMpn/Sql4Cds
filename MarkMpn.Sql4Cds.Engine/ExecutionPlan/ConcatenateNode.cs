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

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var source = Sources[i];

                foreach (var entity in source.Execute(org, metadata, options, parameterTypes, parameterValues))
                {
                    var result = new Entity(entity.LogicalName, entity.Id);

                    foreach (var col in ColumnSet)
                        result[col.OutputColumn] = entity[col.SourceColumns[i]];

                    yield return result;
                }
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();
            var sourceSchema = Sources[0].GetSchema(metadata, parameterTypes);

            foreach (var col in ColumnSet)
                schema.Schema[col.OutputColumn] = sourceSchema.Schema[col.SourceColumns[0]];

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Sources;
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                Sources[i] = Sources[i].FoldQuery(metadata, options, parameterTypes);
                Sources[i].Parent = this;
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var sourceRequiredColumns = ColumnSet
                    .Select(c => c.SourceColumns[i])
                    .Distinct()
                    .ToList();

                Sources[i].AddRequiredColumns(metadata, parameterTypes, sourceRequiredColumns);
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Sources.Sum(s => s.EstimateRowsOut(metadata, parameterTypes, tableSize));
        }
    }

    public class ConcatenateColumn
    {
        public string OutputColumn { get; set; }
        public List<string> SourceColumns { get; } = new List<string>();
    }
}
