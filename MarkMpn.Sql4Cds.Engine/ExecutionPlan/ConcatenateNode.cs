using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Concatenates the results from multiple queries
    /// </summary>
    class ConcatenateNode : BaseDataNode
    {
        /// <summary>
        /// The data sources to concatenate
        /// </summary>
        [Browsable(false)]
        public List<IDataExecutionPlanNode> Sources { get; } = new List<IDataExecutionPlanNode>();

        /// <summary>
        /// The columns to produce in the result and the source columns from each data source
        /// </summary>
        [Category("Concatenate")]
        [Description("The columns to produce in the result and the source columns from each data source")]
        [DisplayName("Column Set")]
        public List<ConcatenateColumn> ColumnSet { get; } = new List<ConcatenateColumn>();

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var source = Sources[i];

                foreach (var entity in source.Execute(dataSources, options, parameterTypes, parameterValues))
                {
                    var result = new Entity(entity.LogicalName, entity.Id);

                    foreach (var col in ColumnSet)
                        result[col.OutputColumn] = entity[col.SourceColumns[i]];

                    yield return result;
                }
            }
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();
            var sourceSchema = Sources[0].GetSchema(dataSources, parameterTypes);

            foreach (var col in ColumnSet)
                schema.Schema[col.OutputColumn] = sourceSchema.Schema[col.SourceColumns[0]];

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Sources;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                Sources[i] = Sources[i].FoldQuery(dataSources, options, parameterTypes);
                Sources[i].Parent = this;
            }

            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var sourceRequiredColumns = ColumnSet
                    .Select(c => c.SourceColumns[i])
                    .Distinct()
                    .ToList();

                Sources[i].AddRequiredColumns(dataSources, parameterTypes, sourceRequiredColumns);
            }
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Sources.Sum(s => s.EstimateRowsOut(dataSources, options, parameterTypes));
        }
    }

    class ConcatenateColumn
    {
        /// <summary>
        /// The name of the column that is generated in the output
        /// </summary>
        [Description("The name of the column that is generated in the output")]
        public string OutputColumn { get; set; }

        /// <summary>
        /// The names of the column in each source node that generates the data for this column
        /// </summary>
        [Description("The names of the column in each source node that generates the data for this column")]
        public List<string> SourceColumns { get; } = new List<string>();
    }
}
