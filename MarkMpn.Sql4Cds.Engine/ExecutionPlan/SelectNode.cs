using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            foreach (var entity in Source.Execute(org, metadata, options))
                yield return entity;
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
    }

    public class SelectColumn
    {
        public string SourceColumn { get; set; }
        public string OutputColumn { get; set; }
        public bool AllColumns { get; set; }
    }
}
