using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public abstract class BaseNode : IExecutionPlanNode
    {
        public string Sql { get; set; }
        public int Index { get; set; }
        public int Length { get; set; }

        public bool IgnoreForFetchXmlFolding { get; set; }

        public abstract IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options);

        public abstract IEnumerable<IExecutionPlanNode> GetSources();

        public abstract NodeSchema GetSchema(IAttributeMetadataCache metadata);

        public abstract IEnumerable<string> GetRequiredColumns();
    }
}
