using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class AssertNode : BaseNode
    {
        public IExecutionPlanNode Source { get; set; }

        public Func<Entity,bool> Assertion { get; set; }

        public string ErrorMessage { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string,object> parameterValues)
        {
            foreach (var entity in Source.Execute(org, metadata, options, parameterValues))
            {
                if (!Assertion(entity))
                    throw new ApplicationException(ErrorMessage);

                yield return entity;
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return Array.Empty<string>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return Source.GetSchema(metadata);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source = Source.MergeNodeDown(metadata, options);
            return this;
        }
    }
}
