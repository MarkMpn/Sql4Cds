using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Checks that each row in the results meets an expected condition
    /// </summary>
    public class AssertNode : BaseNode
    {
        /// <summary>
        /// The data source for the assertion
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        /// <summary>
        /// The function that must be true for each entity in the <see cref="Source"/>
        /// </summary>
        public Func<Entity,bool> Assertion { get; set; }

        /// <summary>
        /// The error message that is generated if any record in the <see cref="Source"/> fails to meet the <see cref="Assertion"/>
        /// </summary>
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
