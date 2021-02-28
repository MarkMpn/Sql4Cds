using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class TopNode : BaseNode
    {
        public ScalarExpression Top { get; set; }

        public bool Percent { get; set; }

        public bool WithTies { get; set; }

        public IExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (WithTies)
                throw new NotImplementedException();

            var topCount = Top.GetValue(null, null, parameterTypes, parameterValues);

            if (!Percent)
            {
                return Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                    .Take(SqlTypeConverter.ChangeType<int>(topCount));
            }
            else
            {
                var count = Source.Execute(org, metadata, options, parameterTypes, parameterValues).Count();
                var top = count * SqlTypeConverter.ChangeType<float>(topCount) / 100;

                return Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                    .Take((int)top);
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            if (!IsConstantValueExpression(Top, null, out var literal))
                return this;

            if (!Percent && !WithTies && Source is FetchXmlScan fetchXml)
            {
                fetchXml.FetchXml.top = literal.Value.ToString();
                return fetchXml;
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }
    }
}
