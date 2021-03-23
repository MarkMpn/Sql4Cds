using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class TopNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        public ScalarExpression Top { get; set; }

        public bool Percent { get; set; }

        public bool WithTies { get; set; }

        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (WithTies)
                throw new NotImplementedException();

            var topCount = Top.Compile(null, parameterTypes)(null, parameterValues);

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

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            if (!Top.IsConstantValueExpression(null, out var literal))
                return this;

            if (!Percent && !WithTies && Int32.TryParse(literal.Value, out var top) && top <= 5000 && Source is FetchXmlScan fetchXml)
            {
                fetchXml.FetchXml.top = literal.Value;
                fetchXml.AllPages = false;
                return fetchXml;
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            var sourceCount = Source.EstimateRowsOut(metadata, parameterTypes, tableSize);

            if (!Top.IsConstantValueExpression(null, out var topLiteral))
                return sourceCount;

            var top = Int32.Parse(topLiteral.Value);

            return Math.Max(0, Math.Min(top, sourceCount));
        }
    }
}
