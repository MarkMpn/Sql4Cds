using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class OffsetFetchNode : BaseNode
    {
        public ScalarExpression Offset { get; set; }

        public ScalarExpression Fetch { get; set; }

        public IExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var offset = SqlTypeConverter.ChangeType<int>(Offset.GetValue(null, null, parameterTypes, parameterValues));
            var fetch = SqlTypeConverter.ChangeType<int>(Fetch.GetValue(null, null, parameterTypes, parameterValues));

            if (offset < 0)
                throw new QueryExecutionException("The offset specified in a OFFSET clause may not be negative.");

            if (fetch <= 0)
                throw new QueryExecutionException("The number of rows provided for a FETCH clause must be greater then zero.");


            return Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                .Skip(offset)
                .Take(fetch);
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

            if (!IsConstantValueExpression(Offset, null, out var offsetLiteral) ||
                !IsConstantValueExpression(Fetch, null, out var fetchLiteral))
                return this;

            if (Source is FetchXmlScan fetchXml)
            {
                var offset = SqlTypeConverter.ChangeType<int>(offsetLiteral.GetValue(null, null, null, null));
                var count = SqlTypeConverter.ChangeType<int>(fetchLiteral.GetValue(null, null, null, null));
                var page = offset / count;

                if (page * count == offset)
                {
                    fetchXml.FetchXml.count = count.ToString();
                    fetchXml.FetchXml.page = (page + 1).ToString();
                    return fetchXml;
                }
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }
    }
}
