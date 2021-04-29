using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an OFFSET/FETCH clause
    /// </summary>
    class OffsetFetchNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The number of records to skip
        /// </summary>
        [Category("Offset")]
        [Description("The number of records to skip")]
        public ScalarExpression Offset { get; set; }

        /// <summary>
        /// The number of records to retrieve
        /// </summary>
        [Category("Offset")]
        [Description("The number of records to retrieve")]
        public ScalarExpression Fetch { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var offset = SqlTypeConverter.ChangeType<int>(Offset.Compile(null, parameterTypes)(null, parameterValues));
            var fetch = SqlTypeConverter.ChangeType<int>(Fetch.Compile(null, parameterTypes)(null, parameterValues));

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

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            if (!Offset.IsConstantValueExpression(null, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(null, out var fetchLiteral))
                return this;

            if (Source is FetchXmlScan fetchXml)
            {
                var offset = SqlTypeConverter.ChangeType<int>(offsetLiteral.Compile(null, null)(null, null));
                var count = SqlTypeConverter.ChangeType<int>(fetchLiteral.Compile(null, null)(null, null));
                var page = offset / count;

                if (page * count == offset && count <= 5000)
                {
                    fetchXml.FetchXml.count = count.ToString();
                    fetchXml.FetchXml.page = (page + 1).ToString();
                    fetchXml.AllPages = false;
                    return fetchXml;
                }
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

            if (!Offset.IsConstantValueExpression(null, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(null, out var fetchLiteral))
                return sourceCount;

            var offset = Int32.Parse(offsetLiteral.Value);
            var fetch = Int32.Parse(fetchLiteral.Value);

            return Math.Max(0, Math.Min(fetch, sourceCount - offset));
        }

        public override string ToString()
        {
            return "Offset";
        }
    }
}
