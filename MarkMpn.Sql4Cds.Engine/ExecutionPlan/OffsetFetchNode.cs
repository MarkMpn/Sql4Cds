using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
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
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var offset = SqlTypeConverter.ChangeType<int>(Offset.Compile(dataSources[options.PrimaryDataSource], null, parameterTypes)(null, parameterValues, options));
            var fetch = SqlTypeConverter.ChangeType<int>(Fetch.Compile(dataSources[options.PrimaryDataSource], null, parameterTypes)(null, parameterValues, options));

            if (offset < 0)
                throw new QueryExecutionException("The offset specified in a OFFSET clause may not be negative.");

            if (fetch <= 0)
                throw new QueryExecutionException("The number of rows provided for a FETCH clause must be greater then zero.");

            return Source.Execute(dataSources, options, parameterTypes, parameterValues)
                .Skip(offset)
                .Take(fetch);
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return Source.GetSchema(dataSources, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;

            if (!Offset.IsConstantValueExpression(dataSources[options.PrimaryDataSource], null, options, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(dataSources[options.PrimaryDataSource], null, options, out var fetchLiteral))
                return this;

            if (Source is FetchXmlScan fetchXml)
            {
                var offset = SqlTypeConverter.ChangeType<int>(offsetLiteral.Compile(dataSources[options.PrimaryDataSource], null, null)(null, null, options));
                var count = SqlTypeConverter.ChangeType<int>(fetchLiteral.Compile(dataSources[options.PrimaryDataSource], null, null)(null, null, options));
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

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var sourceCount = Source.EstimateRowsOut(dataSources, options, parameterTypes);

            if (!Offset.IsConstantValueExpression(dataSources[options.PrimaryDataSource], null, options, out var offsetLiteral) ||
                !Fetch.IsConstantValueExpression(dataSources[options.PrimaryDataSource], null, options, out var fetchLiteral))
                return sourceCount;

            var offset = Int32.Parse(offsetLiteral.Value, CultureInfo.InvariantCulture);
            var fetch = Int32.Parse(fetchLiteral.Value, CultureInfo.InvariantCulture);

            return new RowCountEstimateDefiniteRange(0, Math.Max(0, Math.Min(fetch, sourceCount.Value - offset)));
        }

        public override string ToString()
        {
            return "Offset";
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Offset.GetVariables().Union(Fetch.GetVariables());
        }

        public override object Clone()
        {
            var clone = new OffsetFetchNode
            {
                Fetch = Fetch,
                Offset = Offset,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
