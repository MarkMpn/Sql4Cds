using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a TOP clause
    /// </summary>
    class TopNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The number of records to retrieve
        /// </summary>
        [Category("Top")]
        [Description("The number of records to retrieve")]
        public ScalarExpression Top { get; set; }

        /// <summary>
        /// Indicates if the Top number indicates a percentage or an absolute number of records
        /// </summary>
        [Category("Top")]
        [Description("Indicates if the Top number indicates a percentage or an absolute number of records")]
        public bool Percent { get; set; }

        /// <summary>
        /// Indicates if two records with the same sort order should be included even if the total number of records has been met
        /// </summary>
        [Category("Top")]
        [Description("Indicates if two records with the same sort order should be included even if the total number of records has been met")]
        public bool WithTies { get; set; }

        /// <summary>
        /// The columns to check for ties on
        /// </summary>
        [Category("Top")]
        [Description("When using the WITH TIES option, indicates the columns to check for ties on")]
        public List<string> TieColumns { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var topCount = Top.Compile(null, parameterTypes)(null, parameterValues, options);

            if (Percent)
            {
                int count;

                if (Source is TableSpoolNode spool && spool.SpoolType == SpoolType.Eager)
                    count = spool.GetCount(dataSources, options, parameterTypes, parameterValues);
                else
                    count = Source.Execute(dataSources, options, parameterTypes, parameterValues).Count();

                topCount = (int)(count * SqlTypeConverter.ChangeType<float>(topCount) / 100);
            }

            var top = SqlTypeConverter.ChangeType<int>(topCount);

            if (!WithTies)
            {
                return Source.Execute(dataSources, options, parameterTypes, parameterValues)
                    .Take(top);
            }

            Entity lastRow = null;
            var tieComparer = new DistinctEqualityComparer(TieColumns);

            return Source.Execute(dataSources, options, parameterTypes, parameterValues)
                .TakeWhile((entity, index) =>
                {
                    if (index == top - 1)
                        lastRow = entity;

                    if (index < top)
                        return true;

                    return tieComparer.Equals(lastRow, entity);
                });
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

            if (!Top.IsConstantValueExpression(null, options, out var literal))
                return this;

            // FetchXML can support TOP directly provided it's for no more than 5,000 records
            if (!Percent && !WithTies && Int32.TryParse(literal.Value, out var top) && top <= 5000)
            {
                var fetchXml = Source as FetchXmlScan;

                // Skip over ComputeScalar nodes to fold the TOP into the previous FetchXML node
                if (fetchXml == null && Source is ComputeScalarNode computeScalar)
                    fetchXml = computeScalar.Source as FetchXmlScan;

                if (fetchXml != null)
                {
                    fetchXml.FetchXml.top = literal.Value;
                    fetchXml.AllPages = false;

                    if (Source == fetchXml)
                        return fetchXml;

                    return Source;
                }
            }

            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        protected override int EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var sourceCount = Source.EstimatedRowsOut;

            if (!Top.IsConstantValueExpression(null, options, out var topLiteral))
                return sourceCount;

            var top = Int32.Parse(topLiteral.Value, CultureInfo.InvariantCulture);

            return Math.Max(0, Math.Min(top, sourceCount));
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Top.GetVariables();
        }

        public override object Clone()
        {
            var clone = new TopNode
            {
                Percent = Percent,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                TieColumns = TieColumns,
                Top = Top,
                WithTies = WithTies
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
