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

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            int topCount;

            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);
            Top.GetType(expressionCompilationContext, out var topType);

            if (Percent)
            {
                var top = new ConvertCall { Parameter = Top, DataType = DataTypeHelpers.Float };
                var topPercent = (SqlDouble)top.Compile(expressionCompilationContext)(expressionExecutionContext);

                if (topPercent.IsNull)
                {
                    topCount = 0;
                }
                else
                {
                    int count;

                    if (Source is TableSpoolNode spool && spool.SpoolType == SpoolType.Eager)
                        count = spool.GetCount(context);
                    else
                        count = Source.Execute(context).Count();

                    topCount = (int)Math.Ceiling(count * topPercent.Value / 100);
                }
            }
            else
            {
                var top = new ConvertCall { Parameter = Top, DataType = DataTypeHelpers.BigInt };
                var topValue = (SqlInt64)top.Compile(expressionCompilationContext)(expressionExecutionContext);

                if (topValue.IsNull)
                {
                    topCount = 0;
                }
                else
                {
                    topCount = (int)topValue.Value;
                }
            }

            if (!WithTies)
            {
                return Source.Execute(context)
                    .Take(topCount);
            }

            Entity lastRow = null;
            var tieComparer = new DistinctEqualityComparer(TieColumns);

            return Source.Execute(context)
                .TakeWhile((entity, index) =>
                {
                    if (index == topCount - 1)
                        lastRow = entity;

                    if (index < topCount)
                        return true;

                    return tieComparer.Equals(lastRow, entity);
                });
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return Source.GetSchema(context);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);

            if (!Top.IsConstantValueExpression(expressionCompilationContext, out var literal))
                return this;

            // FetchXML can support TOP directly provided it's for no more than 5,000 records
            if (!Percent && !WithTies && Int32.TryParse(literal.Value, out var top) && top <= 5000)
            {
                FetchXmlScan fetchXml = null;

                // Skip over ComputeScalar or semi join nodes to fold the TOP into the previous FetchXML node
                var source = Source;
                while (fetchXml == null)
                {
                    if (source is ComputeScalarNode computeScalar)
                        source = computeScalar.Source;
                    else if (source is BaseJoinNode join && join.SemiJoin)
                        source = join.LeftSource;
                    else if (source is FetchXmlScan fetch)
                        fetchXml = fetch;
                    else
                        break;
                }

                if (fetchXml != null && fetchXml.FetchXml.count == null)
                {
                    // audit provider doesn't work well with TOP if it's got an inner join on callinguserid,
                    // so reduce the page size instead. This seems to be because the TOP gets applied to the list
                    // of audit records before the join is applied, so any records with no callinguserid are
                    // lost.
                    if (fetchXml.Entity.name == "audit" &&
                        fetchXml.Entity.GetLinkEntities().SingleOrDefault()?.to == "callinguserid" &&
                        fetchXml.Entity.GetLinkEntities().SingleOrDefault()?.linktype == "inner")
                    {
                        fetchXml.FetchXml.count = literal.Value;
                    }
                    else
                    {
                        fetchXml.FetchXml.top = literal.Value;
                        fetchXml.AllPages = false;

                        // Virtual entity providers aren't reliable - fold the TOP into the FetchXML but keep
                        // this node in case the provider doesn't support TOP
                        if (fetchXml.IsUnreliableVirtualEntityProvider)
                            return this;

                        if (Source == fetchXml)
                            return fetchXml;

                        return Source.FoldQuery(context, hints);
                    }
                }

                SetPageSize(context, hints, top);
            }

            return this;
        }

        private void SetPageSize(NodeCompilationContext context, IList<OptimizerHint> hints, int top)
        {
            // Set the page size equal to the top count for the child side of a join - no point retrieving 1000 child records
            // if we're only going to return 10 of them. Don't apply it as a definite TOP though as the audit provider does
            // not always return the requested number of records.
            if (Source is HashJoinNode join)
            {
                var leftSchema = join.LeftSource.GetSchema(context);
                var rightSchema = join.RightSource.GetSchema(context);

                if ((join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.RightOuter) &&
                    join.LeftAttribute.ToSql() == leftSchema.PrimaryKey &&
                    join.RightSource is FetchXmlScan rightFetch &&
                    rightFetch.FetchXml.count == null)
                {
                    rightFetch.FetchXml.count = top.ToString();
                }
                else if ((join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.LeftOuter) &&
                    join.RightAttribute.ToSql() == rightSchema.PrimaryKey &&
                    join.LeftSource is FetchXmlScan leftFetch &&
                    leftFetch.FetchXml.count == null)
                {
                    leftFetch.FetchXml.count = top.ToString();
                }
            }
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var sourceCount = Source.EstimateRowsOut(context);

            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);

            if (!Top.IsConstantValueExpression(expressionCompilationContext, out var topLiteral))
                return sourceCount;

            var top = Decimal.Parse(topLiteral.Value, CultureInfo.InvariantCulture);

            if (Percent)
                return new RowCountEstimate((int)Math.Max(0, Math.Min(Math.Ceiling(sourceCount.Value * top / 100), sourceCount.Value)));
            else if (WithTies)
                return new RowCountEstimate(Math.Max(0, Math.Min((int)top, sourceCount.Value)));
            else
                return new RowCountEstimateDefiniteRange(0, Math.Max(0, Math.Min((int)top, sourceCount.Value)));
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
