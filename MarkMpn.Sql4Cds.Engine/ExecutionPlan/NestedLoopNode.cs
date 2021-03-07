using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a nested loop join
    /// </summary>
    public class NestedLoopNode : BaseJoinNode
    {
        /// <summary>
        /// The condition that must be true for  two records to join
        /// </summary>
        public BooleanExpression JoinCondition { get; set; }

        /// <summary>
        /// Values from the outer query that should be passed as references to the inner query
        /// </summary>
        public Dictionary<string,string> OuterReferences { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            if (OuterReferences != null)
            {
                if (parameterTypes == null)
                    innerParameterTypes = new Dictionary<string, Type>();
                else
                    innerParameterTypes = new Dictionary<string, Type>(parameterTypes);

                foreach (var kvp in OuterReferences)
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key];
            }

            var rightSchema = RightSource.GetSchema(metadata, innerParameterTypes);
            var mergedSchema = GetSchema(metadata, parameterTypes);

            foreach (var left in LeftSource.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                var innerParameters = parameterValues;

                if (OuterReferences != null)
                {
                    if (parameterValues == null)
                        innerParameters = new Dictionary<string, object>();
                    else
                        innerParameters = new Dictionary<string, object>(parameterValues);

                    foreach (var kvp in OuterReferences)
                    {
                        left.Attributes.TryGetValue(kvp.Key, out var outerValue);
                        innerParameters[kvp.Value] = outerValue;
                    }
                }

                var hasRight = false;

                foreach (var right in RightSource.Execute(org, metadata, options, innerParameterTypes, innerParameters))
                {
                    var merged = Merge(left, leftSchema, right, rightSchema);

                    if (JoinCondition == null || JoinCondition.GetValue(merged, mergedSchema, parameterTypes, parameterValues))
                        yield return merged;

                    hasRight = true;

                    if (SemiJoin)
                        break;
                }

                if (!hasRight && JoinType == QualifiedJoinType.LeftOuter)
                    yield return Merge(left, leftSchema, null, rightSchema);
            }
        }

        private IDictionary<string, Type> GetInnerParameterTypes(NodeSchema leftSchema, IDictionary<string, Type> parameterTypes)
        {
            var innerParameterTypes = parameterTypes;

            if (OuterReferences != null)
            {
                if (parameterTypes == null)
                    innerParameterTypes = new Dictionary<string, Type>();
                else
                    innerParameterTypes = new Dictionary<string, Type>(parameterTypes);

                foreach (var kvp in OuterReferences)
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key];
            }

            return innerParameterTypes;
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            LeftSource = LeftSource.FoldQuery(metadata, options, parameterTypes);
            LeftSource.Parent = this;

            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            RightSource = RightSource.FoldQuery(metadata, options, innerParameterTypes);
            RightSource.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (JoinCondition != null)
            {
                foreach (var col in JoinCondition.GetColumns())
                {
                    if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                        requiredColumns.Add(col);
                }
            }

            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var leftColumns = requiredColumns
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .Concat((IEnumerable<string>) OuterReferences?.Keys ?? Array.Empty<string>())
                .Distinct()
                .ToList();
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            var rightSchema = RightSource.GetSchema(metadata, innerParameterTypes);
            var rightColumns = requiredColumns
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .Concat(DefinedValues.Values)
                .Distinct()
                .ToList();

            LeftSource.AddRequiredColumns(metadata, parameterTypes, leftColumns);
            RightSource.AddRequiredColumns(metadata, parameterTypes, rightColumns);
        }

        protected override NodeSchema GetRightSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            return RightSource.GetSchema(metadata, innerParameterTypes);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(metadata, parameterTypes, tableSize);

            // We tend to use a nested loop with an assert node for scalar subqueries - we'll return one record for each record in the outer loop
            if (RightSource is AssertNode)
                return leftEstimate;

            var rightEstimate = RightSource.EstimateRowsOut(metadata, parameterTypes, tableSize);

            if (JoinType == QualifiedJoinType.Inner)
                return Math.Min(leftEstimate, rightEstimate);
            else
                return Math.Max(leftEstimate, rightEstimate);
        }
    }
}
