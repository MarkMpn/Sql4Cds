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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string,object> parameterValues)
        {
            var leftSchema = LeftSource.GetSchema(metadata);
            var rightSchema = RightSource.GetSchema(metadata);
            var mergedSchema = GetSchema(metadata);

            foreach (var left in LeftSource.Execute(org, metadata, options, parameterValues))
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

                foreach (var right in RightSource.Execute(org, metadata, options, innerParameters))
                {
                    var merged = Merge(left, leftSchema, right, rightSchema);

                    if (JoinCondition == null || JoinCondition.GetValue(merged, mergedSchema))
                        yield return merged;

                    hasRight = true;
                }

                if (!hasRight && JoinType == QualifiedJoinType.LeftOuter)
                    yield return left;
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            if (JoinCondition != null)
            {
                foreach (var col in JoinCondition.GetColumns())
                    yield return col;
            }

            if (OuterReferences != null)
            {
                foreach (var col in OuterReferences.Keys)
                    yield return col;
            }
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            LeftSource = LeftSource.MergeNodeDown(metadata, options);
            RightSource = RightSource.MergeNodeDown(metadata, options);
            return this;
        }
    }
}
