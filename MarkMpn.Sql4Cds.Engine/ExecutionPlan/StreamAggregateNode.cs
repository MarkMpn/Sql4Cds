using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values where the input data is already sorted by the grouping keys
    /// </summary>
    class StreamAggregateNode : BaseAggregateNode
    {
        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = base.GetSchema(context);
            var groupByCols = GetGroupingColumns(schema);

            return new NodeSchema(
                primaryKey: schema.PrimaryKey,
                schema: schema.Schema,
                aliases: schema.Aliases,
                sortOrder: groupByCols);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var groupByCols = GetGroupingColumns(schema);
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);

            var isScalarAggregate = IsScalarAggregate;

            InitializeAggregates(expressionCompilationContext);
            Entity currentGroup = null;
            var comparer = new DistinctEqualityComparer(groupByCols);
            var aggregates = CreateAggregateFunctions(expressionExecutionContext, false);
            var states = isScalarAggregate ? ResetAggregates(aggregates) : null;

            foreach (var entity in Source.Execute(context))
            {
                if (!isScalarAggregate || currentGroup != null)
                {
                    var startNewGroup = currentGroup == null;

                    if (currentGroup != null && !comparer.Equals(currentGroup, entity))
                    {
                        // We've reached the end of the previous group - return that row now
                        var result = new Entity();

                        for (var i = 0; i < groupByCols.Count; i++)
                            result[groupByCols[i]] = currentGroup[groupByCols[i]];

                        foreach (var aggregate in GetValues(states))
                            result[aggregate.Key] = aggregate.Value;

                        yield return result;

                        startNewGroup = true;
                    }

                    if (startNewGroup)
                    {
                        currentGroup = entity;
                        states = ResetAggregates(aggregates);
                    }
                }

                expressionExecutionContext.Entity = entity;

                foreach (var func in states.Values)
                    func.AggregateFunction.NextRecord(func.State);
            }

            if (states != null)
            {
                // For scalar aggregates, or for non-scalar aggregates where we've found at least one group, we need to
                // return the values for the final group
                var result = new Entity();

                for (var i = 0; i < groupByCols.Count; i++)
                    result[groupByCols[i]] = currentGroup[groupByCols[i]];

                foreach (var aggregate in GetValues(states))
                    result[aggregate.Key] = aggregate.Value;

                yield return result;
            }
        }

        public override object Clone()
        {
            var clone = new StreamAggregateNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            foreach (var kvp in Aggregates)
                clone.Aggregates.Add(kvp.Key, kvp.Value);

            clone.GroupBy.AddRange(GroupBy);
            clone.Source.Parent = clone;

            foreach (var sort in WithinGroupSorts)
                clone.WithinGroupSorts.Add(sort.Clone());

            return clone;
        }
    }
}
