using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
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
        private bool _fullWindowSchema = true;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (Source is WindowSpoolNode)
            {
                // If this is part of a window frame plan, add the FIRST functions to get the required scalar values
                var schema = Source.GetSchema(context);

                foreach (var col in requiredColumns)
                {
                    if (!schema.ContainsColumn(col, out var normalized))
                        continue;

                    Aggregates.Add(normalized, new Aggregate
                    {
                        AggregateType = AggregateType.First,
                        SqlExpression = normalized.ToColumnReference()
                    });
                }
            }

            base.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;
            return this;
        }

        public override void FinishedFolding(NodeCompilationContext context)
        {
            _fullWindowSchema = false;

            base.FinishedFolding(context);
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = base.GetSchema(context);

            if (Source is WindowSpoolNode)
            {
                // If this is part of a window frame plan, all the columns from the source are potentially available
                // while still building the plan. Only at the end do we limit the available columns and create the implicit
                // FIRST functions. The original primary key and sort order still applies
                var sourceSchema = Source.GetSchema(context);
                var cols = schema.Schema;

                if (_fullWindowSchema)
                {
                    var fullCols = new ColumnList();

                    foreach (var col in sourceSchema.Schema)
                        fullCols.Add(col);

                    foreach (var col in schema.Schema)
                    {
                        if (!fullCols.ContainsKey(col.Key))
                            fullCols.Add(col);
                    }

                    cols = fullCols;
                }

                return new NodeSchema(
                    primaryKey: sourceSchema.PrimaryKey,
                    schema: cols,
                    aliases: schema.Aliases,
                    sortOrder: sourceSchema.SortOrder);
            }
            else
            {
                var groupByCols = GetGroupingColumns(schema);

                return new NodeSchema(
                    primaryKey: schema.PrimaryKey,
                    schema: schema.Schema,
                    aliases: schema.Aliases,
                    sortOrder: groupByCols);
            }
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var groupByCols = GetGroupingColumns(schema);
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);

            var isScalarAggregate = IsScalarAggregate;
            var isWindowAggregate = false;
            var isFastTrackWindowAggregate = false;
            var fastTrackResetCol = string.Empty;

            if (Source is WindowSpoolNode windowSpool)
            {
                isWindowAggregate = true;
                isFastTrackWindowAggregate = windowSpool.UseFastTrackOptimization;
                fastTrackResetCol = windowSpool.SegmentColumn;
            }

            InitializeAggregates(expressionCompilationContext);
            Entity currentGroup = null;
            var comparer = new DistinctEqualityComparer(groupByCols);
            var aggregates = CreateAggregateFunctions(expressionExecutionContext, false);
            var states = isScalarAggregate ? ResetAggregates(aggregates) : null;

            foreach (var entity in Source.Execute(context))
            {
                var startNewGroup = currentGroup == null;

                if (!isScalarAggregate || currentGroup != null)
                {
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
                        var resetFastTrack = isFastTrackWindowAggregate && (currentGroup == null || entity.GetAttributeValue<SqlBoolean>(fastTrackResetCol).Value);
                        currentGroup = entity;

                        if (!isFastTrackWindowAggregate || resetFastTrack)
                        {
                            // Most aggregates - reset the state on each new group
                            states = ResetAggregates(aggregates);
                        }
                        else if (isFastTrackWindowAggregate)
                        {
                            // Window aggregates using fast track optimization - only reset the FIRST functions
                            // and keep other aggregates going until we hit the next partition
                            foreach (var func in states.Values)
                            {
                                if (func.AggregateFunction is First)
                                    func.State = func.AggregateFunction.Reset();
                            }
                        }
                    }
                }

                expressionExecutionContext.Entity = entity;

                // If this is part of a window frame plan, do not aggregate the first row in each group except
                // for the FIRST functions we created implicitly.
                foreach (var func in states.Values)
                {
                    if (!isWindowAggregate || !startNewGroup || func.AggregateFunction is First)
                        func.AggregateFunction.NextRecord(func.State, expressionExecutionContext);
                }
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
