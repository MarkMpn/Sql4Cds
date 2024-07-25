using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a hash join using one column for the hash table
    /// </summary>
    class HashJoinNode : FoldableJoinNode
    {
        class OuterRecord
        {
            public Entity Entity { get; set; }

            public bool Used { get; set; }
        }

        class CompoundKey
        {
            private readonly object[] _keys;

            public CompoundKey(IEnumerable<object> keys)
            {
                _keys = keys.ToArray();
            }

            public override int GetHashCode()
            {
                var hc = _keys[0].GetHashCode();

                for (var i = 1; i < _keys.Length; i++)
                    hc ^= _keys[i].GetHashCode();

                return hc;
            }

            public override bool Equals(object obj)
            {
                if (!(obj is CompoundKey other))
                    return false;

                if (other._keys.Length != _keys.Length)
                    return false;

                for (var i = 0; i < _keys.Length; i++)
                {
                    if (!_keys[i].Equals(other._keys[i]))
                        return false;
                }

                return true;
            }
        }

        private IDictionary<CompoundKey, List<OuterRecord>> _hashTable;

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            _hashTable = new Dictionary<CompoundKey, List<OuterRecord>>();
            var mergedSchema = GetSchema(context, true);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(new ExpressionCompilationContext(context, mergedSchema, null));

            // Build the hash table
            var leftSchema = LeftSource.GetSchema(context);
            var leftCompilationContext = new ExpressionCompilationContext(context, leftSchema, null);
            var rightSchema = RightSource.GetSchema(context);
            var rightCompilationContext = new ExpressionCompilationContext(context, rightSchema, null);

            Func<ExpressionExecutionContext, object>[] leftKeyAccessors = new Func<ExpressionExecutionContext, object>[LeftAttributes.Count];
            Func<ExpressionExecutionContext, object>[] rightKeyAccessors = new Func<ExpressionExecutionContext, object>[LeftAttributes.Count];

            for (var i = 0; i < LeftAttributes.Count; i++)
            {
                LeftAttributes[i].GetType(leftCompilationContext, out var leftColType);
                RightAttributes[i].GetType(rightCompilationContext, out var rightColType);

                if (!SqlTypeConverter.CanMakeConsistentTypes(leftColType, rightColType, context.PrimaryDataSource, null, null, out var keyType))
                    throw new QueryExecutionException($"Cannot match key types {leftColType.ToSql()} and {rightColType.ToSql()}");

                Identifier keyTypeCollation = null;

                if (keyType is SqlDataTypeReferenceWithCollation keyTypeWithCollation)
                    keyTypeCollation = new Identifier { Value = keyTypeWithCollation.Collation.Name };

                var leftKeyAccessor = (ScalarExpression)LeftAttributes[i];
                if (!leftColType.IsSameAs(keyType))
                    leftKeyAccessor = new ConvertCall { Parameter = leftKeyAccessor, DataType = keyType, Collation = keyTypeCollation };
                var leftKeyConverter = leftKeyAccessor.Compile(leftCompilationContext);

                var rightKeyAccessor = (ScalarExpression)RightAttributes[i];
                if (!rightColType.IsSameAs(keyType))
                    rightKeyAccessor = new ConvertCall { Parameter = rightKeyAccessor, DataType = keyType, Collation = keyTypeCollation };
                var rightKeyConverter = rightKeyAccessor.Compile(rightCompilationContext);

                leftKeyAccessors[i] = leftKeyConverter;
                rightKeyAccessors[i] = rightKeyConverter;
            }

            var expressionContext = new ExpressionExecutionContext(context);

            foreach (var entity in LeftSource.Execute(context))
            {
                expressionContext.Entity = entity;
                var key = new CompoundKey(leftKeyAccessors.Select(accessor => accessor(expressionContext)));

                if (!_hashTable.TryGetValue(key, out var list))
                {
                    list = new List<OuterRecord>();
                    _hashTable[key] = list;
                }

                list.Add(new OuterRecord { Entity = entity });
            }

            // Probe the hash table using the right source
            foreach (var entity in RightSource.Execute(context))
            {
                expressionContext.Entity = entity;
                var key = new CompoundKey(rightKeyAccessors.Select(accessor => accessor(expressionContext)));

                var matched = false;

                if (_hashTable.TryGetValue(key, out var list))
                {
                    foreach (var left in list)
                    {
                        if (SemiJoin && left.Used)
                            continue;

                        var finalMerged = Merge(left.Entity, leftSchema, entity, rightSchema, false);
                        var merged = (OutputLeftSchema && OutputRightSchema) || additionalJoinCriteria == null ? finalMerged : Merge(left.Entity, leftSchema, entity, rightSchema, true);
                        expressionContext.Entity = merged;

                        if (additionalJoinCriteria == null || additionalJoinCriteria(expressionContext))
                        {
                            if (!AntiJoin)
                                yield return finalMerged;

                            left.Used = true;
                            matched = true;
                        }
                    }
                }

                if (!matched && (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter))
                    yield return Merge(null, leftSchema, entity, rightSchema, false);
            }

            if (JoinType == QualifiedJoinType.LeftOuter && (!SemiJoin || AntiJoin || DefinedValues.Count > 0) || JoinType == QualifiedJoinType.FullOuter)
            {
                foreach (var unmatched in _hashTable.SelectMany(kvp => kvp.Value).Where(e => !e.Used))
                    yield return Merge(unmatched.Entity, leftSchema, null, rightSchema, false);
            }
        }

        protected override IReadOnlyList<string> GetSortOrder(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            if ((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter) && innerSchema.ContainsColumn(RightAttribute.GetColumnName(), out var sortColumn))
                return innerSchema.SortOrder;

            return null;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var folded = base.FoldQuery(context, hints);

            if (folded != this)
                return folded;

            if (SemiJoin)
                return folded;

            // If we can't fold this query, try to make sure the smaller table is used as the left input to reduce the
            // number of records held in memory in the hash table
            LeftSource.EstimateRowsOut(context);
            RightSource.EstimateRowsOut(context);

            if (LeftSource.EstimatedRowsOut > RightSource.EstimatedRowsOut)
            {
                var leftSource = LeftSource;
                LeftSource = RightSource;
                RightSource = leftSource;

                var leftAttr = LeftAttribute;
                LeftAttribute = RightAttribute;
                RightAttribute = leftAttr;

                if (JoinType == QualifiedJoinType.LeftOuter)
                    JoinType = QualifiedJoinType.RightOuter;
                else if (JoinType == QualifiedJoinType.RightOuter)
                    JoinType = QualifiedJoinType.LeftOuter;
            }

            // Make sure the join keys are not null - the SqlType classes override == to prevent NULL = NULL
            // but .Equals used by the hash table allows them to match
            if (ComparisonType == BooleanComparisonType.Equals)
            {
                if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter)
                    LeftSource = AddNotNullFilter(LeftSource, LeftAttribute, context, hints, true);

                if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter)
                    RightSource = AddNotNullFilter(RightSource, RightAttribute, context, hints, true);
            }

            return this;
        }

        public override object Clone()
        {
            var clone = new HashJoinNode
            {
                AdditionalJoinCriteria = AdditionalJoinCriteria,
                JoinType = JoinType,
                LeftSource = (IDataExecutionPlanNodeInternal)LeftSource.Clone(),
                RightSource = (IDataExecutionPlanNodeInternal)RightSource.Clone(),
                SemiJoin = SemiJoin,
                OutputLeftSchema = OutputLeftSchema,
                OutputRightSchema = OutputRightSchema,
                ComparisonType = ComparisonType,
                AntiJoin = AntiJoin,
            };

            foreach (var attr in LeftAttributes)
                clone.LeftAttributes.Add(attr);

            foreach (var attr in RightAttributes)
                clone.RightAttributes.Add(attr);

            foreach (var kvp in DefinedValues)
                clone.DefinedValues.Add(kvp);

            clone.LeftSource.Parent = clone;
            clone.RightSource.Parent = clone;

            return clone;
        }
    }
}
