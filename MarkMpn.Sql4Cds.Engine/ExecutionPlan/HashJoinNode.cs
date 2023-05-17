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

        private IDictionary<object, List<OuterRecord>> _hashTable;

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            _hashTable = new Dictionary<object, List<OuterRecord>>();
            var mergedSchema = GetSchema(context, true);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(new ExpressionCompilationContext(context, mergedSchema, null));

            // Build the hash table
            var leftSchema = LeftSource.GetSchema(context);
            var leftCompilationContext = new ExpressionCompilationContext(context, leftSchema, null);
            LeftAttribute.GetType(leftCompilationContext, out var leftColType);
            var rightSchema = RightSource.GetSchema(context);
            var rightCompilationContext = new ExpressionCompilationContext(context, rightSchema, null);
            RightAttribute.GetType(rightCompilationContext, out var rightColType);

            if (!SqlTypeConverter.CanMakeConsistentTypes(leftColType, rightColType, context.PrimaryDataSource, out var keyType))
                throw new QueryExecutionException($"Cannot match key types {leftColType.ToSql()} and {rightColType.ToSql()}");

            Identifier keyTypeCollation = null;

            if (keyType is SqlDataTypeReferenceWithCollation keyTypeWithCollation) 
                keyTypeCollation = new Identifier { Value = keyTypeWithCollation.Collation.Name };

            var leftKeyAccessor = (ScalarExpression)LeftAttribute;
            if (!leftColType.IsSameAs(keyType))
                leftKeyAccessor = new ConvertCall { Parameter = leftKeyAccessor, DataType = keyType, Collation = keyTypeCollation };
            var leftKeyConverter = leftKeyAccessor.Compile(leftCompilationContext);

            var rightKeyAccessor = (ScalarExpression)RightAttribute;
            if (!rightColType.IsSameAs(keyType))
                rightKeyAccessor = new ConvertCall { Parameter = rightKeyAccessor, DataType = keyType, Collation = keyTypeCollation };
            var rightKeyConverter = rightKeyAccessor.Compile(rightCompilationContext);

            var expressionContext = new ExpressionExecutionContext(context);

            foreach (var entity in LeftSource.Execute(context))
            {
                expressionContext.Entity = entity;

                var key = leftKeyConverter(expressionContext);

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

                var key = rightKeyConverter(expressionContext);
                var matched = false;

                if (_hashTable.TryGetValue(key, out var list))
                {
                    foreach (var left in list)
                    {
                        if (SemiJoin && left.Used)
                            continue;

                        var merged = Merge(left.Entity, leftSchema, entity, rightSchema);
                        expressionContext.Entity = merged;

                        if (additionalJoinCriteria == null || additionalJoinCriteria(expressionContext))
                        {
                            yield return merged;
                            left.Used = true;
                            matched = true;
                        }
                    }
                }

                if (!matched && (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter))
                    yield return Merge(null, leftSchema, entity, rightSchema);
            }

            if (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter)
            {
                foreach (var unmatched in _hashTable.SelectMany(kvp => kvp.Value).Where(e => !e.Used))
                    yield return Merge(unmatched.Entity, leftSchema, null, rightSchema);
            }
        }

        protected override IReadOnlyList<string> GetSortOrder(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            if ((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter) && innerSchema.ContainsColumn(RightAttribute.GetColumnName(), out var sortColumn))
                return new[] { sortColumn };

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

            return this;
        }

        public override object Clone()
        {
            var clone = new HashJoinNode
            {
                AdditionalJoinCriteria = AdditionalJoinCriteria,
                JoinType = JoinType,
                LeftAttribute = LeftAttribute,
                LeftSource = (IDataExecutionPlanNodeInternal)LeftSource.Clone(),
                RightAttribute = RightAttribute,
                RightSource = (IDataExecutionPlanNodeInternal)RightSource.Clone(),
                SemiJoin = SemiJoin
            };

            foreach (var kvp in DefinedValues)
                clone.DefinedValues.Add(kvp);

            clone.LeftSource.Parent = clone;
            clone.RightSource.Parent = clone;

            return clone;
        }
    }
}
