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

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _hashTable = new Dictionary<object, List<OuterRecord>>();
            var mergedSchema = GetSchema(dataSources, parameterTypes, true);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(dataSources[options.PrimaryDataSource], mergedSchema, parameterTypes);

            // Build the hash table
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftCol);
            var leftColType = leftSchema.Schema[leftCol];
            var rightSchema = RightSource.GetSchema(dataSources, parameterTypes);
            rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightCol);
            var rightColType = rightSchema.Schema[rightCol];

            if (!SqlTypeConverter.CanMakeConsistentTypes(leftColType, rightColType, dataSources[options.PrimaryDataSource], out var keyType))
                throw new QueryExecutionException($"Cannot match key types {leftColType.ToSql()} and {rightColType.ToSql()}");

            var leftKeyAccessor = (ScalarExpression) leftCol.ToColumnReference();
            if (!leftColType.IsSameAs(keyType))
                leftKeyAccessor = new ConvertCall { Parameter = leftKeyAccessor, DataType = keyType };
            var leftKeyConverter = leftKeyAccessor.Compile(dataSources[options.PrimaryDataSource], leftSchema, parameterTypes);

            var rightKeyAccessor = (ScalarExpression)rightCol.ToColumnReference();
            if (!rightColType.IsSameAs(keyType))
                rightKeyAccessor = new ConvertCall { Parameter = rightKeyAccessor, DataType = keyType };
            var rightKeyConverter = rightKeyAccessor.Compile(dataSources[options.PrimaryDataSource], rightSchema, parameterTypes);

            foreach (var entity in LeftSource.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                var key = leftKeyConverter(entity, parameterValues, options);

                if (!_hashTable.TryGetValue(key, out var list))
                {
                    list = new List<OuterRecord>();
                    _hashTable[key] = list;
                }

                list.Add(new OuterRecord { Entity = entity });
            }

            // Probe the hash table using the right source
            foreach (var entity in RightSource.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                var key = rightKeyConverter(entity, parameterValues, options);
                var matched = false;

                if (_hashTable.TryGetValue(key, out var list))
                {
                    foreach (var left in list)
                    {
                        if (SemiJoin && left.Used)
                            continue;

                        var merged = Merge(left.Entity, leftSchema, entity, rightSchema);

                        if (additionalJoinCriteria == null || additionalJoinCriteria(merged, parameterValues, options))
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

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            var folded = base.FoldQuery(dataSources, options, parameterTypes, hints);

            if (folded != this)
                return folded;

            if (SemiJoin)
                return folded;

            // If we can't fold this query, try to make sure the smaller table is used as the left input to reduce the
            // number of records held in memory in the hash table
            LeftSource.EstimateRowsOut(dataSources, options, parameterTypes);
            RightSource.EstimateRowsOut(dataSources, options, parameterTypes);

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
