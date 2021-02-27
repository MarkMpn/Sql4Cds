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
    /// Implements a hash join using one column for the hash table
    /// </summary>
    public class HashJoinNode : FoldableJoinNode
    {
        class OuterRecord
        {
            public Entity Entity { get; set; }

            public bool Used { get; set; }
        }

        private IDictionary<object, List<OuterRecord>> _hashTable;

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _hashTable = new Dictionary<object, List<OuterRecord>>(CaseInsensitiveObjectComparer.Instance);
            var mergedSchema = GetSchema(metadata, parameterTypes);

            // Build the hash table
            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftCol);

            foreach (var entity in LeftSource.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                var key = entity[leftCol];

                if (!_hashTable.TryGetValue(key, out var list))
                {
                    list = new List<OuterRecord>();
                    _hashTable[key] = list;
                }

                list.Add(new OuterRecord { Entity = entity });
            }

            // Probe the hash table using the right source
            var rightSchema = RightSource.GetSchema(metadata, parameterTypes);
            rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightCol);

            foreach (var entity in RightSource.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                var key = entity[rightCol];
                var matched = false;

                if (_hashTable.TryGetValue(key, out var list))
                {
                    foreach (var left in list)
                    {
                        var merged = Merge(left.Entity, leftSchema, entity, rightSchema);

                        if (AdditionalJoinCriteria == null || AdditionalJoinCriteria.GetValue(merged, mergedSchema, parameterTypes, parameterValues))
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
    }
}
