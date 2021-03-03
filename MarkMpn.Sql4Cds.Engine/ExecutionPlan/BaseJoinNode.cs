using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public abstract class BaseJoinNode : BaseNode
    {
        /// <summary>
        /// The first data source to merge
        /// </summary>
        public IExecutionPlanNode LeftSource { get; set; }

        /// <summary>
        /// The second data source to merge
        /// </summary>
        public IExecutionPlanNode RightSource { get; set; }

        /// <summary>
        /// The type of join to apply
        /// </summary>
        public QualifiedJoinType JoinType { get; set; }

        /// <summary>
        /// Indicates if a semi join should be used (single output row for each row from the left source, ignoring duplicate matches from the right source)
        /// </summary>
        public bool SemiJoin { get; set; }

        /// <summary>
        /// For semi joins, lists individual columns that should be created in the output and their corresponding source from the right input
        /// </summary>
        public IDictionary<string, string> DefinedValues { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        protected Entity Merge(Entity leftEntity, NodeSchema leftSchema, Entity rightEntity, NodeSchema rightSchema)
        {
            var merged = new Entity();

            if (leftEntity != null)
            {
                foreach (var attr in leftEntity.Attributes)
                    merged[attr.Key] = attr.Value;
            }
            else
            {
                foreach (var attr in leftSchema.Schema)
                    merged[attr.Key] = null;
            }

            if (!SemiJoin)
            {
                if (rightEntity != null)
                {
                    foreach (var attr in rightEntity.Attributes)
                        merged[attr.Key] = attr.Value;
                }
                else
                {
                    foreach (var attr in rightSchema.Schema)
                        merged[attr.Key] = null;
                }
            }

            foreach (var definedValue in DefinedValues)
            {
                if (rightEntity == null)
                    merged[definedValue.Key] = null;
                else
                    merged[definedValue.Key] = rightEntity[definedValue.Value];
            }

            return merged;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return LeftSource;
            yield return RightSource;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var outerSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var innerSchema = GetRightSchema(metadata, parameterTypes);

            var schema = new NodeSchema();

            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                schema.PrimaryKey = outerSchema.PrimaryKey;

            foreach (var subSchema in new[] { outerSchema, innerSchema })
            {
                // Semi-join does not include data from the right source
                if (SemiJoin && subSchema == innerSchema)
                    continue;

                foreach (var column in subSchema.Schema)
                    schema.Schema[column.Key] = column.Value;

                foreach (var alias in subSchema.Aliases)
                {
                    if (!schema.Aliases.TryGetValue(alias.Key, out var aliasDetails))
                    {
                        aliasDetails = new List<string>();
                        schema.Aliases[alias.Key] = aliasDetails;
                    }

                    schema.Aliases[alias.Key].AddRange(alias.Value);
                }
            }

            foreach (var definedValue in DefinedValues)
                schema.Schema[definedValue.Key] = innerSchema.Schema[definedValue.Value];

            return schema;
        }

        protected virtual NodeSchema GetRightSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return RightSource.GetSchema(metadata, parameterTypes);
        }
    }
}
