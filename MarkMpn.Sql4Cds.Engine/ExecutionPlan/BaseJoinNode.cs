using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A base class for nodes that join the results of two other nodes
    /// </summary>
    abstract class BaseJoinNode : BaseDataNode
    {
        /// <summary>
        /// The first data source to merge
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode LeftSource { get; set; }

        /// <summary>
        /// The second data source to merge
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode RightSource { get; set; }

        /// <summary>
        /// The type of join to apply
        /// </summary>
        [Category("Join")]
        [Description("The type of join to apply")]
        [DisplayName("Join Type")]
        public QualifiedJoinType JoinType { get; set; }

        /// <summary>
        /// Indicates if a semi join should be used (single output row for each row from the left source, ignoring duplicate matches from the right source)
        /// </summary>
        [Category("Join")]
        [Description("Indicates if a semi join should be used (single output row for each row from the left source, ignoring duplicate matches from the right source)")]
        [DisplayName("Semi Join")]
        public bool SemiJoin { get; set; }

        /// <summary>
        /// For semi joins, lists individual columns that should be created in the output and their corresponding source from the right input
        /// </summary>
        [Category("Join")]
        [Description("For semi joins, lists individual columns that should be created in the output and their corresponding source from the right input")]
        [DisplayName("Defined Values")]
        public IDictionary<string, string> DefinedValues { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Creates the output record by merging the records that have been matched from the left and right sources
        /// </summary>
        /// <param name="leftEntity">The data from the left source</param>
        /// <param name="leftSchema">The schema of the left source</param>
        /// <param name="rightEntity">The data from the right source</param>
        /// <param name="rightSchema">The schema of the right source</param>
        /// <returns>The merged data</returns>
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
                    merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value);
            }

            if (rightEntity != null)
            {
                foreach (var attr in rightEntity.Attributes)
                    merged[attr.Key] = attr.Value;
            }
            else
            {
                foreach (var attr in rightSchema.Schema)
                    merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value);
            }

            foreach (var definedValue in DefinedValues)
            {
                if (rightEntity == null)
                    merged[definedValue.Key] = SqlTypeConverter.GetNullValue(rightSchema.Schema[definedValue.Value]);
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

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return GetSchema(dataSources, parameterTypes, false);
        }

        protected virtual NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, bool includeSemiJoin)
        {
            var outerSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var innerSchema = GetRightSchema(dataSources, parameterTypes);

            var schema = new NodeSchema();

            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                schema.PrimaryKey = outerSchema.PrimaryKey;

            foreach (var subSchema in new[] { outerSchema, innerSchema })
            {
                // Semi-join does not include data from the right source
                if (SemiJoin && subSchema == innerSchema && !includeSemiJoin)
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

        protected virtual NodeSchema GetRightSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return RightSource.GetSchema(dataSources, parameterTypes);
        }
    }
}
