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
        private INodeSchema _lastLeftSchema;
        private INodeSchema _lastRightSchema;
        private INodeSchema _lastSchema;

        /// <summary>
        /// The first data source to merge
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal LeftSource { get; set; }

        /// <summary>
        /// The second data source to merge
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal RightSource { get; set; }

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
        protected Entity Merge(Entity leftEntity, INodeSchema leftSchema, Entity rightEntity, INodeSchema rightSchema)
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
                    merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value.ToNetType(out _));
            }

            if (rightEntity != null)
            {
                foreach (var attr in rightEntity.Attributes)
                    merged[attr.Key] = attr.Value;
            }
            else
            {
                foreach (var attr in rightSchema.Schema)
                    merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value.ToNetType(out _));
            }

            foreach (var definedValue in DefinedValues)
            {
                if (rightEntity == null)
                    merged[definedValue.Key] = SqlTypeConverter.GetNullValue(rightSchema.Schema[definedValue.Value].ToNetType(out _));
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

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return GetSchema(dataSources, parameterTypes, false);
        }

        protected virtual INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, bool includeSemiJoin)
        {
            var outerSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var innerSchema = GetRightSchema(dataSources, parameterTypes);

            if (outerSchema == _lastLeftSchema && innerSchema == _lastRightSchema)
                return _lastSchema;

            var schema = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            var primaryKey = GetPrimaryKey(outerSchema, innerSchema);
            var notNullColumns = new List<string>();

            foreach (var subSchema in new[] { outerSchema, innerSchema })
            {
                // Semi-join does not include data from the outer source
                if (SemiJoin && (JoinType == QualifiedJoinType.RightOuter && subSchema == outerSchema || JoinType != QualifiedJoinType.RightOuter && subSchema == innerSchema) && !includeSemiJoin)
                    continue;

                foreach (var column in subSchema.Schema)
                    schema[column.Key] = column.Value;

                foreach (var alias in subSchema.Aliases)
                {
                    if (!aliases.TryGetValue(alias.Key, out var aliasDetails))
                    {
                        aliasDetails = new List<string>();
                        aliases[alias.Key] = aliasDetails;
                    }

                    ((List<string>)aliasDetails).AddRange(alias.Value);
                }

                if (((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter) && subSchema == outerSchema) ||
                    ((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter) && subSchema == innerSchema))
                {
                    foreach (var col in subSchema.NotNullColumns)
                        notNullColumns.Add(col);
                }
            }

            foreach (var definedValue in DefinedValues)
                schema[definedValue.Key] = innerSchema.Schema[definedValue.Value];

            _lastLeftSchema = outerSchema;
            _lastRightSchema = innerSchema;
            _lastSchema = new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                notNullColumns: notNullColumns,
                sortOrder: GetSortOrder(outerSchema, innerSchema));
            return _lastSchema;
        }

        protected virtual string GetPrimaryKey(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                return outerSchema.PrimaryKey;

            return null;
        }

        protected virtual IReadOnlyList<string> GetSortOrder(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            return null;
        }

        protected virtual INodeSchema GetRightSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return RightSource.GetSchema(dataSources, parameterTypes);
        }

        public override string ToString()
        {
            var name = base.ToString();
            name += "\r\n(";

            switch (JoinType)
            {
                case QualifiedJoinType.Inner:
                    name += "Inner";
                    break;

                case QualifiedJoinType.LeftOuter:
                    name += "Left Outer";
                    break;

                case QualifiedJoinType.RightOuter:
                    name += "Right Outer";
                    break;

                case QualifiedJoinType.FullOuter:
                    name += "Full Outer";
                    break;
            }

            if (SemiJoin)
                name += " Semi";

            name += " Join)";

            return name;
        }
    }
}
