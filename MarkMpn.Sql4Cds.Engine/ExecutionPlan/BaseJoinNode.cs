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
        private bool _lastSchemaIncludedSemiJoin;

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
        /// Indicates if columns from the <see cref="LeftSource"/> should be included in the output
        /// </summary>
        [Browsable(false)]
        public bool OutputLeftSchema { get; set; } = true;

        /// <summary>
        /// Indicates if columns from the <see cref="RightSource"/> should be included in the output
        /// </summary>
        [Browsable(false)]
        public bool OutputRightSchema { get; set; } = true;

        /// <summary>
        /// Creates the output record by merging the records that have been matched from the left and right sources
        /// </summary>
        /// <param name="leftEntity">The data from the left source</param>
        /// <param name="leftSchema">The schema of the left source</param>
        /// <param name="rightEntity">The data from the right source</param>
        /// <param name="rightSchema">The schema of the right source</param>
        /// <returns>The merged data</returns>
        protected Entity Merge(Entity leftEntity, INodeSchema leftSchema, Entity rightEntity, INodeSchema rightSchema, bool includeSemiJoin)
        {
            var merged = new Entity();

            if (OutputLeftSchema || includeSemiJoin)
            {
                if (leftEntity != null)
                {
                    foreach (var attr in leftSchema.Schema)
                        merged[attr.Key] = leftEntity[attr.Key];
                }
                else
                {
                    foreach (var attr in leftSchema.Schema)
                        merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value.Type.ToNetType(out _));
                }
            }

            if (OutputRightSchema || includeSemiJoin)
            {
                if (rightEntity != null)
                {
                    foreach (var attr in rightSchema.Schema)
                        merged[attr.Key] = rightEntity[attr.Key];
                }
                else
                {
                    foreach (var attr in rightSchema.Schema)
                        merged[attr.Key] = SqlTypeConverter.GetNullValue(attr.Value.Type.ToNetType(out _));
                }
            }

            foreach (var definedValue in DefinedValues)
            {
                if (rightEntity == null)
                    merged[definedValue.Key] = SqlTypeConverter.GetNullValue(rightSchema.Schema[definedValue.Value].Type.ToNetType(out _));
                else
                    merged[definedValue.Key] = rightEntity[definedValue.Value];
            }

            return merged;
        }

        protected void FoldDefinedValues(INodeSchema rightSchema)
        {
            foreach (var kvp in DefinedValues.ToList())
            {
                if (!rightSchema.ContainsColumn(kvp.Value, out var innerColumn))
                    throw new NotSupportedQueryFragmentException($"Unknown defined column '{kvp.Value}'");

                if (innerColumn != kvp.Value)
                    DefinedValues[kvp.Key] = innerColumn;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return LeftSource;
            yield return RightSource;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return GetSchema(context, false);
        }

        protected virtual INodeSchema GetSchema(NodeCompilationContext context, bool includeSemiJoin)
        {
            var outerSchema = LeftSource.GetSchema(context);
            var innerSchema = GetRightSchema(context);

            if (outerSchema == _lastLeftSchema && innerSchema == _lastRightSchema && includeSemiJoin == _lastSchemaIncludedSemiJoin)
                return _lastSchema;

            var schema = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            var primaryKey = GetPrimaryKey(outerSchema, innerSchema);

            foreach (var subSchema in new[] { outerSchema, innerSchema })
            {
                if (subSchema == outerSchema && !OutputLeftSchema && !includeSemiJoin)
                    continue;

                if (subSchema == innerSchema && !OutputRightSchema && !includeSemiJoin)
                    continue;

                foreach (var column in subSchema.Schema)
                {
                    var nullable = true;

                    if (!column.Value.IsNullable &&
                        (
                            ((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter) && subSchema == outerSchema) ||
                            ((JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter) && subSchema == innerSchema)
                        ))
                    {
                        nullable = false;
                    }

                    schema[column.Key] = new ColumnDefinition(column.Value.Type, nullable, column.Value.IsCalculated, column.Value.IsVisible);
                }

                foreach (var alias in subSchema.Aliases)
                {
                    if (!aliases.TryGetValue(alias.Key, out var aliasDetails))
                    {
                        aliasDetails = new List<string>();
                        aliases[alias.Key] = aliasDetails;
                    }

                    ((List<string>)aliasDetails).AddRange(alias.Value);
                }
            }

            foreach (var definedValue in DefinedValues)
            {
                innerSchema.ContainsColumn(definedValue.Value, out var innerColumn);

                var definedValueSchemaCol = innerSchema.Schema[innerColumn].Invisible().Calculated();

                if (JoinType == QualifiedJoinType.LeftOuter)
                    definedValueSchemaCol = definedValueSchemaCol.Null();

                schema[definedValue.Key] = definedValueSchemaCol;
            }

            _lastLeftSchema = outerSchema;
            _lastRightSchema = innerSchema;
            _lastSchema = new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                sortOrder: GetSortOrder(outerSchema, innerSchema));
            _lastSchemaIncludedSemiJoin = includeSemiJoin;
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

        protected virtual INodeSchema GetRightSchema(NodeCompilationContext context)
        {
            return RightSource.GetSchema(context);
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
