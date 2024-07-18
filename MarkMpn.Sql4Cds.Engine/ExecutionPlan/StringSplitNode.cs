using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class StringSplitNode : BaseDataNode
    {
        private Func<ExpressionExecutionContext, object> _inputExpression;
        private Func<ExpressionExecutionContext, object> _separatorExpression;
        private Collation _inputCollation;

        private StringSplitNode()
        {
        }

        public StringSplitNode(GlobalFunctionTableReference function, NodeCompilationContext context)
        {
            if (function.Parameters.Count > 3)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TooManyArguments(function.Name));

            if (function.Parameters.Count < 2)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InsufficientArguments(function.Name));

            Alias = function.Alias?.Value;
            Input = function.Parameters[0].Clone();
            Separator = function.Parameters[1].Clone();

            // Check expressions are string types and add conversions if not
            var ecc = new ExpressionCompilationContext(context, null, null);
            if (Input.GetType(ecc, out _) != typeof(SqlString))
                Input = new ConvertCall { Parameter = Input, DataType = DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault) };
            if (Separator != null && Separator.GetType(ecc, out _) != typeof(SqlString))
                Separator = new ConvertCall { Parameter = Separator, DataType = DataTypeHelpers.NVarChar(1, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault) };

            // If an ordinal is specified, it must be a constant
            if (function.Parameters.Count == 3)
            {
                if (!(function.Parameters[2] is Literal ordinalLiteral))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.StringSplitOrdinalRequiresLiteral(function.Parameters[2]));

                if (ordinalLiteral.LiteralType != LiteralType.Integer)
                {
                    ordinalLiteral.GetType(ecc, out var ordinalType);
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidArgumentType(ordinalLiteral, ordinalType, 3, "string_split"));
                }

                var ordinalValue = Int32.Parse(ordinalLiteral.Value);
                if (ordinalValue != 0 && ordinalValue != 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidArgumentValue(ordinalLiteral, ordinalValue, 3, "string_split"));

                EnableOrdinal = ordinalValue == 1;
            }
        }

        /// <summary>
        /// The alias for the data source
        /// </summary>
        [Category("String Split")]
        [Description("The alias for the data source")]
        public string Alias { get; set; }

        /// <summary>
        /// The expression that provides the string to split
        /// </summary>
        [Category("String Split")]
        [Description("The expression that provides the string to split")]
        public ScalarExpression Input {  get; set; }

        /// <summary>
        /// The expression that defines the separator to split on
        /// </summary>
        [Category("String Split")]
        [Description("The expression that defines the separator to split on")]
        public ScalarExpression Separator { get; set; }

        /// <summary>
        /// Indicates whether the output should contain an ordinal column
        /// </summary>
        [Category("String Split")]
        [Description("Indicates whether the output should contain an ordinal column")]
        public bool EnableOrdinal { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var ecc = new ExpressionCompilationContext(context, null, null);
            _inputExpression = Input.Compile(ecc);
            _separatorExpression = Separator?.Compile(ecc);

            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var columns = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            if (_inputCollation == null)
            {
                var ecc = new ExpressionCompilationContext(context, null, null);
                Input.GetType(ecc, out var inputType);
                _inputCollation = (inputType as SqlDataTypeReferenceWithCollation)?.Collation ?? context.PrimaryDataSource.DefaultCollation;
            }

            columns.Add(PrefixWithAlias("value", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(Int32.MaxValue, _inputCollation, CollationLabel.Implicit), false, false));

            if (EnableOrdinal)
                columns.Add(PrefixWithAlias("ordinal", aliases), new ColumnDefinition(DataTypeHelpers.Int, false, false));

            var schema = new NodeSchema(
                columns,
                aliases,
                null,
                null
                );

            return schema;
        }

        private string PrefixWithAlias(string name, IDictionary<string, IReadOnlyList<string>> aliases)
        {
            name = name.EscapeIdentifier();

            var fullName = Alias == null ? name : (Alias.EscapeIdentifier() + "." + name);

            if (aliases != null)
            {
                if (!aliases.TryGetValue(name, out var alias))
                {
                    alias = new List<string>();
                    aliases[name] = alias;
                }

                ((List<string>)alias).Add(fullName);
            }

            return fullName;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return new RowCountEstimate(10);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var eec = new ExpressionExecutionContext(context);

            var input = (SqlString) _inputExpression(eec);
            var separator = (SqlString) _separatorExpression(eec);

            if (separator.IsNull || separator.Value.Length != 1)
                throw new QueryExecutionException(Sql4CdsError.InvalidProcedureParameterType(Separator, "separator", "nchar(1)/nvarchar(1)"));

            if (input.IsNull || input.Value.Length == 0)
                yield break;

            var parts = input.Value.Split(separator.Value[0]);
            var ordinal = 1;

            foreach (var part in parts)
            {
                var entity = new Entity
                {
                    [PrefixWithAlias("value", null)] = _inputCollation.ToSqlString(part),
                    [PrefixWithAlias("ordinal", null)] = (SqlInt32)ordinal
                };

                yield return entity;
                ordinal++;
            }
        }

        public override object Clone()
        {
            return new StringSplitNode
            {
                Alias = Alias,
                Input = Input.Clone(),
                Separator = Separator.Clone(),
                EnableOrdinal = EnableOrdinal,
                _inputExpression = _inputExpression,
                _separatorExpression = _separatorExpression,
                _inputCollation = _inputCollation
            };
        }

        public override string ToString()
        {
            return "Table Valued Function\r\n[STRING_SPLIT]";
        }
    }
}
