using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenJsonNode : BaseDataNode
    {
        private Func<ExpressionExecutionContext, object> _jsonExpression;
        private Func<ExpressionExecutionContext, object> _pathExpression;
        private Collation _jsonCollation;

        private static readonly Collation _keyCollation;

        static OpenJsonNode()
        {
            _keyCollation = new Collation(null, 1033, SqlCompareOptions.BinarySort2, null);
        }

        private OpenJsonNode()
        {
        }

        public OpenJsonNode(OpenJsonTableReference tvf)
        {
            Alias = tvf.Alias?.Value;
            Json = tvf.Variable.Clone();
            Path = tvf.RowPattern?.Clone();

            // TODO: Check expressions are string types and add conversions if not
        }

        /// <summary>
        /// The alias for the data source
        /// </summary>
        [Category("Open JSON")]
        [Description("The alias for the data source")]
        public string Alias { get; set; }

        /// <summary>
        /// The expression that provides the JSON to parse
        /// </summary>
        [Category("Open JSON")]
        [Description("The expression that provides the JSON to parse")]
        public ScalarExpression Json {  get; set; }

        /// <summary>
        /// The expression that defines the JSON path to the object or array to parse
        /// </summary>
        [Category("Open JSON")]
        [Description("The expression that defines the JSON path to the object or array to parse")]
        public ScalarExpression Path { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var ecc = new ExpressionCompilationContext(context, null, null);
            _jsonExpression = Json.Compile(ecc);
            _pathExpression = Path?.Compile(ecc);

            Json.GetType(ecc, out var jsonType);
            _jsonCollation = (jsonType as SqlDataTypeReferenceWithCollation)?.Collation ?? context.PrimaryDataSource.DefaultCollation;

            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var columns = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            columns.Add(PrefixWithAlias("key", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(4000, _keyCollation, CollationLabel.Implicit), false, false));
            columns.Add(PrefixWithAlias("value", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(Int32.MaxValue, _jsonCollation, CollationLabel.Implicit), true, false));
            columns.Add(PrefixWithAlias("type", aliases), new ColumnDefinition(DataTypeHelpers.Int, false, false));

            var schema = new NodeSchema(
                columns,
                aliases,
                columns.First().Key,
                null
                );

            return schema;
        }

        private string PrefixWithAlias(string name, IDictionary<string, IReadOnlyList<string>> aliases)
        {
            name = name.EscapeIdentifier();

            if (Alias == null)
                return name;

            var fullName = Alias.EscapeIdentifier() + "." + name;

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

            var json = (SqlString) _jsonExpression(eec);

            if (json.IsNull || json.Value.Length == 0)
                yield break;

            string path;

            if (_pathExpression != null)
            {
                var pathValue = (SqlString)_pathExpression(eec);

                if (pathValue.IsNull)
                    yield break;

                path = pathValue.Value;
            }
            else
            {
                path = "$";
            }

            JsonPath jpath;
            JToken jsonDoc;
            JToken jtoken;

            try
            {
                jpath = new JsonPath(path);
                jsonDoc = JToken.Parse(json.Value);
                jtoken = jpath.Evaluate(jsonDoc);
            }
            catch (Newtonsoft.Json.JsonException ex)
            {
                throw new QueryExecutionException(ex.Message, ex);
            }

            if (jtoken == null)
            {
                if (jpath.Mode == JsonPathMode.Lax)
                    yield break;
                else
                    throw new QueryExecutionException("Property does not exist");
            }

            var keyCol = PrefixWithAlias("key", null);
            var valueCol = PrefixWithAlias("value", null);
            var typeCol = PrefixWithAlias("type", null);

            if (jtoken.Type == JTokenType.Object)
            {
                foreach (var prop in ((JObject)jtoken).Properties())
                {
                    var key = _keyCollation.ToSqlString(prop.Name);
                    var value = GetValue(prop.Value);
                    var type = GetType(prop.Value);

                    yield return new Entity
                    {
                        [keyCol] = key,
                        [valueCol] = value,
                        [typeCol] = type
                    };
                }
            }
            else if (jtoken.Type == JTokenType.Array)
            {
                for (var i = 0; i < ((JArray)jtoken).Count; i++)
                {
                    var subToken = ((JArray)jtoken)[i];
                    var key = _keyCollation.ToSqlString(i.ToString());
                    var value = GetValue(subToken);
                    var type = GetType(subToken);

                    yield return new Entity
                    {
                        [keyCol] = key,
                        [valueCol] = value,
                        [typeCol] = type
                    };
                }
            }
            else
            {
                if (jpath.Mode == JsonPathMode.Lax)
                    yield break;
                else
                    throw new QueryExecutionException("Not an object or array");
            }
        }

        private SqlString GetValue(JToken token)
        {
            string str;

            if (token is JContainer)
                str = token.ToString();
            else
                str = token.Value<string>();

            return _jsonCollation.ToSqlString(str);
        }

        private SqlInt32 GetType(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Null:
                    return 0;

                case JTokenType.String:
                    return 1;

                case JTokenType.Integer:
                case JTokenType.Float:
                    return 2;

                case JTokenType.Boolean:
                    return 3;

                case JTokenType.Array:
                    return 4;

                case JTokenType.Object:
                    return 5;

                default:
                    throw new QueryExecutionException($"Unexpected token type: {token.Type}");
            }
        }

        public override object Clone()
        {
            return new OpenJsonNode
            {
                Alias = Alias,
                Json = Json.Clone(),
                Path = Path.Clone(),
                _jsonExpression = _jsonExpression,
                _pathExpression = _pathExpression,
                _jsonCollation = _jsonCollation
            };
        }
    }
}
