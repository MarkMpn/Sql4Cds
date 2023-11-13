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
        private List<Func<INullable, INullable>> _conversions;

        private static readonly Collation _keyCollation;

        static OpenJsonNode()
        {
            _keyCollation = new Collation(null, 1033, SqlCompareOptions.BinarySort2, null);
        }

        private OpenJsonNode()
        {
        }

        public OpenJsonNode(OpenJsonTableReference tvf, NodeCompilationContext context)
        {
            Alias = tvf.Alias?.Value;
            Json = tvf.Variable.Clone();
            Path = tvf.RowPattern?.Clone();
            Schema = tvf.SchemaDeclarationItems;

            // TODO: Check expressions are string types and add conversions if not

            // Validate the schema definition
            if (Schema != null)
            {
                var schema = GetSchema(context);

                var sourceType = DataTypeHelpers.NVarChar(Int32.MaxValue, Collation.USEnglish, CollationLabel.Implicit);
                _conversions = schema.Schema
                    .Select(col => SqlTypeConverter.GetConversion(sourceType, col.Value.Type))
                    .ToList();
            }
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

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        [Browsable(false)]
        public IList<SchemaDeclarationItemOpenjson> Schema { get; set; }

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

            if (Schema == null)
            {
                columns.Add(PrefixWithAlias("key", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(4000, _keyCollation, CollationLabel.Implicit), false, false));
                columns.Add(PrefixWithAlias("value", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(Int32.MaxValue, _jsonCollation, CollationLabel.Implicit), true, false));
                columns.Add(PrefixWithAlias("type", aliases), new ColumnDefinition(DataTypeHelpers.Int, false, false));
            }
            else
            {
                var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var col in Schema)
                {
                    if (!columnNames.Add(col.ColumnDefinition.ColumnIdentifier.Value))
                        throw new NotSupportedQueryFragmentException("Duplicate column name", col.ColumnDefinition.ColumnIdentifier);

                    var type = col.ColumnDefinition.DataType;

                    if (type is SqlDataTypeReference sqlType && sqlType.SqlDataTypeOption.IsStringType())
                    {
                        var collation = context.PrimaryDataSource.DefaultCollation;

                        if (col.ColumnDefinition.Collation != null && !Collation.TryParse(col.ColumnDefinition.Collation.Value, out collation))
                            throw new NotSupportedQueryFragmentException("Invalid collation", col.ColumnDefinition.Collation);

                        type = new SqlDataTypeReferenceWithCollation
                        {
                            SqlDataTypeOption = sqlType.SqlDataTypeOption,
                            Collation = collation,
                            CollationLabel = col.ColumnDefinition.Collation == null ? CollationLabel.CoercibleDefault : CollationLabel.Implicit
                        };

                        foreach (var param in sqlType.Parameters)
                            ((SqlDataTypeReferenceWithCollation)type).Parameters.Add(param);
                    }

                    if (col.AsJson)
                    {
                        if (!(type is SqlDataTypeReference nvarcharType) ||
                            nvarcharType.SqlDataTypeOption != SqlDataTypeOption.NVarChar ||
                            nvarcharType.Parameters.Count != 1 ||
                            !(nvarcharType.Parameters[0] is MaxLiteral))
                        {
                            throw new NotSupportedQueryFragmentException("AS JSON column must be of NVARCHAR(max) type", col.ColumnDefinition.DataType);
                        }
                    }

                    columns.Add(PrefixWithAlias(col.ColumnDefinition.ColumnIdentifier.Value, aliases), new ColumnDefinition(type, true, false));
                }
            }

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

            var schema = GetSchema(context);
            var keyCol = PrefixWithAlias("key", null);
            var valueCol = PrefixWithAlias("value", null);
            var typeCol = PrefixWithAlias("type", null);

            JsonPath[] mappings = null;

            if (Schema != null)
            {
                mappings = Schema
                    .Select(col => col.Mapping as StringLiteral)
                    .Select(mapping => mapping == null ? null : new JsonPath(mapping.Value))
                    .ToArray();
            }

            if (jtoken.Type == JTokenType.Object)
            {
                foreach (var prop in ((JObject)jtoken).Properties())
                {
                    if (Schema == null)
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
                    else
                    {
                        yield return TokenToEntity(prop.Value, schema, mappings);
                    }
                }
            }
            else if (jtoken.Type == JTokenType.Array)
            {
                for (var i = 0; i < ((JArray)jtoken).Count; i++)
                {
                    if (Schema == null)
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
                    else
                    {
                        yield return TokenToEntity(((JArray)jtoken)[i], schema, mappings);
                    }
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

        private Entity TokenToEntity(JToken token, INodeSchema schema, JsonPath[] mappings)
        {
            var result = new Entity();

            for (var i = 0; i < schema.Schema.Count; i++)
            {
                var mapping = mappings[i];
                JToken value;

                if (mapping != null)
                    value = mapping.Evaluate(token);
                else if (token is JObject obj)
                    value = obj.Property(Schema[i].ColumnDefinition.ColumnIdentifier.Value)?.Value;
                else
                    value = null;

                string stringValue;

                if (Schema[i].AsJson)
                {
                    if (value is JArray || value is JObject)
                        stringValue = value.ToString();
                    else if (mapping == null || mapping.Mode == JsonPathMode.Lax)
                        stringValue = null;
                    else
                        throw new QueryExecutionException("");
                }
                else
                {
                    if (value is JArray || value is JObject)
                    {
                        if (mapping == null || mapping.Mode == JsonPathMode.Lax)
                            stringValue = null;
                        else
                            throw new QueryExecutionException("");
                    }
                    else
                    {
                        stringValue = value.Value<string>();
                    }
                }

                var sqlStringValue = Collation.USEnglish.ToSqlString(stringValue);
                var sqlValue = _conversions[i](sqlStringValue);

                result[PrefixWithAlias(Schema[i].ColumnDefinition.ColumnIdentifier.Value, null)] = sqlValue;
            }

            return result;
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
                Schema = Schema,
                _jsonExpression = _jsonExpression,
                _pathExpression = _pathExpression,
                _jsonCollation = _jsonCollation,
                _conversions = _conversions,
            };
        }
    }
}
