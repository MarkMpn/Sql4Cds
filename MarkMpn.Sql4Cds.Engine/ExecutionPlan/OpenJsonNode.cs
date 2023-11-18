using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

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
            Schema = tvf.SchemaDeclarationItems.Count == 0 ? null : tvf.SchemaDeclarationItems;

            // Check expressions are string types and add conversions if not
            var ecc = new ExpressionCompilationContext(context, null, null);
            if (Json.GetType(ecc, out _) != typeof(SqlString))
                Json = new ConvertCall { Parameter = Json, DataType = DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault) };
            if (Path != null && Path.GetType(ecc, out _) != typeof(SqlString))
                Path = new ConvertCall { Parameter = Path, DataType = DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault) };

            // Validate the schema definition
            if (Schema != null)
            {
                var schema = GetSchema(context);

                // Set up the conversion functions to convert the string values extracted from the JSON to the type defined in the schema
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

            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var columns = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            if (Schema == null)
            {
                if (_jsonCollation == null)
                {
                    var ecc = new ExpressionCompilationContext(context, null, null);
                    Json.GetType(ecc, out var jsonType);
                    _jsonCollation = (jsonType as SqlDataTypeReferenceWithCollation)?.Collation ?? context.PrimaryDataSource.DefaultCollation;
                }

                columns.Add(PrefixWithAlias("key", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(4000, _keyCollation, CollationLabel.Implicit), false, false));
                columns.Add(PrefixWithAlias("value", aliases), new ColumnDefinition(DataTypeHelpers.NVarChar(Int32.MaxValue, _jsonCollation, CollationLabel.Implicit), true, false));
                columns.Add(PrefixWithAlias("type", aliases), new ColumnDefinition(DataTypeHelpers.Int, false, false));
            }
            else
            {
                foreach (var col in Schema)
                {
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
                            throw new NotSupportedQueryFragmentException("AS JSON option can be specified only for column of nvarchar(max) type in WITH clause", col.ColumnDefinition.DataType);
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
            JsonElement jsonDoc;
            JsonElement? jtoken;

            try
            {
                jpath = new JsonPath(path);
                jsonDoc = JsonDocument.Parse(json.Value).RootElement;

                // Don't allow JSON scalar values, only objects or arrays
                if (jsonDoc.ValueKind != JsonValueKind.Object && jsonDoc.ValueKind != JsonValueKind.Array)
                    throw new JsonException("JSON text is not properly formatted. Object or array is required");

                jtoken = jpath.Evaluate(jsonDoc);
            }
            catch (JsonException ex)
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

            if (jtoken.Value.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in jtoken.Value.EnumerateObject())
                {
                    if (Schema == null)
                    {
                        var key = _keyCollation.ToSqlString(prop.Name);
                        var value = _jsonCollation.ToSqlString(GetValue(prop.Value));
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
            else if (jtoken.Value.ValueKind == JsonValueKind.Array)
            {
                var i = 0;

                foreach (var item in jtoken.Value.EnumerateArray())
                {
                    if (Schema == null)
                    {
                        var key = _keyCollation.ToSqlString(i.ToString());
                        var value = _jsonCollation.ToSqlString(GetValue(item));
                        var type = GetType(item);

                        yield return new Entity
                        {
                            [keyCol] = key,
                            [valueCol] = value,
                            [typeCol] = type
                        };
                    }
                    else
                    {
                        yield return TokenToEntity(item, schema, mappings);
                    }

                    i++;
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

        private Entity TokenToEntity(JsonElement token, INodeSchema schema, JsonPath[] mappings)
        {
            var result = new Entity();

            for (var i = 0; i < schema.Schema.Count; i++)
            {
                var mapping = mappings[i];
                JsonElement? value;

                if (mapping != null)
                {
                    value = mapping.Evaluate(token);
                }
                else if (token.ValueKind == JsonValueKind.Object)
                {
                    if (token.TryGetProperty(Schema[i].ColumnDefinition.ColumnIdentifier.Value, out var prop))
                        value = prop;
                    else
                        value = null;
                }
                else
                {
                    value = null;
                }

                string stringValue;

                if (Schema[i].AsJson)
                {
                    if (value?.ValueKind == JsonValueKind.Array || value?.ValueKind == JsonValueKind.Object)
                        stringValue = value.Value.ToString();
                    else if (mapping == null || mapping.Mode == JsonPathMode.Lax)
                        stringValue = null;
                    else
                        throw new QueryExecutionException("Object or array cannot be found in the specified JSON path");
                }
                else
                {
                    if (value?.ValueKind == JsonValueKind.Array || value?.ValueKind == JsonValueKind.Object || value == null)
                    {
                        if (mapping == null || mapping.Mode == JsonPathMode.Lax)
                            stringValue = null;
                        else if (value == null)
                            throw new QueryExecutionException("Property cannot be found on the specified JSON path");
                        else
                            throw new QueryExecutionException("Object or array cannot be found in the specified JSON path");
                    }
                    else
                    {
                        stringValue = GetValue(value.Value);
                    }
                }

                var sqlStringValue = Collation.USEnglish.ToSqlString(stringValue);
                var sqlValue = _conversions[i](sqlStringValue);

                result[PrefixWithAlias(Schema[i].ColumnDefinition.ColumnIdentifier.Value, null)] = sqlValue;
            }

            return result;
        }

        private string GetValue(JsonElement token)
        {
            switch (token.ValueKind)
            {
                case JsonValueKind.Object:
                case JsonValueKind.Array:
                    return token.ToString();

                case JsonValueKind.String:
                    return token.GetString();

                case JsonValueKind.Number:
                    return token.GetDecimal().ToString();

                case JsonValueKind.True:
                case JsonValueKind.False:
                    return token.GetBoolean() ? "true" : "false";

                default:
                    return null;
            }
        }

        private SqlInt32 GetType(JsonElement token)
        {
            switch (token.ValueKind)
            {
                case JsonValueKind.Null:
                    return 0;

                case JsonValueKind.String:
                    return 1;

                case JsonValueKind.Number:
                    return 2;

                case JsonValueKind.True:
                case JsonValueKind.False:
                    return 3;

                case JsonValueKind.Array:
                    return 4;

                case JsonValueKind.Object:
                    return 5;

                default:
                    throw new QueryExecutionException($"Unexpected token type: {token.ValueKind}");
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

        public override string ToString()
        {
            return $"Table Valued Function\r\n[OPENJSON_{(Schema == null ? "DEFAULT" : "EXPLICIT")}]";
        }
    }
}
