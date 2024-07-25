using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Web;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Collects information relevant to a warning or error returned by SQL 4 CDS
    /// </summary>
    public class Sql4CdsError
    {
        private static Dictionary<int, Sql4CdsError> _errorNumberToDetails;

        static Sql4CdsError()
        {
            _errorNumberToDetails = new Dictionary<int, Sql4CdsError>();

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.Engine.Resources.Errors.csv"))
            using (var reader = new StreamReader(stream))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    var number = Int32.Parse(parts[0]);
                    var @class = byte.Parse(parts[1]);
                    var message = String.Join(",", parts, 2, parts.Length - 2);

                    _errorNumberToDetails[number] = new Sql4CdsError(@class, number, message);
                }
            }
        }

        internal Sql4CdsError(byte @class, int lineNumber, int number, string procedure, string server, byte state, string message, TSqlFragment fragment = null)
        {
            Class = @class;
            LineNumber = lineNumber;
            Number = number;
            Procedure = procedure;
            Server = server;
            State = state;
            Message = message;
            Fragment = fragment;
        }

        internal Sql4CdsError(byte @class, int number, string message, TSqlFragment fragment = null) : this(@class, -1, number, null, null, 1, message, fragment)
        {
        }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Class"/>
        public byte Class { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.LineNumber"/>
        public int LineNumber { get; internal set; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Number"/>
        public int Number { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Procedure"/>
        public string Procedure { get; internal set; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Class"/>
        public string Server { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.State"/>
        public byte State { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Message"/>
        public string Message { get; }

        /// <summary>
        /// Returns the <see cref="TSqlFragment"/> that caused the error
        /// </summary>
        public TSqlFragment Fragment { get; }

        internal static IEnumerable<Sql4CdsError> GetAllErrors()
        {
            return _errorNumberToDetails.Values;
        }

        internal static Sql4CdsError Create(int number, TSqlFragment fragment)
        {
            var template = _errorNumberToDetails[number];
            return new Sql4CdsError(template.Class, template.Number, (string)ExpressionFunctions.FormatMessage(template.Message, null, Array.Empty<INullable>()), fragment);
        }

        internal static Sql4CdsError Create(int number, TSqlFragment fragment, params INullable[] values)
        {
            var template = _errorNumberToDetails[number];
            return new Sql4CdsError(template.Class, template.Number, (string)ExpressionFunctions.FormatMessage(template.Message, null, values), fragment);
        }

        internal static Sql4CdsError Create(int number, TSqlFragment fragment, params object[] values)
        {
            var template = _errorNumberToDetails[number];
            return new Sql4CdsError(template.Class, template.Number, (string)ExpressionFunctions.FormatMessage(template.Message, null, values.Select(v => Collation.USEnglish.ToSqlString(v.ToString())).Cast<INullable>().ToArray()), fragment);
        }

        internal static Sql4CdsError InternalError(string message, TSqlFragment fragment = null)
        {
            return new Sql4CdsError(16, 10337, message, fragment);
        }

        internal static Sql4CdsError NotSupported(TSqlFragment fragment, string clause)
        {
            return Create(40517, fragment, (SqlInt32)clause.Length, Collation.USEnglish.ToSqlString(clause));
        }

        internal static Sql4CdsError SyntaxError(TSqlFragment fragment)
        {
            var sql = fragment.ToSql();
            return Create(102, fragment, (SqlInt32)sql.Length, Collation.USEnglish.ToSqlString(sql));
        }

        internal static Sql4CdsError SyntaxErrorKeyword(TSqlFragment fragment, string keyword)
        {
            return Create(156, fragment, (SqlInt32)keyword.Length, Collation.USEnglish.ToSqlString(keyword));
        }

        internal static Sql4CdsError TypeClash(TSqlFragment fragment, DataTypeReference type1, DataTypeReference type2)
        {
            return Create(206, fragment, GetTypeName(type1), GetTypeName(type2));
        }

        internal static Sql4CdsError InvalidLengthOrPrecision(SqlDataTypeReference type)
        {
            var precision = Int32.Parse(type.Parameters[0].Value);
            return Create(1001, type, (SqlInt32)type.StartLine, (SqlInt32)precision);
        }

        internal static Sql4CdsError ArithmeticOverflow(DataTypeReference sourceType, DataTypeReference targetType)
        {
            return Create(8115, targetType, GetTypeName(sourceType), GetTypeName(targetType));
        }

        internal static Sql4CdsError ArithmeticOverflow(DataTypeReference targetType, SqlInt32 value)
        {
            return Create(220, targetType, Collation.USEnglish.ToSqlString(GetTypeName(targetType)), value);
        }

        internal static Sql4CdsError UndeclaredVariable(VariableReference variable)
        {
            return Create(137, variable, (SqlInt32)variable.Name.Length, Collation.USEnglish.ToSqlString(variable.Name));
        }

        internal static Sql4CdsError UndeclaredVariable(GlobalVariableExpression variable)
        {
            return Create(137, variable, (SqlInt32)variable.Name.Length, Collation.USEnglish.ToSqlString(variable.Name));
        }

        internal static Sql4CdsError DuplicateColumn(Identifier table, string columnName)
        {
            return Create(8156, table, (SqlInt32)columnName.Length, Collation.USEnglish.ToSqlString(columnName), (SqlInt32)table.Value.Length, Collation.USEnglish.ToSqlString(table.Value));
        }

        internal static Sql4CdsError InvalidCollation(Identifier collation)
        {
            return Create(448, collation, (SqlInt32)collation.Value.Length, Collation.USEnglish.ToSqlString(collation.Value));
        }

        internal static Sql4CdsError NonAggregateColumnReference(ColumnReferenceExpression column)
        {
            var tableName = column.MultiPartIdentifier.Identifiers.Count == 1 ? "" : column.MultiPartIdentifier.Identifiers[column.MultiPartIdentifier.Identifiers.Count - 2].Value;
            var columnName = column.MultiPartIdentifier.Identifiers[column.MultiPartIdentifier.Identifiers.Count - 1].Value;

            return Create(8120, column, (SqlInt32)tableName.Length, Collation.USEnglish.ToSqlString(tableName), (SqlInt32)columnName.Length, Collation.USEnglish.ToSqlString(columnName));
        }

        internal static Sql4CdsError InvalidColumnName(ColumnReferenceExpression column)
        {
            var name = column.GetColumnName();
            return Create(207, column, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidColumnName(string column)
        {
            return Create(207, null, (SqlInt32)column.Length, Collation.USEnglish.ToSqlString(column));
        }

        internal static Sql4CdsError InvalidObjectName(SchemaObjectName obj)
        {
            var name = obj.ToSql();
            return Create(208, obj, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidObjectName(Identifier obj)
        {
            var name = obj.ToSql();
            return Create(208, obj, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError NonFunctionCalledWithParameters(SchemaObjectName obj)
        {
            var name = obj.ToSql();
            return Create(215, obj, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError FunctionCalledWithoutParameters(SchemaObjectName obj)
        {
            var name = obj.ToSql();
            return Create(216, obj, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidSprocName(SchemaObjectName sproc)
        {
            var name = sproc.ToSql();
            return Create(2812, sproc, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError AmbiguousColumnName(ColumnReferenceExpression column)
        {
            var name = column.GetColumnName();
            return Create(209, column, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError ConversionFailed(DataTypeReference sourceType, Literal sourceValue, DataTypeReference targetType)
        {
            return Create(245, sourceValue, Collation.USEnglish.ToSqlString(GetTypeName(sourceType)), (SqlInt32)sourceValue.Value.Length, Collation.USEnglish.ToSqlString(sourceValue.Value), Collation.USEnglish.ToSqlString(GetTypeName(targetType)));
        }

        internal static Sql4CdsError CollationConflict(TSqlFragment fragment, Collation source, Collation target, string operationName)
        {
            return Create(468, fragment, (SqlInt32)(source?.Name.Length ?? 0), Collation.USEnglish.ToSqlString(source?.Name), (SqlInt32)(target?.Name.Length ?? 0), Collation.USEnglish.ToSqlString(target?.Name), Collation.USEnglish.ToSqlString(operationName));
        }

        internal static Sql4CdsError DuplicateAlias(Identifier identifier)
        {
            return Create(1011, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value));
        }

        internal static Sql4CdsError AliasNameSameAsTableName(Identifier alias, Identifier table)
        {
            return Create(1012, alias, (SqlInt32)alias.Value.Length, Collation.USEnglish.ToSqlString(alias.Value), (SqlInt32)table.Value.Length, Collation.USEnglish.ToSqlString(table.Value));
        }

        internal static Sql4CdsError DuplicateTable(SchemaObjectName table1, SchemaObjectName table2)
        {
            var name1 = table1.ToSql();
            var name2 = table2.ToSql();
            return Create(1013, table1, (SqlInt32)name1.Length, Collation.USEnglish.ToSqlString(name1), (SqlInt32)name2.Length, Collation.USEnglish.ToSqlString(name2));
        }

        internal static Sql4CdsError NotNullInsert(Identifier column, Identifier table, string operation, TSqlFragment fragment = null)
        {
            return Create(515, fragment ?? column, (SqlInt32)column.Value.Length, Collation.USEnglish.ToSqlString(column.Value), (SqlInt32)table.Value.Length, Collation.USEnglish.ToSqlString(table.Value), Collation.USEnglish.ToSqlString(operation));
        }

        internal static Sql4CdsError ReadOnlyColumn(ColumnReferenceExpression column)
        {
            var name = column.GetColumnName();
            return Create(271, column, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidDataType(DataTypeReference type)
        {
            var name = GetTypeName(type);
            return Create(2715, type, (SqlInt32)0, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidHint(StringLiteral hint)
        {
            return Create(10715, hint, (SqlInt32)hint.Value.Length, Collation.USEnglish.ToSqlString(hint.Value));
        }

        internal static Sql4CdsError ImpersonationError(string username)
        {
            return Create(15517, null, (SqlInt32)username.Length, Collation.USEnglish.ToSqlString(username));
        }

        internal static Sql4CdsError InsufficientArguments(SchemaObjectName sproc)
        {
            var name = sproc.ToSql();
            return Create(313, sproc, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InsufficientArguments(Identifier function)
        {
            var name = function.ToSql();
            return Create(313, function, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError TooManyArguments(SchemaObjectName sprocOrFunc, bool isSproc)
        {
            var name = sprocOrFunc.ToSql();
            var err = Create(8144, sprocOrFunc, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));

            if (isSproc)
                err.Procedure = sprocOrFunc.BaseIdentifier.Value;

            return err;
        }

        internal static Sql4CdsError TooManyArguments(Identifier function)
        {
            var name = function.ToSql();
            var err = Create(8144, function, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));

            return err;
        }

        internal static Sql4CdsError NamedParametersRequiredAfter(ExecuteParameter param, int paramIndex)
        {
            return Create(119, param, (SqlInt32)paramIndex);
        }

        internal static Sql4CdsError InvalidParameterName(ExecuteParameter param, SchemaObjectName sproc)
        {
            var name = sproc.ToSql();
            var err = Create(8145, param, (SqlInt32)param.Variable.Name.Length - 1, Collation.USEnglish.ToSqlString(param.Variable.Name.Substring(1)), (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
            err.Procedure = sproc.BaseIdentifier.Value;
            return err;
        }

        internal static Sql4CdsError MissingParameter(SchemaObjectName sprocOrFunc, string param, bool isSproc)
        {
            var name = sprocOrFunc.ToSql();
            var err = Create(201, sprocOrFunc, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name), (SqlInt32)param.Length, Collation.USEnglish.ToSqlString(param));

            if (isSproc)
                err.Procedure = sprocOrFunc.BaseIdentifier.Value;

            return err;
        }

        internal static Sql4CdsError ExplicitConversionNotAllowed(TSqlFragment fragment, DataTypeReference from, DataTypeReference to)
        {
            return Create(529, fragment, Collation.USEnglish.ToSqlString(GetTypeName(from)), Collation.USEnglish.ToSqlString(GetTypeName(to)));
        }

        internal static Sql4CdsError InvalidArgumentType(TSqlFragment fragment, DataTypeReference type, int paramNum, string function)
        {
            return Create(8116, fragment, Collation.USEnglish.ToSqlString(GetTypeName(type)), (SqlInt32)paramNum, Collation.USEnglish.ToSqlString(function));
        }

        internal static Sql4CdsError InvalidArgumentValue(TSqlFragment fragment, int value, int paramNum, string function)
        {
            return Create(4199, fragment, (SqlInt32)value, (SqlInt32)paramNum, Collation.USEnglish.ToSqlString(function));
        }

        internal static Sql4CdsError StringTruncation(TSqlFragment fragment, string table, string column, string value)
        {
            return Create(2628, fragment, (SqlInt32)table.Length, Collation.USEnglish.ToSqlString(table), (SqlInt32)column.Length, Collation.USEnglish.ToSqlString(column), (SqlInt32)value.Length, Collation.USEnglish.ToSqlString(value));
        }

        internal static Sql4CdsError ConversionError(TSqlFragment fragment, DataTypeReference from, DataTypeReference to)
        {
            return Create(8114, fragment, Collation.USEnglish.ToSqlString(GetTypeName(from)), Collation.USEnglish.ToSqlString(GetTypeName(to)));
        }

        internal static Sql4CdsError ConversionErrorWithValue(TSqlFragment fragment, DataTypeReference from, DataTypeReference to, SqlString value)
        {
            return Create(245, fragment, Collation.USEnglish.ToSqlString(GetTypeName(from)), (SqlInt32)(value.IsNull ? 0 : value.Value.Length), value, Collation.USEnglish.ToSqlString(GetTypeName(to)));
        }

        internal static Sql4CdsError DateTimeParseError(TSqlFragment fragment)
        {
            return Create(241, fragment);
        }

        internal static Sql4CdsError MoneyParseError(TSqlFragment fragment)
        {
            return Create(235, fragment);
        }

        internal static Sql4CdsError SmallMoneyParseError(TSqlFragment fragment)
        {
            return Create(293, fragment);
        }

        internal static Sql4CdsError GuidParseError(TSqlFragment fragment)
        {
            return Create(8169, fragment);
        }

        internal static Sql4CdsError AsJsonRequiresNVarCharMax(DataTypeReference fragment)
        {
            return Create(13618, fragment);
        }

        internal static Sql4CdsError JsonPropertyNotFound(TSqlFragment fragment)
        {
            return Create(13608, fragment);
        }

        internal static Sql4CdsError JsonScalarValueNotFound(TSqlFragment fragment)
        {
            return Create(13623, fragment);
        }

        internal static Sql4CdsError JsonObjectOrArrayNotFound(TSqlFragment fragment)
        {
            return Create(13624, fragment);
        }

        internal static Sql4CdsError JsonStringTruncation(TSqlFragment fragment)
        {
            return Create(13625, fragment);
        }

        internal static Sql4CdsError JsonNotArrayOrObject(TSqlFragment fragment)
        {
            return Create(13611, fragment);
        }

        internal static Sql4CdsError JsonPathFormatError(char c, int index)
        {
            return Create(13607, null, Collation.USEnglish.ToSqlString(c.ToString()), (SqlInt32)index);
        }

        internal static Sql4CdsError XQueryMissingVariable(string name)
        {
            return Create(9501, null, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidParameter(TSqlFragment fragment, int paramNum, string function)
        {
            return Create(1023, fragment, (SqlInt32)paramNum, Collation.USEnglish.ToSqlString(function));
        }

        internal static Sql4CdsError InvalidOptionValue(TSqlFragment value, string type)
        {
            var valueName = value is ColumnReferenceExpression col ? col.GetColumnName()
                : value is StringLiteral lit ? lit.Value
                : value.ToSql();

            return Create(155, value, (SqlInt32)valueName.Length, Collation.USEnglish.ToSqlString(valueName), Collation.USEnglish.ToSqlString(type));
        }

        internal static Sql4CdsError InvalidFunction(Identifier identifier)
        {
            return Create(195, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value), Collation.USEnglish.ToSqlString("built-in function name"));
        }

        internal static Sql4CdsError InvalidFunctionParameterCount(Identifier identifier, int expectedArgs)
        {
            return Create(174, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value), (SqlInt32)expectedArgs);
        }

        internal static Sql4CdsError XmlDataTypeMethodRequiresStringLiteralParameter(TSqlFragment fragment, Identifier identifier, int paramNum)
        {
            return Create(8172, fragment, (SqlInt32)paramNum, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value));
        }

        internal static Sql4CdsError InvalidDataTypeForXmlValueMethod(StringLiteral type)
        {
            return Create(9500, type, (SqlInt32)type.Value.Length, Collation.USEnglish.ToSqlString(type.Value));
        }

        internal static Sql4CdsError InvalidWithinGroupOrdering(TSqlFragment fragment)
        {
            return Create(8711, fragment);
        }

        internal static Sql4CdsError InvalidWithinGroupClause(Identifier identifier)
        {
            return Create(10757, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value));
        }

        internal static Sql4CdsError InvalidOffsetClause(TSqlFragment fragment)
        {
            return Create(10743, fragment);
        }

        internal static Sql4CdsError InvalidTopOrFetchClause(TSqlFragment fragment)
        {
            return Create(1060, fragment);
        }

        internal static Sql4CdsError TopNWithTiesRequiresOrderBy(TopRowFilter fragment)
        {
            return Create(1062, fragment);
        }

        internal static Sql4CdsError TableValueConstructorRequiresConsistentColumns(TSqlFragment fragment)
        {
            return Create(10709, fragment);
        }

        internal static Sql4CdsError TableValueConstructorTooManyColumns(Identifier identifier)
        {
            return Create(8158, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value));
        }

        internal static Sql4CdsError TableValueConstructorTooFewColumns(Identifier identifier)
        {
            return Create(8159, identifier, (SqlInt32)identifier.Value.Length, Collation.USEnglish.ToSqlString(identifier.Value));
        }

        internal static Sql4CdsError InvalidDataTypeForSubstitutionParameter(int paramNum)
        {
            return Create(2786, null, (SqlInt32)paramNum);
        }

        internal static Sql4CdsError DuplicateInsertUpdateColumn(ColumnReferenceExpression col)
        {
            var colName = col.MultiPartIdentifier.Identifiers.Last().Value;

            return Create(264, col, (SqlInt32)colName.Length, Collation.USEnglish.ToSqlString(colName));
        }

        internal static Sql4CdsError AmbiguousTable(NamedTableReference table)
        {
            var tableName = table.Alias?.Value ?? table.SchemaObject.BaseIdentifier.Value;

            return Create(8154, table, (SqlInt32)tableName.Length, Collation.USEnglish.ToSqlString(tableName));
        }

        internal static Sql4CdsError InsertTooManyColumns(InsertStatement insert)
        {
            return Create(insert.InsertSpecification.InsertSource is ValuesInsertSource ? 109 : 120, insert);
        }

        internal static Sql4CdsError InsertTooFewColumns(InsertStatement insert)
        {
            return Create(insert.InsertSpecification.InsertSource is ValuesInsertSource ? 110 : 121, insert);
        }

        internal static Sql4CdsError ForBrowseNotSupported(BrowseForClause forBrowse)
        {
            return Create(176, forBrowse);
        }

        internal static Sql4CdsError CteNotAllowedDistinct(QuerySpecification query, string cteName)
        {
            return Create(460, query, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteNotAllowedGroupByInRecursivePart(TSqlFragment fragment, string cteName)
        {
            return Create(467, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteNotAllowedTopOffsetInRecursivePart(TSqlFragment fragment, string cteName)
        {
            return Create(461, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteNotAllowedOuterJoinInRecursivePart(TSqlFragment fragment, string cteName)
        {
            return Create(462, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteNotAllowedRecursiveReferenceInSubquery(TSqlFragment fragment)
        {
            return Create(465, fragment);
        }

        internal static Sql4CdsError CteRecursiveMemberWithoutUnionAll(TSqlFragment fragment, string cteName)
        {
            return Create(252, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteMultipleRecursiveMembers(TSqlFragment fragment, string cteName)
        {
            return Create(253, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteNoAnchorMember(TSqlFragment fragment, string cteName)
        {
            return Create(246, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteUnnamedColumn(Identifier cteName, int colNumber)
        {
            return Create(8155, cteName, (SqlInt32)colNumber, (SqlInt32)cteName.Value.Length, Collation.USEnglish.ToSqlString(cteName.Value));
        }

        internal static Sql4CdsError CteNotAllowedHintsInRecursivePart(TSqlFragment fragment, string cteName)
        {
            return Create(4150, fragment, (SqlInt32)cteName.Length, Collation.USEnglish.ToSqlString(cteName));
        }

        internal static Sql4CdsError CteDuplicateName(Identifier cteName)
        {
            return Create(239, cteName, (SqlInt32)cteName.Value.Length, Collation.USEnglish.ToSqlString(cteName.Value));
        }

        internal static Sql4CdsError OrderByWithoutTop(OrderByClause orderBy)
        {
            return Create(1033, orderBy);
        }

        internal static Sql4CdsError InvalidConversionStyle(ConvertCall convert, int style, DataTypeReference type)
        {
            return Create(281, convert, (SqlInt32)style, Collation.USEnglish.ToSqlString(GetTypeName(type)));
        }

        internal static Sql4CdsError DuplicateGotoLabel(LabelStatement label)
        {
            return Create(133, label, (SqlInt32)label.Value.Length, Collation.USEnglish.ToSqlString(label.Value));
        }

        internal static Sql4CdsError UnknownGotoLabel(GoToStatement @goto)
        {
            return Create(133, @goto, (SqlInt32)@goto.LabelName.Value.Length, Collation.USEnglish.ToSqlString(@goto.LabelName.Value));
        }

        internal static Sql4CdsError GotoIntoTryOrCatch(GoToStatement @goto)
        {
            return Create(1026, @goto);
        }

        internal static Sql4CdsError ThrowOutsideCatch(ThrowStatement @throw)
        {
            return Create(10704, @throw);
        }

        internal static Sql4CdsError InvalidColumnForFullTextSearch(ColumnReferenceExpression col)
        {
            var colName = col.GetColumnName();
            return Create(7670, col, (SqlInt32)colName.Length, Collation.USEnglish.ToSqlString(colName));
        }

        internal static Sql4CdsError InvalidErrorNumber(int number)
        {
            return Create(2732, null, (SqlInt32)number);
        }

        internal static Sql4CdsError InvalidSeverityLevel(int maxSeverity)
        {
            return Create(2754, null, (SqlInt32)maxSeverity);
        }

        internal static Sql4CdsError InvalidWaitForTimeSyntax(string time)
        {
            return Create(148, null, (SqlInt32)time.Length, Collation.USEnglish.ToSqlString(time));
        }

        internal static Sql4CdsError InvalidWaitForType(DataTypeReference type)
        {
            return Create(9815, null, Collation.USEnglish.ToSqlString(GetTypeName(type)));
        }

        internal static Sql4CdsError ExceededMaxRecursion(LiteralOptimizerHint hint, int max, int value)
        {
            return Create(310, hint, (SqlInt32)value, (SqlInt32)max);
        }

        internal static Sql4CdsError ExceedeMaxRaiseErrorParameters(ScalarExpression param, int max)
        {
            return Create(2747, param, (SqlInt32)max);
        }

        internal static Sql4CdsError InvalidRaiseErrorParameterType(ScalarExpression param, DataTypeReference type, int paramNum)
        {
            return Create(2748, param, Collation.USEnglish.ToSqlString(GetTypeName(type)), (SqlInt32)paramNum);
        }

        internal static Sql4CdsError SysAdminRequired(TSqlFragment fragment, string option, string command)
        {
            return Create(2778, fragment, Collation.USEnglish.ToSqlString(option), Collation.USEnglish.ToSqlString(command));
        }

        internal static Sql4CdsError InvalidFormatSpecification(string spec)
        {
            return Create(2787, null, (SqlInt32)spec.Length, Collation.USEnglish.ToSqlString(spec));
        }

        internal static Sql4CdsError DuplicateVariable(DeclareVariableElement declaration)
        {
            return Create(134, declaration, (SqlInt32)declaration.VariableName.Value.Length, Collation.USEnglish.ToSqlString(declaration.VariableName.Value));
        }

        internal static Sql4CdsError UnsupportedStatement(TSqlFragment statement, string name)
        {
            return Create(40517, statement, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidSelectVariableAssignment(SelectElement element)
        {
            return Create(141, element);
        }

        internal static Sql4CdsError SetOperationWithDifferentColumnCounts(BinaryQueryExpression query)
        {
            return Create(205, query);
        }

        internal static Sql4CdsError InvalidAggregateInWhereClause(FunctionCall aggregate)
        {
            return Create(147, aggregate);
        }

        internal static Sql4CdsError InvalidTopWithOffset(TopRowFilter top)
        {
            return Create(10741, top);
        }

        internal static Sql4CdsError MultiColumnScalarSubquery(ScalarSubquery subquery)
        {
            return Create(116, subquery);
        }

        internal static Sql4CdsError InvalidStringAggSeparator(TSqlFragment separator)
        {
            return Create(8733, separator);
        }

        internal static Sql4CdsError InvalidOrderByColumnNumber(ExpressionWithSortOrder sort)
        {
            return Create(108, sort, (SqlInt32)int.Parse(((IntegerLiteral)sort.Expression).Value));
        }

        internal static Sql4CdsError InvalidColumnPrefix(SelectStarExpression select)
        {
            var prefix = String.Join(".", select.Qualifier.Identifiers.Select(id => id.Value));
            return Create(107, select, (SqlInt32)prefix.Length, Collation.USEnglish.ToSqlString(prefix));
        }

        internal static Sql4CdsError NotSupported(TSqlFragment fragment)
        {
            return Create(40133, fragment);
        }

        internal static Sql4CdsError InvalidFullTextPattern(string pattern)
        {
            return Create(19306, null, (SqlInt32)pattern.Length, Collation.USEnglish.ToSqlString(pattern));
        }

        internal static Sql4CdsError ConflictingHints(StringLiteral hint, string name)
        {
            return Create(1042, hint, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidOutputConstant(ExecuteParameter param, SchemaObjectName sproc)
        {
            var err = Create(179, param);
            err.Procedure = sproc.BaseIdentifier.Value;
            return err;
        }

        internal static Sql4CdsError DivideByZero()
        {
            return Create(8134, null);
        }

        internal static Sql4CdsError InvalidAggregateOrSubqueryInGroupByClause(TSqlFragment fragment)
        {
            return Create(144, fragment);
        }

        internal static Sql4CdsError SubqueriesNotAllowed(TSqlFragment fragment)
        {
            return Create(1046, fragment);
        }

        internal static Sql4CdsError ConstantExpressionsOnly(ColumnReferenceExpression col)
        {
            var name = col.GetColumnName();
            return Create(128, col, (SqlInt32)name.Length, Collation.USEnglish.ToSqlString(name));
        }

        internal static Sql4CdsError InvalidTypeForStatement(TSqlFragment fragment, string name)
        {
            return Create(15533, fragment, name);
        }

        internal static Sql4CdsError IncompatibleDataTypesForOperator(TSqlFragment fragment, DataTypeReference type1, DataTypeReference type2, string op)
        {
            return Create(402, fragment, Collation.USEnglish.ToSqlString(GetTypeName(type1)), Collation.USEnglish.ToSqlString(GetTypeName(type2)), Collation.USEnglish.ToSqlString(op));
        }

        internal static Sql4CdsError StringSplitOrdinalRequiresLiteral(TSqlFragment fragment)
        {
            return Create(8748, fragment);
        }

        internal static Sql4CdsError InvalidProcedureParameterType(TSqlFragment fragment, string parameter, string type)
        {
            return Create(214, fragment, parameter, type);
        }

        private static string GetTypeName(DataTypeReference type)
        {
            if (type is SqlDataTypeReference sqlType)
                return sqlType.SqlDataTypeOption.ToString().ToLowerInvariant();

            if (type is XmlDataTypeReference)
                return "xml";
            
            return ((UserDataTypeReference)type).Name.ToSql();
        }

        /// <summary>
        /// Creates a copy of this error for a specific fragment
        /// </summary>
        /// <param name="expression">The fragment the error should be applied to</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        internal Sql4CdsError ForFragment(BooleanComparisonExpression expression)
        {
            if (expression == null)
                return this;

            return new Sql4CdsError(Class, -1, Number, Procedure, Server, State, Message, expression);
        }
    }

    /// <summary>
    /// Defines an exception that exposes a <see cref="Sql4CdsError"/>
    /// </summary>
    interface ISql4CdsErrorException
    {
        /// <summary>
        /// The <see cref="Sql4CdsError"/>s to report back to the user
        /// </summary>
        IReadOnlyList<Sql4CdsError> Errors { get; }
    }
}
