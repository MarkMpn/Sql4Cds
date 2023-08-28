using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualBasic;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.XPath;
using Wmhelp.XPath2;
using Wmhelp.XPath2.Value;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Implements functions that can be called from SQL expressions
    /// </summary>
    class ExpressionFunctions
    {
        /// <summary>
        /// Creates a SqlEntityReference value
        /// </summary>
        /// <param name="logicalName">The logical name of the entity type being referred to</param>
        /// <param name="id">The unique identifier of the record</param>
        /// <returns>A <see cref="SqlEntityReference"/> value combining the <paramref name="logicalName"/> and <paramref name="id"/></returns>
        public static SqlEntityReference CreateLookup(SqlString logicalName, SqlGuid id)
        {
            if (logicalName.IsNull || id.IsNull)
                return SqlEntityReference.Null;

            return new SqlEntityReference(null, logicalName.Value, id);
        }

        /// <summary>
        /// Extracts a scalar value from a JSON string
        /// </summary>
        /// <param name="json">An expression containing the JSON document to parse</param>
        /// <param name="jpath">A JSON path that specifies the property to extract</param>
        /// <returns>Returns a single text value of type nvarchar(4000)</returns>
        [MaxLength(4000)]
        public static SqlString Json_Value(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlString.Null;

            var path = jpath.Value;
            var lax = !path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase);

            if (path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(7);
            else if (path.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(4);

            try
            {
                var jsonDoc = JToken.Parse(json.Value);
                var jtoken = jsonDoc.SelectToken(path);

                if (jtoken == null)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Property does not exist");
                }

                if (jtoken.Type == JTokenType.Object || jtoken.Type == JTokenType.Array)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Not a scalar value");
                }

                var value = jtoken.Value<string>();

                if (value == null)
                    return SqlString.Null;

                if (value.Length > 4000)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Value too long");
                }

                return new SqlString(value, json.LCID, json.SqlCompareOptions);
            }
            catch (Newtonsoft.Json.JsonException ex)
            {
                throw new QueryExecutionException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Tests whether a specified SQL/JSON path exists in the input JSON string.
        /// </summary>
        /// <param name="json">An expression containing the JSON document to parse</param>
        /// <param name="jpath">A JSON path that specifies the property to extract</param>
        /// <returns>A value indicating if the path exists in the JSON document</returns>
        public static SqlBoolean Json_Path_Exists(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlBoolean.Null;

            var path = jpath.Value;

            if (path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(7);
            else if (path.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(4);

            try
            {

                var jsonDoc = JToken.Parse(json.Value);
                var jtoken = jsonDoc.SelectToken(path);

                return jtoken != null;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Implements the DATEADD function
        /// </summary>
        /// <param name="datepart">The part of <paramref name="date"/> to which DATEADD adds an integer <paramref name="number"/></param>
        /// <param name="number">The value to add to the <paramref name="datepart"/> of the <paramref name="date"/></param>
        /// <param name="date">The date value to add to</param>
        /// <returns>The modified date</returns>
        /// <see href="https://docs.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver15"/>
        public static SqlDateTime DateAdd(SqlString datepart, SqlDouble number, SqlDateTime date)
        {
            if (number.IsNull || date.IsNull)
                return SqlDateTime.Null;

            var interval = DatePartToInterval(datepart.Value);
            var value = DateAndTime.DateAdd(interval, number.Value, date.Value);

            // DateAdd loses the Kind property for some interval types - add it back in again
            if (value.Kind == DateTimeKind.Unspecified)
                value = DateTime.SpecifyKind(value, date.Value.Kind);

            return value;
        }

        /// <summary>
        /// Implements the DATEDIFF function
        /// </summary>
        /// <param name="datepart">The units in which DATEDIFF reports the difference between <paramref name="startdate"/> and <paramref name="enddate"/></param>
        /// <param name="startdate">The first date to compare from</param>
        /// <param name="enddate">The second date to compare to</param>
        /// <returns>The number of whole <paramref name="datepart"/> units between <paramref name="startdate"/> and <paramref name="enddate"/></returns>
        /// <see href="https://docs.microsoft.com/en-us/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver15"/>
        public static SqlInt32 DateDiff(SqlString datepart, SqlDateTime startdate, SqlDateTime enddate)
        {
            if (startdate.IsNull || enddate.IsNull)
                return SqlInt32.Null;

            var interval = DatePartToInterval(datepart.Value);
            return (int) DateAndTime.DateDiff(interval, startdate.Value, enddate.Value);
        }

        /// <summary>
        /// Implements the DATEPART function
        /// </summary>
        /// <param name="datepart">The specific part of the <paramref name="date"/> argument for which DATEPART will return an integer</param>
        /// <param name="date">The date to extract the <paramref name="datepart"/> from</param>
        /// <returns>The <paramref name="datepart"/> of the <paramref name="date"/></returns>
        public static SqlInt32 DatePart(SqlString datepart, SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            var interval = DatePartToInterval(datepart.Value);
            return DateAndTime.DatePart(interval, date.Value);
        }

        /// <summary>
        /// Converts the SQL datepart argument names to the equivalent enum values used by VisualBasic
        /// </summary>
        /// <param name="datepart">The SQL name for the datepart argument</param>
        /// <returns>The equivalent <see cref="DateInterval"/> value</returns>
        internal static DateInterval DatePartToInterval(string datepart)
        {
            switch (datepart.ToLower())
            {
                case "year":
                case "yy":
                case "yyyy":
                    return DateInterval.Year;

                case "quarter":
                case "qq":
                case "q":
                    return DateInterval.Quarter;

                case "month":
                case "mm":
                case "m":
                    return DateInterval.Month;

                case "dayofyear":
                case "dy":
                case "y":
                    return DateInterval.DayOfYear;

                case "day":
                case "dd":
                case "d":
                    return DateInterval.Day;

                case "weekday":
                case "dw":
                case "w":
                    return DateInterval.Weekday;

                case "week":
                case "wk":
                case "ww":
                    return DateInterval.WeekOfYear;

                case "hour":
                case "hh":
                    return DateInterval.Hour;

                case "minute":
                case "mi":
                case "n":
                    return DateInterval.Minute;

                case "second":
                case "ss":
                case "s":
                    return DateInterval.Second;

                default:
                    throw new ArgumentOutOfRangeException(nameof(datepart), $"Unsupported DATEPART value {datepart}");
            }
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime GetDate()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime SysDateTime()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime SysDateTimeOffset()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static SqlDateTime GetUtcDate()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static SqlDateTime SysUtcDateTime()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the day of the month from the specified date
        /// </summary>
        /// <param name="date">The date to get the day number from</param>
        /// <returns>The day of the month</returns>
        public static SqlInt32 Day(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Day;
        }

        /// <summary>
        /// Gets the month number from the specified date
        /// </summary>
        /// <param name="date">The date to get the month number from</param>
        /// <returns>The month number</returns>
        public static SqlInt32 Month(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Month;
        }

        /// <summary>
        /// Gets the year from the specified date
        /// </summary>
        /// <param name="date">The date to get the year number from</param>
        /// <returns>The year number</returns>
        public static SqlInt32 Year(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Year;
        }

        /// <summary>
        /// Returns the prefix of a string
        /// </summary>
        /// <param name="s">The string to get the prefix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The first <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        [CollationSensitive]
        public static SqlString Left(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return new SqlString(s.Value.Substring(0, length.Value), s.LCID, s.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the suffix of a string
        /// </summary>
        /// <param name="s">The string to get the suffix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The last <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        [CollationSensitive]
        public static SqlString Right(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return new SqlString(s.Value.Substring(s.Value.Length - length.Value, length.Value), s.LCID, s.SqlCompareOptions);
        }

        /// <summary>
        /// Replaces all occurrences of a specified string value with another string value.
        /// </summary>
        /// <param name="input">The string expression to be searched</param>
        /// <param name="find">The substring to be found</param>
        /// <param name="replace">The replacement string</param>
        /// <returns>Replaces any instances of <paramref name="find"/> with <paramref name="replace"/> in the <paramref name="input"/></returns>
        [CollationSensitive]
        public static SqlString Replace(SqlString input, SqlString find, SqlString replace)
        {
            if (input.IsNull || find.IsNull || replace.IsNull)
                return SqlString.Null;

            return new SqlString(Regex.Replace(input.Value, Regex.Escape(find.Value), replace.Value.Replace("$", "$$"), RegexOptions.IgnoreCase), input.LCID, input.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the number of characters of the specified string expression, excluding trailing spaces
        /// </summary>
        /// <param name="s">The string expression to be evaluated</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlInt32 Len(SqlString s)
        {
            if (s.IsNull)
                return SqlInt32.Null;

            return s.Value.TrimEnd().Length;
        }

        /// <summary>
        /// Returns the number of bytes used to represent any expression
        /// </summary>
        /// <param name="value">Any expression</param>
        /// <returns></returns>
        public static SqlInt32 DataLength<T>(T value, [SourceType] DataTypeReference type)
            where T:INullable
        {
            if (value.IsNull)
                return SqlInt32.Null;

            var sqlType = type as SqlDataTypeReference;

            if (sqlType != null && value is SqlString s && 
                (sqlType.SqlDataTypeOption == SqlDataTypeOption.VarChar || sqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar))
            {
                var length = s.Value.Length;

                if (sqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar)
                    length *= 2;

                return length;
            }

            var size = type.GetSize();

            if (sqlType != null && sqlType.SqlDataTypeOption == SqlDataTypeOption.NChar)
                size *= 2;

            return size;
        }

        /// <summary>
        /// Returns part of a character expression
        /// </summary>
        /// <param name="expression">A character expression</param>
        /// <param name="start">An integer that specifies where the returned characters start (the numbering is 1 based, meaning that the first character in the expression is 1)</param>
        /// <param name="length">A positive integer that specifies how many characters of the expression will be returned</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlString Substring(SqlString expression, SqlInt32 start, [MaxLength] SqlInt32 length)
        {
            if (expression.IsNull || start.IsNull || length.IsNull)
                return SqlString.Null;

            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (start < 1)
                start = 1;

            if (start > expression.Value.Length)
                return new SqlString(String.Empty, expression.LCID, expression.SqlCompareOptions);

            start -= 1;

            if (start + length > expression.Value.Length)
                length = expression.Value.Length - start;

            return new SqlString(expression.Value.Substring(start.Value, length.Value), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the start and end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString Trim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.Trim(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the start of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString LTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.TrimStart(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString RTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.TrimEnd(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Searches for one character expression inside a second character expression, returning the starting position of the first expression if found
        /// </summary>
        /// <param name="find">A character expression containing the sequence to find</param>
        /// <param name="search">A character expression to search.</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlInt32 CharIndex(SqlString find, SqlString search)
        {
            return CharIndex(find, search, 0);
        }

        /// <summary>
        /// Searches for one character expression inside a second character expression, returning the starting position of the first expression if found
        /// </summary>
        /// <param name="find">A character expression containing the sequence to find</param>
        /// <param name="search">A character expression to search.</param>
        /// <param name="startLocation">An integer or bigint expression at which the search starts. If start_location is not specified, has a negative value, or has a zero (0) value, the search starts at the beginning of expressionToSearch.</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlInt32 CharIndex(SqlString find, SqlString search, SqlInt32 startLocation)
        {
            if (find.IsNull || search.IsNull || startLocation.IsNull)
                return SqlInt32.Null;

            if (startLocation <= 0)
                startLocation = 1;

            if (startLocation > search.Value.Length)
                return 0;

            return search.Value.IndexOf(find.Value, startLocation.Value - 1, StringComparison.OrdinalIgnoreCase) + 1;
        }

        /// <summary>
        /// Returns the starting position of the first occurrence of a pattern in a specified expression, or zero if the pattern is not found, on all valid text and character data types.
        /// </summary>
        /// <param name="pattern">A character expression that contains the sequence to be found. Wildcard characters can be used; however, the % character must come before and follow <paramref name="pattern"/></param>
        /// <param name="expression">An expression that is searched for the specified <paramref name="pattern"/></param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlInt32 PatIndex(SqlString pattern, SqlString expression)
        {
            if (pattern.IsNull || expression.IsNull)
                return 0;

            var regex = ExpressionExtensions.LikeToRegex(pattern, SqlString.Null, true);
            var value = expression.Value;

            if (expression.SqlCompareOptions.HasFlag(SqlCompareOptions.IgnoreNonSpace))
                value = ExpressionExtensions.RemoveDiacritics(value);

            var match = regex.Match(value);

            if (!match.Success)
                return 0;

            return match.Index + 1;
        }

        /// <summary>
        /// Returns the single-byte character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        public static SqlString Char(SqlInt32 value, ExpressionExecutionContext context)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 255)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the Unicode character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        public static SqlString NChar(SqlInt32 value, ExpressionExecutionContext context)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 0x10FFFF)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the ASCII code value of the leftmost character of a character expression.
        /// </summary>
        /// <param name="value">A string to convert</param>
        /// <returns></returns>
        public static SqlInt32 Ascii(SqlString value)
        {
            if (value.IsNull || value.Value.Length == 0)
                return SqlInt32.Null;

            var b = Encoding.ASCII.GetBytes(value.Value);
            return b[0];
        }

        /// <summary>
        /// Returns the integer value, as defined by the Unicode standard, for the first character of the input expression.
        /// </summary>
        /// <param name="value">A string to convert</param>
        /// <returns></returns>
        public static SqlInt32 Unicode(SqlString value)
        {
            if (value.IsNull || value.Value.Length == 0)
                return SqlInt32.Null;

            return value.Value[0];
        }

        /// <summary>
        /// Returns the identifier of the user
        /// </summary>
        /// <param name="context">The context in which the expression is being executed</param>
        /// <returns></returns>
        public static SqlEntityReference User_Name(ExpressionExecutionContext context)
        {
            return new SqlEntityReference(context.Options.PrimaryDataSource, "systemuser", context.Options.UserId);
        }

        /// <summary>
        /// The value of <paramref name="check"/> is returned if it is not NULL; otherwise, <paramref name="replacement"/> is returned
        /// </summary>
        /// <typeparam name="T">The type of values being compared</typeparam>
        /// <param name="check">The expression to be checked for NULL</param>
        /// <param name="replacement">The value to be returned if <paramref name="check"/> is NULL</param>
        /// <returns></returns>
        public static T IsNull<T>(T check, T replacement)
            where T:INullable
        {
            if (!check.IsNull)
                return check;

            return replacement;
        }

        /// <summary>
        /// Returns a value formatted with the specified format and optional culture
        /// </summary>
        /// <typeparam name="T">The type of value to be formatted</typeparam>
        /// <param name="value">The value to be formatted</param>
        /// <param name="format">Format pattern</param>
        /// <param name="culture">Optional argument specifying a culture</param>
        /// <returns></returns>
        public static SqlString Format<T>(T value, SqlString format, [Optional] SqlString culture, ExpressionExecutionContext context)
            where T : INullable
        {
            if (value.IsNull)
                return SqlString.Null;

            var valueProp = typeof(T).GetProperty("Value");

            if (!typeof(IFormattable).IsAssignableFrom(valueProp.PropertyType))
                throw new QueryExecutionException("Invalid type for FORMAT function");

            var innerValue = (IFormattable)valueProp.GetValue(value);
            return Format(innerValue, format, culture, context);
        }

        private static SqlString Format(IFormattable value, SqlString format, SqlString culture, ExpressionExecutionContext context)
        {
            if (value == null)
                return SqlString.Null;

            if (format.IsNull)
                return SqlString.Null;

            var cultureInfo = CultureInfo.CurrentCulture;

            try
            {
                if (!culture.IsNull)
                    cultureInfo = CultureInfo.GetCultureInfo(culture.Value);

                var formatted = value.ToString(format.Value, cultureInfo);
                return context.PrimaryDataSource.DefaultCollation.ToSqlString(formatted);
            }
            catch
            {
                return SqlString.Null;
            }
        }

        /// <summary>
        /// Returns a character expression with lowercase character data converted to uppercase
        /// </summary>
        /// <param name="value">An expression of character data</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlString Upper([MaxLength] SqlString value)
        {
            if (value.IsNull)
                return value;

            return new SqlString(value.Value.ToUpper(value.CultureInfo), value.LCID, value.SqlCompareOptions);
        }

        /// <summary>
        /// Returns a character expression with uppercase character data converted to lowercase
        /// </summary>
        /// <param name="value">An expression of character data</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlString Lower([MaxLength] SqlString value)
        {
            if (value.IsNull)
                return value;

            return new SqlString(value.Value.ToLower(value.CultureInfo), value.LCID, value.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the requested property of a specified collation
        /// </summary>
        /// <param name="collation">The name of the collation</param>
        /// <param name="property">The collation property</param>
        /// <returns></returns>
        public static SqlInt32 CollationProperty(SqlString collation, SqlString property)
        {
            if (collation.IsNull || property.IsNull)
                return SqlInt32.Null;

            if (!Collation.TryParse(collation.Value, out var coll))
                return SqlInt32.Null;

            switch (property.Value.ToLowerInvariant())
            {
                case "codepage":
                    return 0;

                case "lcid":
                    return coll.LCID;

                case "comparisonstyle":
                    var compare = 0;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreCase))
                        compare |= 1;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreNonSpace))
                        compare |= 2;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreKanaType))
                        compare |= 65536;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreWidth))
                        compare |= 131072;

                    return compare;

                case "version":
                    if (coll.Name.Contains("140"))
                        return 3;

                    if (coll.Name.Contains("100"))
                        return 2;

                    if (coll.Name.Contains("90"))
                        return 1;

                    return 0;
            }

            return SqlInt32.Null;
        }

        /// <summary>
        /// Specifies an XQuery against an instance of the xml data type.
        /// </summary>
        /// <param name="value">The xml data to query</param>
        /// <param name="query">The XQuery expression to apply</param>
        /// <param name="context">The context the expression is evaluated in</param>
        /// <param name="schema">The schema of data available to the query</param>
        /// <returns></returns>
        public static SqlXml Query(SqlXml value, XPath2Expression query, ExpressionExecutionContext context, INodeSchema schema)
        {
            if (value.IsNull)
                return value;

            var doc = new XPathDocument(value.CreateReader());
            var nav = doc.CreateNavigator();
            var result = query.Evaluate(new XPath2ExpressionContext(context, schema, nav), null);
            var stream = new MemoryStream();

            var xmlWriterSettings = new XmlWriterSettings
            {
                CloseOutput = false,
                ConformanceLevel = ConformanceLevel.Fragment,
                Encoding = Encoding.GetEncoding("utf-16"),
                OmitXmlDeclaration = true
            };
            var xmlWriter = XmlWriter.Create(stream, xmlWriterSettings);

            foreach (XPathNavigator r in (XPath2NodeIterator)result)
            {
                var reader = r.ReadSubtree();

                if (reader.ReadState == ReadState.Initial)
                    reader.Read();

                while (!reader.EOF)
                    xmlWriter.WriteNode(reader, defattr: true);
            }

            xmlWriter.Flush();
            stream.Position = 0;
            return new SqlXml(stream);
        }

        /// <summary>
        /// Performs an XQuery against the XML and returns a value of SQL type. This method returns a scalar value.
        /// </summary>
        /// <param name="value">The xml data to query</param>
        /// <param name="query">The XQuery expression to apply</param>
        /// <param name="targetType">The type of data to return</param>
        /// <param name="context">The context the expression is evaluated in</param>
        /// <param name="schema">The schema of data available to the query</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static object Value(SqlXml value, XPath2Expression query, [TargetType] DataTypeReference targetType, ExpressionExecutionContext context, INodeSchema schema)
        {
            if (value.IsNull)
                return value;

            var doc = new XPathDocument(value.CreateReader());
            var nav = doc.CreateNavigator();
            var result = query.Evaluate(new XPath2ExpressionContext(context, schema, nav), null);

            var targetNetType = targetType.ToNetType(out _);

            INullable sqlValue;

            if (result == null)
                sqlValue = SqlTypeConverter.GetNullValue(targetNetType);
            else if (result is Base64BinaryValue bin)
                sqlValue = new SqlBinary(bin.BinaryValue);
            else
                throw new NotSupportedException("Unhandled return type " + result.GetType().FullName);

            if (sqlValue.GetType() != targetNetType)
                sqlValue = (INullable) SqlTypeConverter.ChangeType(sqlValue, targetNetType);

            return sqlValue;
        }

        /// <summary>
        /// Deletes a specified length of characters in the first string at the start position and then inserts the second string into the first string at the start position
        /// </summary>
        /// <param name="value">The first string to manipulate</param>
        /// <param name="start">The starting position within the first string to make the edits at</param>
        /// <param name="length">The number of characters to remove from the first string</param>
        /// <param name="replaceWith">The second string to insert into the first string</param>
        /// <returns></returns>
        [CollationSensitive]
        public static SqlString Stuff(SqlString value, SqlInt32 start, SqlInt32 length, SqlString replaceWith)
        {
            if (value.IsNull || start.IsNull || length.IsNull || start.Value <= 0 || start.Value > value.Value.Length || length.Value < 0)
                return SqlString.Null;

            var sb = new StringBuilder(value.Value);

            if (length.Value > 0)
                sb.Remove(start.Value - 1, Math.Min(length.Value, value.Value.Length - start.Value + 1));

            if (!replaceWith.IsNull)
                sb.Insert(start.Value - 1, replaceWith.Value);

            return new SqlString(sb.ToString(), value.LCID, value.SqlCompareOptions);
        }

        public static SqlVariant ServerProperty(SqlString propertyName, ExpressionExecutionContext context)
        {
            if (propertyName.IsNull)
                return SqlVariant.Null;

            var dataSource = context.PrimaryDataSource;

#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;
#else
            var svc = dataSource.Connection as CrmServiceClient;
#endif

            switch (propertyName.Value.ToLowerInvariant())
            {
                case "collation":
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(dataSource.DefaultCollation.Name));

                case "collationid":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(dataSource.DefaultCollation.LCID));

                case "comparisonstyle":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32((int)dataSource.DefaultCollation.CompareOptions));

                case "edition":
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString("Enterprise Edition"));

                case "editionid":
                    return new SqlVariant(DataTypeHelpers.BigInt, new SqlInt64(1804890536));

                case "enginedition":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(3));

                case "issingleuser":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(0));

                case "machinename":
                case "servername":
                    string machineName = dataSource.Name;

#if NETCOREAPP
                    if (svc != null)
                        machineName = svc.ConnectedOrgUriActual.Host;
#else
                    if (svc != null)
                        machineName = svc.CrmConnectOrgUriActual.Host;
#endif
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(machineName));

                case "pathseparator":
                    return new SqlVariant(DataTypeHelpers.NVarChar(1, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(Path.DirectorySeparatorChar.ToString()));

                case "processid":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(System.Diagnostics.Process.GetCurrentProcess().Id));

                case "productversion":
                    string orgVersion = null;

#if NETCOREAPP
                    if (svc != null)
                        orgVersion = svc.ConnectedOrgVersion.ToString();
#else
                    if (svc != null)
                        orgVersion = svc.ConnectedOrgVersion.ToString();
#endif

                    if (orgVersion == null)
                        orgVersion = ((RetrieveVersionResponse)dataSource.Execute(new RetrieveVersionRequest())).Version;

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(orgVersion));
            }

            return SqlVariant.Null;
        }

        public static SqlVariant Sql_Variant_Property(SqlVariant expression, SqlString property)
        {
            if (property.IsNull)
                return SqlVariant.Null;

            switch (property.Value.ToLowerInvariant())
            {
                case "basetype":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), SqlString.Null);

                    if (expression.BaseType is SqlDataTypeReference sqlType)
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(sqlType.SqlDataTypeOption.ToString().ToLowerInvariant()));

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(expression.BaseType.ToSql()));

                case "precision":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetPrecision()));

                case "scale":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetScale()));

                case "totalbytes":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null);

                    return new SqlVariant(DataTypeHelpers.Int, DataLength<INullable>(expression.Value, expression.BaseType));

                case "collation":
                    if (!(expression.BaseType is SqlDataTypeReferenceWithCollation coll))
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), SqlString.Null);

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(coll.Collation.Name));

                case "maxlength":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetSize()));
            }

            return SqlVariant.Null;
        }
    }

    /// <summary>
    /// Helper methods for generating function call expressions
    /// </summary>
    class Expr
    {
        /// <summary>
        /// Create a method call expression
        /// </summary>
        /// <typeparam name="T">The type of value returned by the method</typeparam>
        /// <param name="expression">The expression containing the method call to</param>
        /// <param name="args">The expressions to supply as arguments to the method call</param>
        /// <returns>The method call expression</returns>
        /// <see href="http://blog.functionalfun.net/2009/10/getting-methodinfo-of-generic-method.html"/>
        public static MethodCallExpression Call<T>(Expression<Func<T>> expression, params Expression[] args)
        {
            var method = GetMethodInfo((LambdaExpression)expression);
            return Call(method, args);
        }

        /// <summary>
        /// Creates a method call expression
        /// </summary>
        /// <param name="method">The details of the method to call</param>
        /// <param name="args">The expressions to supply as arguments to the method call</param>
        /// <returns>The method call expression</returns>
        public static MethodCallExpression Call(MethodInfo method, params Expression[] args)
        {
            var parameters = method.GetParameters();
            var converted = new Expression[parameters.Length];

            for (var i = 0; i < parameters.Length; i++)
            {
                if (parameters[i].ParameterType == args[i].Type)
                {
                    converted[i] = args[i];
                }
                else
                {
                    if (i == parameters.Length - 1 && parameters[i].ParameterType.IsArray && !args[i].Type.IsArray)
                    {
                        var elementType = parameters[i].ParameterType.GetElementType();
                        var elements = new List<Expression>();

                        for (var j = i; j < args.Length; j++)
                            elements.Add(Convert(args[j], elementType));

                        converted[i] = Expression.NewArrayInit(elementType, elements.ToArray());
                    }
                    else
                    {
                        converted[i] = Convert(args[i], parameters[i].ParameterType);
                    }
                }
            }

            return Expression.Call(method, converted);
        }

        /// <summary>
        /// Given a lambda expression that calls a method, returns the method info.
        /// </summary>
        /// <param name="expression">The expression.</param>
        public static MethodInfo GetMethodInfo<T>(Expression<Func<T>> expression)
        {
            var method = GetMethodInfo((LambdaExpression)expression);
            return method;
        }

        /// <summary>
        /// Given a lambda expression that calls a method, returns the method info.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <see href="http://blog.functionalfun.net/2009/10/getting-methodinfo-of-generic-method.html"/>
        private static MethodInfo GetMethodInfo(LambdaExpression expression)
        {
            if (!(expression.Body is MethodCallExpression outermostExpression))
                throw new ArgumentException("Invalid Expression. Expression should consist of a Method call only.");

            return outermostExpression.Method;
        }

        /// <summary>
        /// Placeholder for a function argument when using <see cref="Call{T}(Expression{Func{T}}, Expression[])"/>
        /// </summary>
        /// <typeparam name="T">The type of value expected by the function argument</typeparam>
        /// <returns></returns>
        public static T Arg<T>()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Box an expression
        /// </summary>
        /// <param name="expr"></param>
        /// <returns></returns>
        public static Expression Box(Expression expr)
        {
            if (!expr.Type.IsValueType)
                return expr;

            return Expression.Convert(expr, typeof(object));
        }

        /// <summary>
        /// Applies the required conversion process to return a value of an expected type from an expression
        /// </summary>
        /// <param name="expr">The expression that is generating a value</param>
        /// <param name="type">The type of value required from an expression</param>
        /// <returns>An expression that converts the generated value to the required type</returns>
        public static Expression Convert(Expression expr, Type type)
        {
            // Simple case - expression already generates the required type
            if (expr.Type == type)
                return expr;

            // If the generated type directly implements the required interface
            if (type.IsInterface && expr.Type.GetInterfaces().Contains(type))
                return expr;

            // Box value types
            if (expr.Type.IsValueType && type == typeof(object))
                return Expression.Convert(expr, type);

            // WIP: Implicit type conversions to match SQL
            if (type == typeof(bool?))
            {
                // https://docs.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql
                if (expr.Type == typeof(string))
                    return Expr.Call(() => StringToBool(Arg<string>()), expr);

                if (expr.Type == typeof(int))
                    expr = Expression.Convert(expr, typeof(int?));

                if (expr.Type == typeof(int?))
                    return Expr.Call(() => IntToBool(Arg<int?>()), expr);
            }

            // In-built conversions between value types
            if (expr.Type.IsValueType && type.IsValueType)
                return Expression.Convert(expr, type);

            // Parse string literals to DateTime values
            if (expr.Type == typeof(string) && type == typeof(DateTime?))
                return Expr.Call(() => ParseDateTime(Arg<string>()), expr);

            // Convert integers to optionset values
            if (expr.Type == typeof(int) && type == typeof(OptionSetValue))
                return Expr.Call(() => CreateOptionSetValue(Arg<int>()), expr);
            if (expr.Type == typeof(int?) && type == typeof(OptionSetValue))
                return Expr.Call(() => CreateOptionSetValue(Arg<int?>()), expr);

            // Extract IDs from EntityReferences
            if (expr.Type == typeof(EntityReference) && type == typeof(Guid?))
                return Expr.Call(() => GetEntityReferenceId(Arg<EntityReference>()), expr);

            // Check for compatible class types
            if (expr.Type.IsClass && type.IsClass)
            {
                var baseType = expr.Type.BaseType;

                while (baseType != null)
                {
                    if (baseType == type)
                        return expr;

                    baseType = baseType.BaseType;
                }
            }

            // Check for compatible array types
            if (expr.Type.IsArray && type.IsArray)
            {
                if (type.GetElementType().IsAssignableFrom(expr.Type.GetElementType()))
                    return expr;
            }

            throw new NotSupportedException($"Cannot convert from {expr.Type} to {type}");
        }

        private static Guid? GetEntityReferenceId(EntityReference entityReference)
        {
            return entityReference.Id;
        }

        private static bool? StringToBool(string str)
        {
            if (str == null)
                return null;

            if (str.Equals("TRUE", StringComparison.OrdinalIgnoreCase))
                return true;

            if (str.Equals("FALSE", StringComparison.OrdinalIgnoreCase))
                return false;

            throw new ArgumentOutOfRangeException($"Cannot convert string value '{str}' to boolean");
        }

        private static bool? IntToBool(int? value)
        {
            if (value == null)
                return null;

            if (value == 0)
                return false;

            return true;
        }

        private static DateTime? ParseDateTime(string str)
        {
            if (str == null)
                return null;

            return DateTime.Parse(str);
        }

        private static OptionSetValue CreateOptionSetValue(int? i)
        {
            if (i == null)
                return null;

            return new OptionSetValue(i.Value);
        }
    }

    /// <summary>
    /// Indicates that the parameter gives the maximum length of the return value
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    class MaxLengthAttribute : Attribute
    {
        /// <summary>
        /// Sets the maximum length of the result as the maximum length of this parameter
        /// </summary>
        public MaxLengthAttribute()
        {
        }

        /// <summary>
        /// Sets the maximum length of the result to a fixed value
        /// </summary>
        /// <param name="maxLength"></param>
        public MaxLengthAttribute(int maxLength)
        {
            MaxLength = maxLength;
        }

        public int? MaxLength { get; }
    }

    /// <summary>
    /// Indicates that the parameter gives the type of the returned value
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class TargetTypeAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that the parameter gives the type of the preceding parameter
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class SourceTypeAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that the parameter is optional
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class OptionalAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that a function is collation sensitive
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    class CollationSensitiveAttribute : Attribute
    {
    }
}
