using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.XTB
{
    class FunctionMetadata
    {
        [AttributeUsage(AttributeTargets.Method)]
        public class AggregateAttribute : Attribute
        {
        }

        [AttributeUsage(AttributeTargets.Method)]
        public class ParameterlessCallAttribute : Attribute
        {
        }

        public abstract class SqlFunctions
        {
            [Aggregate]
            [Description("Finds the minimum value of the given expression")]
            public abstract double min(double value);

            [Aggregate]
            [Description("Finds the maximum value of the given expression")]
            public abstract double max(double value);

            [Aggregate]
            [Description("Finds the total value of the given expression")]
            public abstract double sum(double value);

            [Aggregate]
            [Description("Finds the average value of the given expression")]
            public abstract double avg(double value);

            [Aggregate]
            [Description("Finds the number of non-null values")]
            public abstract int count(object value);

            [Aggregate]
            [Description("Concatenates the values of string expressions and places separator values between them")]
            public abstract string string_agg(string value, string separator);

            [Description("Creates a lookup value to reference a record")]
            public abstract EntityReference createlookup(string logicalName, Guid id);

            [Description("Finds the difference between two date values")]
            public abstract int datediff(string datepart, DateTime startdate, DateTime enddate);

            [Description("Adds a number value to a date value")]
            public abstract DateTime dateadd(string datepart, int number, DateTime date);

            [Description("Extracts a scalar value from a JSON string")]
            public abstract string json_value(string json, string path);

            [Description("Tests whether a specified SQL/JSON path exists in the input JSON string")]
            public abstract bool json_path_exists(string json, string path);

            [Description("Replaces one string value with another")]
            public abstract string replace(string input, string find, string replace);

            [Description("Gets the length of the specified string expression")]
            public abstract int len(string value);

            [Description("Gets the number of bytes used to represent any expression")]
            public abstract int datalength(object value);

            [Description("Returns the character with the specified integer code")]
            public abstract string @char(int value);

            [Description("Returns the ASCII code value of the leftmost character of a character expression")]
            public abstract int ascii(string value);

            [Description("Returns the character with the specified integer code")]
            public abstract string nchar(int value);

            [Description("Returns the integer Unicode value of the first character of the input expression")]
            public abstract int unicode(string value);

            [Description("Extracts a portion of the specified string expression")]
            public abstract string substring(string value, int start, int length);

            [Description("Removes any leading or trailing spaces from the specified string expression")]
            public abstract string trim(string value);

            [Description("Removes any leading spaces from the specified string expression")]
            public abstract string ltrim(string value);

            [Description("Removes any trailing spaces from the specified string expression")]
            public abstract string rtrim(string value);

            [Description("Returns the specified number of characters from the start of the string expression")]
            public abstract string left(string value, int length);

            [Description("Returns the specified number of characters from the end of the string expression")]
            public abstract string right(string value, int length);

            [Description("Extracts the year number from a date value")]
            public abstract int year(DateTime date);

            [Description("Extracts the month number from a date value")]
            public abstract int month(DateTime date);

            [Description("Extracts the day number from a date value")]
            public abstract int day(DateTime date);

            [Description("Gets the current date & time")]
            public abstract DateTime getdate();

            [ParameterlessCall]
            [Description("Gets the current date & time")]
            public abstract DateTime current_timestamp();

            [Description("Gets the current date & time")]
            public abstract DateTime sysdatetime();

            [Description("Gets the current date & time")]
            public abstract DateTime sysdatetimeoffset();

            [Description("Gets the current date & time in UTC timezone")]
            public abstract DateTime getutcdate();

            [Description("Gets the current date & time in UTC timezone")]
            public abstract DateTime sysutcdatetime();

            [Description("Extracts the requested part of a date value")]
            public abstract int datepart(string datepart, DateTime date);

            [Description("Returns the unique identifier of the current user")]
            public abstract Guid user_name();

            [ParameterlessCall]
            [Description("Returns the unique identifier of the current user")]
            public abstract Guid current_user();

            [ParameterlessCall]
            [Description("Returns the unique identifier of the current user")]
            public abstract Guid session_user();

            [ParameterlessCall]
            [Description("Returns the unique identifier of the current user")]
            public abstract Guid system_user();

            [Description("Replaces NULL with the specified replacement value")]
            public abstract object isnull(object check, object replacement);

            [Description("Returns a value formatted with the specified format and optional culture")]
            public abstract object format(object value, string format, string culture);

            [Description("Returns the starting position of the first occurrence of a pattern in a specified expression, or zero if the pattern is not found")]
            public abstract string patindex(string pattern, string expression);

            [Description("Converts a string to uppercase")]
            public abstract string upper(string value);

            [Description("Converts a string to lowercase")]
            public abstract string lower(string value);

            [Description("Returns the requested property of a specified collation")]
            public abstract int collationproperty(string collation_name, string property);

            [Description("Deletes a specified length of characters in the first string at the start position and then inserts the second string into the first string at the start position")]
            public abstract string stuff(string character_expression, int start, int length, string replace_with_expression);
        }
    }
}
