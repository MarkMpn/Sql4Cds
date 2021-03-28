using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds
{
    class FunctionMetadata
    {
        [AttributeUsage(AttributeTargets.Method)]
        public class AggregateAttribute : Attribute
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

            [Description("Finds the difference between two date values")]
            public abstract int datediff(string datepart, DateTime startdate, DateTime enddate);

            [Description("Adds a number value to a date value")]
            public abstract DateTime dateadd(string datepart, int number, DateTime date);

            [Description("Creates a lookup value to be used for polymorphic lookup fields in INSERT or UPDATE queries")]
            public abstract EntityReference createlookup(string entitytype, string id);

            [Description("Replaces one string value with another")]
            public abstract string replace(string input, string find, string replace);

            [Description("Gets the length of the specified string expression")]
            public abstract int len(string value);

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
        }
    }
}
