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

        public abstract class FetchXmlOperators
        {
            [Description("Matches a date/time value that occurred yesterday (relative to the current user timezone)")]
            public abstract DateTime yesterday();

            [Description("Matches a date/time value that occurred today (relative to the current user timezone)")]
            public abstract DateTime today();

            [Description("Matches a date/time value that occurs tomorrow (relative to the current user timezone)")]
            public abstract DateTime tomorrow();

            [Description("Matches a date/time value that occurred in the previous seven days or any time today up to now (relative to the current user timezone)")]
            public abstract DateTime lastsevendays();

            [Description("Matches a date/time value that occurs later today or during the following seven days (relative to the current user timezone)")]
            public abstract DateTime nextsevendays();

            [Description("Matches a date/time value that occurred during the previous week (Sunday to Saturday, relative to the current user timezone)")]
            public abstract DateTime lastweek();

            [Description("Matches a date/time value that occurs during the current week (Sunday to Saturday, relative to the current user timezone)")]
            public abstract DateTime thisweek();

            [Description("Matches a date/time value that occurs during the next week (Sunday to Saturday, relative to the current user timezone)")]
            public abstract DateTime nextweek();

            [Description("Matches a date/time value that occurred during the previous calendar month (relative to the current user timezone)")]
            public abstract DateTime lastmonth();

            [Description("Matches a date/time value that occurs during the current calendar month (relative to the current user timezone)")]
            public abstract DateTime thismonth();

            [Description("Matches a date/time value that occurs during the next calendar month (relative to the current user timezone)")]
            public abstract DateTime nextmonth();

            [Description("Matches a date/time value that occurs at any time during the given date")]
            public abstract DateTime on(DateTime date);

            [Description("Matches a date/time value that occurs before or at any time during the given date")]
            public abstract DateTime onorbefore(DateTime date);

            [Description("Matches a date/time value that occurs after or at any time during the given date")]
            public abstract DateTime onorafter(DateTime date);

            [Description("Matches a date/time value that occurred during the previous calendar year (relative to the current user timezone)")]
            public abstract DateTime lastyear();

            [Description("Matches a date/time value that occurs during the current calendar year (relative to the current user timezone)")]
            public abstract DateTime thisyear();

            [Description("Matches a date/time value that occurs during the next calendar year (relative to the current user timezone)")]
            public abstract DateTime nextyear();

            [Description("Matches a date/time value that occurred during the previous x whole hours, or from the start of the current hour up to now (relative to the current user timezone)")]
            public abstract DateTime lastxhours(int x);

            [Description("Matches a date/time value that occurs during the next x whole hours, or during the remainder of the current hour (relative to the current user timezone)")]
            public abstract DateTime nextxhours(int x);

            [Description("Matches a date/time value that occurred during the previous x whole days, or from the start of the current day up to now (relative to the current user timezone)")]
            public abstract DateTime lastxdays(int x);

            [Description("Matches a date/time value that occurs during the next x whole days, or during the remainder of the current day (relative to the current user timezone)")]
            public abstract DateTime nextxdays(int x);

            [Description("Matches a date/time value that occurred during the previous (x * 7) whole days, or from the start of the current day up to now (relative to the current user timezone)")]
            public abstract DateTime lastxweeks(int x);

            [Description("Matches a date/time value that occurs during the next (x * 7) whole days, or during the remainder of the current day (relative to the current user timezone)")]
            public abstract DateTime nextxweeks(int x);

            [Description("Matches a date/time value that occurred during the previous x months, or from the start of the current day up to now (relative to the current user timezone)")]
            public abstract DateTime lastxmonths(int x);

            [Description("Matches a date/time value that occurs during the next x months, or during the remainder of the current day (relative to the current user timezone)")]
            public abstract DateTime nextxmonths(int x);

            [Description("Matches a date/time value that occurred more than x months ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxmonths(int x);

            [Description("Matches a date/time value that occurred more than x years ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxyears(int x);

            [Description("Matches a date/time value that occurred more than (x * 7) days ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxweeks(int x);

            [Description("Matches a date/time value that occurred more than x days ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxdays(int x);

            [Description("Matches a date/time value that occurred more than x hours ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxhours(int x);

            [Description("Matches a date/time value that occurred more than x minutes ago (relative to the current user timezone)")]
            public abstract DateTime olderthanxminutes(int x);

            [Description("Matches a date/time value that occurred during the previous x years (relative to the current user timezone)")]
            public abstract DateTime lastxyears(int x);

            [Description("Matches a date/time value that occurs during the next x years (relative to the current user timezone)")]
            public abstract DateTime nextxyears(int x);

            [Description("Matches the ID of the current user")]
            public abstract EntityReference equserid();

            [Description("Matches any value except the ID of the current user")]
            public abstract EntityReference neuserid();

            [Description("Matches the ID of any team the current user is a member of")]
            public abstract EntityReference equserteams();

            [Description("Matches the ID of the current user or any team the user is a member of")]
            public abstract EntityReference equseroruserteams();

            [Description("Matches the ID of the current user or any user below them in the hierarchy")]
            public abstract EntityReference equseroruserhierarchy();

            [Description("Matches the ID of the current user, any user below them in the hierarchy, or any team those users are members of")]
            public abstract EntityReference equseroruserhierarchyandteams();

            [Description("Matches the ID of the business unit the current user is in")]
            public abstract EntityReference eqbusinessid();

            [Description("Matches any value except the ID of the business unit the current user is in")]
            public abstract EntityReference nebusinessid();

            [Description("Matches the language code for the current user")]
            public abstract int equserlanguage();

            [Description("Matches a date/time value that occurs during the current fiscal year")]
            public abstract DateTime thisfiscalyear();

            [Description("Matches a date/time value that occurs during the current fiscal period (month/quarter)")]
            public abstract DateTime thisfiscalperiod();

            [Description("Matches a date/time value that occurs during the next fiscal year")]
            public abstract DateTime nextfiscalyear();

            [Description("Matches a date/time value that occurs during the next fiscal period (month/quarter)")]
            public abstract DateTime nextfiscalperiod();

            [Description("Matches a date/time value that occurs during the last fiscal year")]
            public abstract DateTime lastfiscalyear();

            [Description("Matches a date/time value that occurs during the last fiscal period (month/quarter)")]
            public abstract DateTime lastfiscalperiod();

            [Description("Matches a date/time value that occurred during the last x fiscal years, or from the start of the current fiscal year up to now")]
            public abstract DateTime lastxfiscalyears(int x);

            [Description("Matches a date/time value that occurred during the last x fiscal periods, or from the start of the current fiscal period up to now")]
            public abstract DateTime lastxfiscalperiods(int x);

            [Description("Matches a date/time value that occurs during the next x fiscal years, or the remainder of the current fiscal year")]
            public abstract DateTime nextxfiscalyears(int x);

            [Description("Matches a date/time value that occurred during the next x fiscal periods, or the remainder of the current fiscal period")]
            public abstract DateTime nextxfiscalperiods(int x);

            [Description("Matches a date/time value that occurs during the given fiscal year")]
            public abstract DateTime infiscalyear(int year);

            [Description("Matches a date/time value that occurs during the given fiscal period in any year")]
            public abstract DateTime infiscalperiod(int period);

            [Description("Matches a date/time value that occurs during the given fiscal period & year")]
            public abstract DateTime infiscalperiodandyear(int year, int period);

            [Description("Matches a date/time value that occurs during or before the given fiscal period & year")]
            public abstract DateTime inorbeforefiscalperiodandyear(int year, int period);

            [Description("Matches a date/time value that occurs during or after the given fiscal period & year")]
            public abstract DateTime inorafterfiscalperiodandyear(int year, int period);

            [Description("Matches a text value that begins with the given prefix")]
            public abstract string beginswith(string prefix);

            [Description("Matches a text value that does not begin with the given prefix")]
            public abstract string notbeginwith(string prefix);

            [Description("Matches a text value that ends with the given suffix")]
            public abstract string endswith(string suffix);

            [Description("Matches a text value that does not end with the given suffix")]
            public abstract string notendwith(string suffix);

            [Description("Matches a record that is below the given ID in the hierarchy")]
            public abstract EntityReference under(Guid id);

            [Description("Matches a record that matches or is below the given ID in the hierarchy")]
            public abstract EntityReference eqorunder(Guid id);

            [Description("Matches any record that is not below the given ID in the hierarchy")]
            public abstract EntityReference notunder(Guid id);

            [Description("Matches a record that is above the given ID in the hierarchy")]
            public abstract EntityReference above(Guid id);

            [Description("Matches a record that matches or is above the given ID in the hierarchy")]
            public abstract EntityReference eqorabove(Guid id);

            [Description("Matches a multi-select picklist value that contains any of the specified values")]
            public abstract OptionSetValueCollection containvalues(params int[] values);

            [Description("Matches a multi-select picklist value that doesn't contains any of the specified values")]
            public abstract OptionSetValueCollection notcontainvalues(params int[] values);
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
