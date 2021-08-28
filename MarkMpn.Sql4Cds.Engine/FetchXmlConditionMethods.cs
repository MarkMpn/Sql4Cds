using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Contains implementations of FetchXML-specific conditions
    /// </summary>
    public static class FetchXmlConditionMethods
    {
        [Description("Matches a date/time value that occurred yesterday (relative to the current user timezone)")]
        public static SqlBoolean Yesterday(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred today (relative to the current user timezone)")]
        public static SqlBoolean Today(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs tomorrow (relative to the current user timezone)")]
        public static SqlBoolean Tomorrow(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred in the previous seven days or any time today up to now (relative to the current user timezone)")]
        public static SqlBoolean LastSevenDays(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs later today or during the following seven days (relative to the current user timezone)")]
        public static SqlBoolean NextSevenDays(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous week (Sunday to Saturday, relative to the current user timezone)")]
        public static SqlBoolean LastWeek(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the current week (Sunday to Saturday, relative to the current user timezone)")]
        public static SqlBoolean ThisWeek(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the next week (Sunday to Saturday, relative to the current user timezone)")]
        public static SqlBoolean NextWeek(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous calendar month (relative to the current user timezone)")]
        public static SqlBoolean LastMonth(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the current calendar month (relative to the current user timezone)")]
        public static SqlBoolean ThisMonth(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the next calendar month (relative to the current user timezone)")]
        public static SqlBoolean NextMonth(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs at any time during the given date")]
        public static SqlBoolean On(SqlDateTime field, SqlDateTime date) => ThrowException();

        [Description("Matches a date/time value that occurs before or at any time during the given date")]
        public static SqlBoolean OnOrBefore(SqlDateTime field, SqlDateTime date) => ThrowException();

        [Description("Matches a date/time value that occurs after or at any time during the given date")]
        public static SqlBoolean OnOrAfter(SqlDateTime field, SqlDateTime date) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous calendar year (relative to the current user timezone)")]
        public static SqlBoolean LastYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the current calendar year (relative to the current user timezone)")]
        public static SqlBoolean ThisYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the next calendar year (relative to the current user timezone)")]
        public static SqlBoolean NextYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous x whole hours, or from the start of the current hour up to now (relative to the current user timezone)")]
        public static SqlBoolean LastXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next x whole hours, or during the remainder of the current hour (relative to the current user timezone)")]
        public static SqlBoolean NextXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous x whole days, or from the start of the current day up to now (relative to the current user timezone)")]
        public static SqlBoolean LastXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next x whole days, or during the remainder of the current day (relative to the current user timezone)")]
        public static SqlBoolean NextXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous (x * 7) whole days, or from the start of the current day up to now (relative to the current user timezone)")]
        public static SqlBoolean LastXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next (x * 7) whole days, or during the remainder of the current day (relative to the current user timezone)")]
        public static SqlBoolean NextXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous x months, or from the start of the current day up to now (relative to the current user timezone)")]
        public static SqlBoolean LastXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next x months, or during the remainder of the current day (relative to the current user timezone)")]
        public static SqlBoolean NextXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than x months ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than x years ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than (x * 7) days ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than x days ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than x hours ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred more than x minutes ago (relative to the current user timezone)")]
        public static SqlBoolean OlderThanXMinutes(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the previous x years (relative to the current user timezone)")]
        public static SqlBoolean LastXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next x years (relative to the current user timezone)")]
        public static SqlBoolean NextXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches the ID of the current user")]
        public static SqlBoolean EqUserId(SqlGuid field) => ThrowException();

        [Description("Matches any value except the ID of the current user")]
        public static SqlBoolean NeUserId(SqlGuid field) => ThrowException();

        [Description("Matches the ID of any team the current user is a member of")]
        public static SqlBoolean EqUserTeams(SqlGuid field) => ThrowException();

        [Description("Matches the ID of the current user or any team the user is a member of")]
        public static SqlBoolean EqUserOrUserTeams(SqlGuid field) => ThrowException();

        [Description("Matches the ID of the current user or any user below them in the hierarchy")]
        public static SqlBoolean EqUserOrUserHierarchy(SqlGuid field) => ThrowException();

        [Description("Matches the ID of the current user, any user below them in the hierarchy, or any team those users are members of")]
        public static SqlBoolean EqUserOrUserHierarchyAndTeams(SqlGuid field) => ThrowException();

        [Description("Matches the ID of the business unit the current user is in")]
        public static SqlBoolean EqBusinessId(SqlGuid field) => ThrowException();

        [Description("Matches any value except the ID of the business unit the current user is in")]
        public static SqlBoolean NeBusinessId(SqlGuid field) => ThrowException();

        [Description("Matches the language code for the current user")]
        public static SqlBoolean EqUserLanguage(SqlInt32 field) => ThrowException();

        [Description("Matches a date/time value that occurs during the current fiscal year")]
        public static SqlBoolean ThisFiscalYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the current fiscal period (month/quarter)")]
        public static SqlBoolean ThisFiscalPeriod(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the next fiscal year")]
        public static SqlBoolean NextFiscalYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the next fiscal period (month/quarter)")]
        public static SqlBoolean NextFiscalPeriod(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the last fiscal year")]
        public static SqlBoolean LastFiscalYear(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurs during the last fiscal period (month/quarter)")]
        public static SqlBoolean LastFiscalPeriod(SqlDateTime field) => ThrowException();

        [Description("Matches a date/time value that occurred during the last x fiscal years, or from the start of the current fiscal year up to now")]
        public static SqlBoolean LastXFiscalYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the last x fiscal periods, or from the start of the current fiscal period up to now")]
        public static SqlBoolean LastXFiscalPeriods(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the next x fiscal years, or the remainder of the current fiscal year")]
        public static SqlBoolean NextXFiscalYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurred during the next x fiscal periods, or the remainder of the current fiscal period")]
        public static SqlBoolean NextXFiscalPeriods(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the given fiscal year")]
        public static SqlBoolean InFiscalYear(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the given fiscal period in any year")]
        public static SqlBoolean InFiscalPeriod(SqlDateTime field, SqlInt32 value) => ThrowException();

        [Description("Matches a date/time value that occurs during the given fiscal period & year")]
        public static SqlBoolean InFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        [Description("Matches a date/time value that occurs during or before the given fiscal period & year")]
        public static SqlBoolean InOrBeforeFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        [Description("Matches a date/time value that occurs during or after the given fiscal period & year")]
        public static SqlBoolean InOrAfterFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        [Description("Matches a text value that begins with the given prefix")]
        public static SqlBoolean BeginsWith(SqlString field, SqlString value) => ThrowException();

        [Description("Matches a text value that does not begin with the given prefix")]
        public static SqlBoolean NotBeginWith(SqlString field, SqlString value) => ThrowException();

        [Description("Matches a text value that ends with the given suffix")]
        public static SqlBoolean EndsWith(SqlString field, SqlString value) => ThrowException();

        [Description("Matches a text value that does not end with the given suffix")]
        public static SqlBoolean NotEndWith(SqlString field, SqlString value) => ThrowException();

        [Description("Matches a record that is below the given ID in the hierarchy")]
        public static SqlBoolean Under(SqlGuid field, SqlGuid value) => ThrowException();

        [Description("Matches a record that matches or is below the given ID in the hierarchy")]
        public static SqlBoolean EqOrUnder(SqlGuid field, SqlGuid value) => ThrowException();

        [Description("Matches any record that is not below the given ID in the hierarchy")]
        public static SqlBoolean NotUnder(SqlGuid field, SqlGuid value) => ThrowException();

        [Description("Matches a record that is above the given ID in the hierarchy")]
        public static SqlBoolean Above(SqlGuid field, SqlGuid value) => ThrowException();

        [Description("Matches a record that matches or is above the given ID in the hierarchy")]
        public static SqlBoolean EqOrAbove(SqlGuid field, SqlGuid value) => ThrowException();

        [Description("Matches a multi-select picklist value that contains any of the specified values")]
        public static SqlBoolean ContainValues(SqlString field, SqlInt32[] value) => ThrowException();

        [Description("Matches a multi-select picklist value that doesn't contains any of the specified values")]
        public static SqlBoolean NotContainValues(SqlString field, SqlInt32[] value) => ThrowException();

        private static SqlBoolean ThrowException()
        {
            throw new NotImplementedException("Custom FetchXML filter conditions must only be used where they can be folded into a FetchXML Scan operator");
        }
    }
}
