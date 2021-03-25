using System;
using System.Collections.Generic;
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
    static class FetchXmlConditionMethods
    {
        public static SqlBoolean Yesterday(SqlDateTime field) => ThrowException();

        public static SqlBoolean Today(SqlDateTime field) => ThrowException();
        
        public static SqlBoolean Tomorrow(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastSevenDays(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextSevenDays(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastWeek(SqlDateTime field) => ThrowException();

        public static SqlBoolean ThisWeek(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextWeek(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastMonth(SqlDateTime field) => ThrowException();

        public static SqlBoolean ThisMonth(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextMonth(SqlDateTime field) => ThrowException();

        public static SqlBoolean On(SqlDateTime field, SqlDateTime date) => ThrowException();

        public static SqlBoolean OnOrBefore(SqlDateTime field, SqlDateTime date) => ThrowException();

        public static SqlBoolean OnOrAfter(SqlDateTime field, SqlDateTime date) => ThrowException();

        public static SqlBoolean LastYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean ThisYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean LastXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean LastXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean LastXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXMonths(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXWeeks(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXDays(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXHours(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean OlderThanXMinutes(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean LastXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean EqUserId(SqlGuid field) => ThrowException();

        public static SqlBoolean NeUserId(SqlGuid field) => ThrowException();

        public static SqlBoolean EqUserTeams(SqlGuid field) => ThrowException();

        public static SqlBoolean EqUserOrUserTeams(SqlGuid field) => ThrowException();

        public static SqlBoolean EqUserOrUserHierarchy(SqlGuid field) => ThrowException();

        public static SqlBoolean EqUserOrUserHierarchyAndTeams(SqlGuid field) => ThrowException();

        public static SqlBoolean EqBusinessId(SqlGuid field) => ThrowException();

        public static SqlBoolean NeBusinessId(SqlGuid field) => ThrowException();

        public static SqlBoolean EqUserLanguage(SqlInt32 field) => ThrowException();

        public static SqlBoolean ThisFiscalYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean ThisFiscalPeriod(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextFiscalYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean NextFiscalPeriod(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastFiscalYear(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastFiscalPeriod(SqlDateTime field) => ThrowException();

        public static SqlBoolean LastXFiscalYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean LastXFiscalPeriods(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXFiscalYears(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean NextXFiscalPeriods(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean InFiscalYear(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean InFiscalPeriod(SqlDateTime field, SqlInt32 value) => ThrowException();

        public static SqlBoolean InFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        public static SqlBoolean InOrBeforeFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        public static SqlBoolean InOrAfterFiscalPeriodAndYear(SqlDateTime field, SqlInt32 period, SqlInt32 year) => ThrowException();

        public static SqlBoolean Under(SqlGuid field, SqlGuid value) => ThrowException();

        public static SqlBoolean EqOrUnder(SqlGuid field, SqlGuid value) => ThrowException();

        public static SqlBoolean NotUnder(SqlGuid field, SqlGuid value) => ThrowException();

        public static SqlBoolean Above(SqlGuid field, SqlGuid value) => ThrowException();

        public static SqlBoolean EqOrAbove(SqlGuid field, SqlGuid value) => ThrowException();

        public static SqlBoolean ContainValues(OptionSetValueCollection field, SqlInt32[] value) => ThrowException();

        public static SqlBoolean NotContainValues(OptionSetValueCollection field, SqlInt32[] value) => ThrowException();

        private static SqlBoolean ThrowException()
        {
            throw new NotImplementedException("Custom FetchXML filter conditions must only be used where they can be folded into a FetchXML Scan operator");
        }
    }
}
