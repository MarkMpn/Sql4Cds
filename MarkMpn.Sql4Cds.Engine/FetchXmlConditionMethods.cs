using System;
using System.Collections.Generic;
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
        public static bool Yesterday(DateTime? field) => ThrowException();

        public static bool Today(DateTime? field) => ThrowException();
        
        public static bool Tomorrow(DateTime? field) => ThrowException();

        public static bool LastSevenDays(DateTime? field) => ThrowException();

        public static bool NextSevenDays(DateTime? field) => ThrowException();

        public static bool LastWeek(DateTime? field) => ThrowException();

        public static bool ThisWeek(DateTime? field) => ThrowException();

        public static bool NextWeek(DateTime? field) => ThrowException();

        public static bool LastMonth(DateTime? field) => ThrowException();

        public static bool ThisMonth(DateTime? field) => ThrowException();

        public static bool NextMonth(DateTime? field) => ThrowException();

        public static bool On(DateTime? field, DateTime? date) => ThrowException();

        public static bool OnOrBefore(DateTime? field, DateTime? date) => ThrowException();

        public static bool OnOrAfter(DateTime? field, DateTime? date) => ThrowException();

        public static bool LastYear(DateTime? field) => ThrowException();

        public static bool ThisYear(DateTime? field) => ThrowException();

        public static bool NextYear(DateTime? field) => ThrowException();

        public static bool LastXHours(DateTime? field, int? value) => ThrowException();

        public static bool NextXHours(DateTime? field, int? value) => ThrowException();

        public static bool LastXDays(DateTime? field, int? value) => ThrowException();

        public static bool NextXDays(DateTime? field, int? value) => ThrowException();

        public static bool LastXWeeks(DateTime? field, int? value) => ThrowException();

        public static bool NextXWeeks(DateTime? field, int? value) => ThrowException();

        public static bool LastXMonths(DateTime? field, int? value) => ThrowException();

        public static bool NextXMonths(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXMonths(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXYears(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXWeeks(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXDays(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXHours(DateTime? field, int? value) => ThrowException();

        public static bool OlderThanXMinutes(DateTime? field, int? value) => ThrowException();

        public static bool LastXYears(DateTime? field, int? value) => ThrowException();

        public static bool NextXYears(DateTime? field, int? value) => ThrowException();

        public static bool EqUserId(Guid? field) => ThrowException();

        public static bool NeUserId(Guid? field) => ThrowException();

        public static bool EqUserTeams(Guid? field) => ThrowException();

        public static bool EqUserOrUserTeams(Guid? field) => ThrowException();

        public static bool EqUserOrUserHierarchy(Guid? field) => ThrowException();

        public static bool EqUserOrUserHierarchyAndTeams(Guid? field) => ThrowException();

        public static bool EqBusinessId(Guid? field) => ThrowException();

        public static bool NeBusinessId(Guid? field) => ThrowException();

        public static bool EqUserLanguage(int? field) => ThrowException();

        public static bool ThisFiscalYear(DateTime? field) => ThrowException();

        public static bool ThisFiscalPeriod(DateTime? field) => ThrowException();

        public static bool NextFiscalYear(DateTime? field) => ThrowException();

        public static bool NextFiscalPeriod(DateTime? field) => ThrowException();

        public static bool LastFiscalYear(DateTime? field) => ThrowException();

        public static bool LastFiscalPeriod(DateTime? field) => ThrowException();

        public static bool LastXFiscalYears(DateTime? field, int? value) => ThrowException();

        public static bool LastXFiscalPeriods(DateTime? field, int? value) => ThrowException();

        public static bool NextXFiscalYears(DateTime? field, int? value) => ThrowException();

        public static bool NextXFiscalPeriods(DateTime? field, int? value) => ThrowException();

        public static bool InFiscalYear(DateTime? field, int? value) => ThrowException();

        public static bool InFiscalPeriod(DateTime? field, int? value) => ThrowException();

        public static bool InFiscalPeriodAndYear(DateTime? field, int? period, int? year) => ThrowException();

        public static bool InOrBeforeFiscalPeriodAndYear(DateTime? field, int? period, int? year) => ThrowException();

        public static bool InOrAfterFiscalPeriodAndYear(DateTime? field, int? period, int? year) => ThrowException();

        public static bool Under(Guid? field, Guid? value) => ThrowException();

        public static bool EqOrUnder(Guid? field, Guid? value) => ThrowException();

        public static bool NotUnder(Guid? field, Guid? value) => ThrowException();

        public static bool Above(Guid? field, Guid? value) => ThrowException();

        public static bool EqOrAbove(Guid? field, Guid? value) => ThrowException();

        public static bool ContainValues(OptionSetValueCollection field, int?[] value) => ThrowException();

        public static bool NotContainValues(OptionSetValueCollection field, int?[] value) => ThrowException();

        private static Exception CreateException()
        {
            return new NotImplementedException("Custom FetchXML filter conditions must only be used where they can be folded into a FetchXML Scan operator");
        }

        private static bool ThrowException()
        {
            throw new NotImplementedException();
        }
    }
}
