using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Microsoft.SqlTools.ServiceLayer.QueryExecution
{
    internal class SR
    {
        public static string QueryServiceColumnNull => "(No column name)";
        public static string QueryServiceCellNull => "NULL";
        public static string QueryServiceDataReaderByteCountInvalid { get; internal set; }
        public static string QueryServiceDataReaderCharCountInvalid { get; internal set; }
        public static string QueryServiceDataReaderXmlCountInvalid { get; internal set; }

        internal static string QueryServiceUnsupportedSqlVariantType(string sqlVariantType, string columnName)
        {
            throw new NotImplementedException();
        }
    }
}
