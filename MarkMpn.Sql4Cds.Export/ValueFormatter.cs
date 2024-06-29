using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Text;
using Microsoft.SqlTools.ServiceLayer.QueryExecution;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts;

namespace MarkMpn.Sql4Cds.Export
{
    public static class ValueFormatter
    {
        public static DbCellValue Format(object value, string dataTypeName, int? numericScale, bool localFormatDates)
        {
            if (value == null || value is DBNull || value is INullable nullable && nullable.IsNull)
            {
                return new DbCellValue
                {
                    DisplayValue = SR.QueryServiceCellNull,
                    IsNull = true
                };
            }

            var text = value?.ToString();

            if (value is bool b)
            {
                text = b ? "1" : "0";
            }
            else if (value is DateTime dt)
            {
                if (dataTypeName == "date")
                {
                    if (localFormatDates)
                        text = dt.ToShortDateString();
                    else
                        text = dt.ToString("yyyy-MM-dd");
                }
                else if (dataTypeName == "smalldatetime")
                {
                    if (localFormatDates)
                        text = dt.ToShortDateString() + " " + dt.ToString("HH:mm");
                    else
                        text = dt.ToString("yyyy-MM-dd HH:mm");
                }
                else if (!localFormatDates)
                {
                    text = dt.ToString("yyyy-MM-dd HH:mm:ss" + (numericScale == 0 ? "" : ("." + new string('f', numericScale.Value))));
                }
            }
            else if (value is TimeSpan ts && !localFormatDates)
            {
                text = ts.ToString("hh\\:mm\\:ss" + (numericScale == 0 ? "" : ("\\." + new string('f', numericScale.Value))));
            }
            else if (value is decimal dec)
            {
                text = dec.ToString("0" + (numericScale == 0 ? "" : ("." + new string('0', numericScale.Value))));
            }

            return new DbCellValue
            {
                DisplayValue = text,
                InvariantCultureDisplayValue = text,
                RawObject = value,
            };
        }
    }
}
