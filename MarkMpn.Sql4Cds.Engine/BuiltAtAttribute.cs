using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    [AttributeUsage(AttributeTargets.Assembly)]
    class BuiltAtAttribute : Attribute
    {
        public BuiltAtAttribute(string buildDate)
        {
            BuildDate = buildDate;
        }

        public string BuildDate { get; }

        public DateTime Date => DateTime.ParseExact(BuildDate, "yyyyMMddHHmmss", null);
    }
}
