using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.SSMS
{
    class VersionChecker
    {
        public static Task<Version> Result { get; private set; }

        public static void Check()
        {
            Result = Task.Run(() =>
            {
                var req = WebRequest.CreateHttp("https://markcarrington.dev/sql4cds-version.txt");

                using (var resp = req.GetResponse())
                using (var stream =resp.GetResponseStream())
                using (var reader = new StreamReader(stream))
                {
                    var s = reader.ReadToEnd();
                    var version = new Version(s);

                    return version;
                }
            });
        }
    }
}
