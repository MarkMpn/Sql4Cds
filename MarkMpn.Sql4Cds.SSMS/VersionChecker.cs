using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    class VersionChecker
    {
        public static Task<Version> Result { get; private set; }

        public static void Check()
        {
            Result = System.Threading.Tasks.Task.Run(() =>
            {
                var req = WebRequest.CreateHttp("https://markcarrington.dev/sql4cds-version.txt");

                using (var resp = req.GetResponse())
                using (var stream =resp.GetResponseStream())
                using (var reader = new StreamReader(stream))
                {
                    var s = reader.ReadToEnd();
                    var version = new Version(s);

                    var currentVersion = Assembly.GetExecutingAssembly().GetName().Version;
                    if (version > currentVersion)
                        VsShellUtilities.LogWarning("SQL 4 CDS", $"Updated version available. Current version {currentVersion}, latest version {version}");

                    return version;
                }
            });
        }
    }
}
