using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    public class OptionsPage : DialogPage
    {
        [Category("Safety")]
        [DisplayName("Block DELETE without WHERE")]
        [Description("Prevents executing a DELETE command without a WHERE clause that would affect all records")]
        [DefaultValue(true)]
        public bool BlockDeleteWithoutWhere { get; set; } = true;

        [Category("Safety")]
        [DisplayName("Block UPDATE without WHERE")]
        [Description("Prevents executing an UPDATE command without a WHERE clause that would affect all records")]
        [DefaultValue(true)]
        public bool BlockUpdateWithoutWhere { get; set; } = true;

        [Category("Performance")]
        [DisplayName("Batch Size")]
        [Description("The number of requests to send in each batch to the Dataverse API")]
        [DefaultValue(100)]
        public int BatchSize { get; set; } = 100;

        [Category("Performance")]
        [DisplayName("Max. Degree of Parallelism")]
        [Description("The number of threads to use for simultaneous requests to the Dataverse API")]
        [DefaultValue(10)]
        public int MaxDegreeOfParallelism { get; set; } = 10;

        [Category("Version")]
        [DisplayName("Installed")]
        [Description("Installed version of SQL 4 CDS - SSMS Edition")]
        public string Version { get; } = Assembly.GetExecutingAssembly().GetName().Version.ToString(3);

        [Category("Version")]
        [DisplayName("Latest")]
        [Description("Latest version of SQL 4 CDS available for download")]
        public string Latest
        {
            get
            {
                if (VersionChecker.Result.IsCompleted && !VersionChecker.Result.IsFaulted)
                    return VersionChecker.Result.Result.ToString(3);

                if (VersionChecker.Result.IsFaulted)
                    return "Error loading latest version";

                return "Loading...";
            }
        }

        public bool ShouldSerializeLatest()
        {
            return VersionChecker.Result.IsCompleted && Latest != Version;
        }
    }
}
