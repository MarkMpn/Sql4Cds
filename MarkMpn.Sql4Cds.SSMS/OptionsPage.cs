using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
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
    }
}
