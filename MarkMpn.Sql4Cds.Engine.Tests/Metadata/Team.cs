using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
{
    [EntityLogicalName("team")]
    class Team
    {
        [AttributeLogicalName("teamid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("teamid")]
        public Guid TeamId { get; set; }

        [AttributeLogicalName("name")]
        public string Name { get; set; }
    }
}
