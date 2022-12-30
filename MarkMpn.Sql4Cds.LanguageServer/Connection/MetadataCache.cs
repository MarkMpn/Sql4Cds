using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Metadata;
using System.Xml.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{

    public class MetadataCache
    {
        public int MetadataQueryVersion { get; set; }

        public string ClientVersionStamp { get; set; }

        public EntityMetadata[] EntityMetadata { get; set; }
    }
}
