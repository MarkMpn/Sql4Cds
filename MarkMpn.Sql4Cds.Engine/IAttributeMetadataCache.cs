using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    public interface IAttributeMetadataCache
    {
        EntityMetadata this[string name] { get; }
    }
}
