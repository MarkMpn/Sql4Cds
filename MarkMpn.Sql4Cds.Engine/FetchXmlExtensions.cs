using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;

namespace MarkMpn.Sql4Cds.Engine
{
    static class FetchXmlExtensions
    {
        public static void AddItem(this FetchEntityType entity, object item)
        {
            if (entity.Items == null)
                entity.Items = new[] { item };
            else
                entity.Items = entity.Items.Concat(new[] { item }).ToArray();
        }

        public static void AddItem(this FetchLinkEntityType linkEntity, object item)
        {
            if (linkEntity.Items == null)
                linkEntity.Items = new[] { item };
            else
                linkEntity.Items = linkEntity.Items.Concat(new[] { item }).ToArray();
        }
    }
}
