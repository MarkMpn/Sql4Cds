using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;

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

        public static void AddItem(this filter filter, object item)
        {
            if (filter.Items == null)
                filter.Items = new[] { item };
            else
                filter.Items = filter.Items.Concat(new[] { item }).ToArray();
        }

        public static FetchLinkEntityType FindLinkEntity(this FetchEntityType entity, string alias)
        {
            return FindLinkEntity(entity.Items, alias);
        }

        public static FetchLinkEntityType FindLinkEntity(object[] items, string alias)
        {
            if (items == null)
                return null;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (linkEntity.alias != null && linkEntity.alias.Equals(alias, StringComparison.OrdinalIgnoreCase))
                    return linkEntity;

                var childMatch = FindLinkEntity(linkEntity.Items, alias);

                if (childMatch != null)
                    return childMatch;
            }

            return null;
        }

        public static IEnumerable<FetchLinkEntityType> GetLinkEntities(this FetchEntityType entity, bool innerOnly = false)
        {
            foreach (var linkEntity in GetLinkEntities(entity.Items, innerOnly))
                yield return linkEntity;
        }

        private static IEnumerable<FetchLinkEntityType> GetLinkEntities(object[] items, bool innerOnly)
        {
            if (items == null)
                yield break;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (innerOnly && linkEntity.linktype != "inner")
                    continue;

                yield return linkEntity;

                foreach (var childLinkEntity in GetLinkEntities(linkEntity.Items, innerOnly))
                    yield return childLinkEntity;
            }
        }

        public static FetchAttributeType FindAliasedAttribute(this FetchEntityType entity, string colName, Func<FetchAttributeType, bool> predicate, out FetchLinkEntityType linkEntity)
        {
            linkEntity = null;

            return FindAliasedAttribute(entity.Items, colName, predicate, ref linkEntity);
        }

        private static FetchAttributeType FindAliasedAttribute(object[] items, string colName, Func<FetchAttributeType, bool> predicate, ref FetchLinkEntityType linkEntity)
        {
            if (items == null)
                return null;

            var match = items.OfType<FetchAttributeType>()
                .Where(a => a.alias == colName && (predicate == null || predicate(a)))
                .FirstOrDefault();

            if (match != null)
                return match;

            foreach (var le in items.OfType<FetchLinkEntityType>())
            {
                match = FindAliasedAttribute(le.Items, colName, predicate, ref linkEntity);

                if (match != null)
                {
                    if (linkEntity == null)
                        linkEntity = le;

                    return match;
                }
            }

            return null;
        }

        public static IEnumerable<condition> GetConditions(this FetchEntityType entity, bool andOnly = false)
        {
            return GetConditions(entity.Items, andOnly);
        }

        public static IEnumerable<condition> GetConditions(this FetchLinkEntityType linkEntity, bool andOnly = false)
        {
            return GetConditions(linkEntity.Items, andOnly);
        }

        private static IEnumerable<condition> GetConditions(object[] items, bool andOnly)
        {
            if (items == null)
                return Array.Empty<condition>();

            return items.OfType<condition>()
                .Concat(items.OfType<filter>().Where(f => f.type == filterType.and || !andOnly).SelectMany(f => GetConditions(f.Items, andOnly)));
        }

        public static void RemoveAttributes(this FetchEntityType entity)
        {
            if (entity.Items == null)
                return;

            entity.Items = entity.Items.Where(i => !(i is FetchAttributeType || i is allattributes)).ToArray();

            foreach (var link in entity.GetLinkEntities())
            {
                if (link.Items == null)
                    continue;

                link.Items = link.Items.Where(i => !(i is FetchAttributeType || i is allattributes)).ToArray();
            }
        }

        public static FetchLinkEntityType RemoveNotNullJoinCondition(this FetchLinkEntityType linkEntity)
        {
            if (linkEntity.Items == null)
                return linkEntity;

            foreach (var filter in linkEntity.Items.OfType<filter>())
            {
                var notNull = filter.Items
                    .OfType<condition>()
                    .Where(c => c.attribute == linkEntity.from && c.entityname == null && c.@operator == @operator.notnull);

                filter.Items = filter.Items.Except(notNull).ToArray();
            }

            return linkEntity;
        }


        public static IEnumerable<condition> GetConditions(this filter filter)
        {
            return filter.Items
                .OfType<condition>()
                .Concat(filter.Items
                    .OfType<filter>()
                    .SelectMany(f => f.GetConditions()));
        }

        public static void RemoveSorts(this FetchEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(i => !(i is FetchOrderType)).ToArray();

                foreach (var linkEntity in entity.GetLinkEntities())
                    linkEntity.RemoveSorts();
            }
        }

        public static void RemoveSorts(this FetchLinkEntityType linkEntity, bool recurse = false)
        {
            if (linkEntity.Items == null)
                return;

            linkEntity.Items = linkEntity.Items.Where(i => !(i is FetchOrderType)).ToArray();

            if (recurse)
            {
                foreach (var child in GetLinkEntities(linkEntity.Items, false))
                    linkEntity.RemoveSorts(true);
            }
        }
    }
}
