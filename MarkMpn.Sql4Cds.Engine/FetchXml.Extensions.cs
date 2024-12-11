using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Xml.Serialization;

namespace MarkMpn.Sql4Cds.Engine.FetchXml
{
    partial class condition
    {
        [XmlAttribute("valueof")]
        public string ValueOf { get; set; }

        [XmlAttribute(Namespace = "MarkMpn.SQL4CDS")]
        [DefaultValue(false)]
        public bool IsVariable { get; set; }

        public override bool Equals(object obj)
        {
            if (!(obj is condition other))
                return false;

            if (other.entityname != entityname ||
                other.attribute != attribute ||
                other.@operator != @operator ||
                other.value != value ||
                other.ValueOf != ValueOf ||
                other.IsVariable != IsVariable ||
                other.aggregate != aggregate ||
                other.aggregateSpecified != aggregateSpecified ||
                other.alias != alias ||
                other.column != column ||
                other.rowaggregate != rowaggregate ||
                other.rowaggregateSpecified != rowaggregateSpecified)
                return false;

            if (other.Items == null ^ Items == null)
                return false;

            if (Items != null)
            {
                if (other.Items.Length != Items.Length)
                    return false;

                for (var i = 0; i < Items.Length; i++)
                {
                    if (other.Items[i].Value != Items[i].Value ||
                        other.Items[i].IsVariable != Items[i].IsVariable)
                        return false;
                }
            }

            return true;
        }

        public override int GetHashCode()
        {
            var hash = 0;
            hash ^= entityname?.GetHashCode() ?? 0;
            hash ^= attribute?.GetHashCode() ?? 0;
            hash ^= @operator.GetHashCode();
            hash ^= value?.GetHashCode() ?? 0;
            hash ^= ValueOf?.GetHashCode() ?? 0;
            hash ^= IsVariable.GetHashCode();
            hash ^= aggregate.GetHashCode();
            hash ^= aggregateSpecified.GetHashCode();
            hash ^= alias?.GetHashCode() ?? 0;
            hash ^= column?.GetHashCode() ?? 0;
            hash ^= rowaggregate.GetHashCode();
            hash ^= rowaggregateSpecified.GetHashCode();

            if (Items != null)
            {
                foreach (var item in Items)
                {
                    hash ^= item.Value?.GetHashCode() ?? 0;
                    hash ^= item.IsVariable.GetHashCode();
                }
            }

            return hash;
        }
    }

    partial class conditionValue
    {
        [XmlAttribute(Namespace = "MarkMpn.SQL4CDS")]
        [DefaultValue(false)]
        public bool IsVariable { get; set; }
    }

    partial class FetchLinkEntityType
    {
        [XmlIgnore]
        public bool SemiJoin { get; set; }

        [XmlIgnore]
        public bool RequireTablePrefix { get; set; }
    }

    partial class FetchType
    {
        [XmlAttribute("options")]
        public string Options { get; set; }

        [XmlAttribute("datasource")]
        public string DataSource { get; set; }

        [XmlAttribute("useraworderby")]
        [DefaultValue(false)]
        public bool UseRawOrderBy { get; set; }
    }

    partial class FetchOrderType
    {
        [XmlAttribute]
        public string entityname { get; set; }
    }
}
