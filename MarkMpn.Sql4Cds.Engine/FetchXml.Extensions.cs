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
    }
}
