using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Linq;

namespace xrmtb.XrmToolBox.Controls.Helper
{
    public class EntityWrapper
    {
        #region Private Fields

        private readonly Entity entity;
        private IAttributeMetadataCache metadata;

        #endregion Private Fields

        #region Public Constructors

        public EntityWrapper(Entity entity, string format, IAttributeMetadataCache metadata)
        {
            this.entity = entity;
            this.Format = format;
            this.metadata = metadata;
        }

        #endregion Public Constructors

        #region Public Properties

        public Entity Entity => entity;

        public string Format { get; set; }

        public static string EntityToString(Entity entity, IAttributeMetadataCache metadata, string Format = null)
        {
            if (entity == null)
            {
                return string.Empty;
            }
            var value = Format;
            if (string.IsNullOrWhiteSpace(value))
            {
                metadata.TryGetValue(entity.LogicalName, out var entityMetadata);
                value = entityMetadata?.PrimaryNameAttribute ?? string.Empty;
            }
            if (!value.Contains("{{") || !value.Contains("}}"))
            {
                value = "{{" + value + "}}";
            }
            while (value.Contains("{{") && value.Contains("}}"))
            {
                var part = value.Substring(value.IndexOf("{{") + 2).Split(new string[] { "}}" }, StringSplitOptions.None)[0];
                var attribute = part;
                var format = string.Empty;
                if (part.Contains("|"))
                {
                    attribute = part.Split('|')[0];
                    format = part.Split('|')[1];
                }
                var partvalue = GetFormattedValue(entity, metadata, attribute, format);
                value = value.Replace("{{" + part + "}}", partvalue);
            }
            return value;
        }

        #endregion Public Properties

        #region Private Methods

        private static string GetFormattedValue(Entity entity, IAttributeMetadataCache metadata, string attribute, string format)
        {
            if (!entity.Contains(attribute))
            {
                return string.Empty;
            }
            var value = entity[attribute];

            AttributeMetadata attributeMetadata = null;
            if (metadata.TryGetValue(entity.LogicalName, out var entityMetadata))
                attributeMetadata = entityMetadata.Attributes.SingleOrDefault(a => a.LogicalName == attribute);

            if (EntitySerializer.AttributeToBaseType(value) is DateTime dtvalue && (dtvalue).Kind == DateTimeKind.Utc)
            {
                value = dtvalue.ToLocalTime();
            }
            if (!ValueTypeIsFriendly(value) && metadata != null)
            {
                value = EntitySerializer.AttributeToString(value, attributeMetadata, format);
            }
            else
            {
                value = EntitySerializer.AttributeToBaseType(value).ToString();
            }
            return value.ToString();
        }

        private static bool ValueTypeIsFriendly(object value)
        {
            return value is Int32 || value is decimal || value is double || value is string || value is Money;
        }

        #endregion Private Methods

        #region Public Methods

        public override string ToString()
        {
            if (entity == null)
            {
                return string.Empty;
            }
            return EntityToString(entity, metadata, Format);
        }

        #endregion Public Methods
    }
}
