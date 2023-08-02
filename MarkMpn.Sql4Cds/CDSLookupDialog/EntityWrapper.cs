using Microsoft.Xrm.Sdk;
using System;

namespace xrmtb.XrmToolBox.Controls.Helper
{
    public class EntityWrapper
    {
        #region Private Fields

        private readonly Entity entity;
        private IOrganizationService service;

        #endregion Private Fields

        #region Public Constructors

        public EntityWrapper(Entity entity, string format, IOrganizationService organizationService)
        {
            this.entity = entity;
            this.Format = format;
            this.service = organizationService;
        }

        #endregion Public Constructors

        #region Public Properties

        public Entity Entity => entity;

        public string Format { get; set; }

        public static string EntityToString(Entity entity, IOrganizationService service, string Format = null)
        {
            if (entity == null)
            {
                return string.Empty;
            }
            var value = Format;
            if (string.IsNullOrWhiteSpace(value))
            {
                value = MetadataHelper.GetPrimaryAttribute(service, entity.LogicalName)?.LogicalName ?? string.Empty;
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
                var partvalue = GetFormattedValue(entity, service, attribute, format);
                value = value.Replace("{{" + part + "}}", partvalue);
            }
            return value;
        }

        #endregion Public Properties

        #region Private Methods

        private static string GetFormattedValue(Entity entity, IOrganizationService service, string attribute, string format)
        {
            if (!entity.Contains(attribute))
            {
                return string.Empty;
            }
            var value = entity[attribute];
            var metadata = MetadataHelper.GetAttribute(service, entity.LogicalName, attribute, value);
            if (EntitySerializer.AttributeToBaseType(value) is DateTime dtvalue && (dtvalue).Kind == DateTimeKind.Utc)
            {
                value = dtvalue.ToLocalTime();
            }
            if (!ValueTypeIsFriendly(value) && metadata != null)
            {
                value = EntitySerializer.AttributeToString(value, metadata, format);
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
            return EntityToString(entity, service, Format);
        }

        #endregion Public Methods
    }
}
