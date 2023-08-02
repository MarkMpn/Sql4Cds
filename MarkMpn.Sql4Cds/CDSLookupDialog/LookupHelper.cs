using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.ServiceModel;
using System.Xml;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace xrmtb.XrmToolBox.Controls.Helper
{
    /// <summary>
    /// Helper methods to work with Quick Find views
    /// </summary>
    internal static class LookupHelper
    {
        private const int ViewType_QuickFind = 4;
        #region Internal Methods

        /// <summary>
        /// Runs a Quick Find search
        /// </summary>
        /// <param name="service">The <see cref="IOrganizationService"/> to use to run the query</param>
        /// <param name="logicalName">The logical name of the entity to search</param>
        /// <param name="view">The definition of the Quick Find view to use</param>
        /// <param name="search">The value to search for</param>
        /// <returns>A list of matching record</returns>
        internal static EntityCollection ExecuteQuickFind(this IOrganizationService service, string logicalName, Entity view, string search)
        {
            if (service == null)
            {
                return null;
            }
            var fetchDoc = new XmlDocument();
            fetchDoc.LoadXml(view.GetAttributeValue<string>(Savedquery.Fetchxml));
            var filterNodes = fetchDoc.SelectNodes("fetch/entity/filter");
            var metadata = MetadataHelper.GetEntity(service, logicalName);
            foreach (XmlNode filterNode in filterNodes)
            {
                ProcessFilter(metadata, filterNode, search);
            }
            return service.RetrieveMultiple(new FetchExpression(fetchDoc.OuterXml));
        }

        internal static EntityCollection RetrieveSystemViews(this IOrganizationService service, string logicalname, bool quickfind)
        {
            if (service == null)
            {
                return null;
            }
            var qe = new QueryExpression(Savedquery.EntityName);
            qe.ColumnSet.AddColumns(Savedquery.PrimaryName, Savedquery.Fetchxml, Savedquery.Layoutxml, Savedquery.QueryType, Savedquery.Isquickfindquery);
            qe.Criteria.AddCondition(Savedquery.Fetchxml, ConditionOperator.NotNull);
            qe.Criteria.AddCondition(Savedquery.Layoutxml, ConditionOperator.NotNull);
            qe.Criteria.AddCondition(Savedquery.ReturnedTypeCode, ConditionOperator.Equal, logicalname);
            qe.Criteria.AddCondition(Savedquery.QueryType, quickfind ? ConditionOperator.Equal : ConditionOperator.NotEqual, ViewType_QuickFind);
            try
            {
                var newviews = service.RetrieveMultiple(qe);
                return newviews;
            }
            catch (FaultException<OrganizationServiceFault>)
            {
                return null;
            }
        }

        internal static EntityCollection RetrievePersonalViews(this IOrganizationService service, string logicalname)
        {
            if (service == null)
            {
                return null;
            }
            var qe = new QueryExpression(UserQuery.EntityName);
            qe.ColumnSet.AddColumns(UserQuery.PrimaryName, UserQuery.Fetchxml, UserQuery.Layoutxml, UserQuery.QueryType);
            qe.Criteria.AddCondition(UserQuery.Fetchxml, ConditionOperator.NotNull);
            qe.Criteria.AddCondition(UserQuery.Layoutxml, ConditionOperator.NotNull);
            qe.Criteria.AddCondition(UserQuery.ReturnedTypeCode, ConditionOperator.Equal, logicalname);
            try
            {
                var newviews = service.RetrieveMultiple(qe);
                return newviews;
            }
            catch (FaultException<OrganizationServiceFault>)
            {
                return null;
            }
        }

        #endregion Internal Methods

        #region Private Methods

        private static void ProcessFilter(EntityMetadata metadata, XmlNode node, string searchTerm)
        {
            foreach (XmlNode condition in node.SelectNodes("condition"))
            {
                if (condition.Attributes["value"]?.Value?.StartsWith("{") != true)
                {
                    continue;
                }
                var attr = metadata.Attributes.First(a => a.LogicalName == condition.Attributes["attribute"].Value);

                #region Manage each attribute type

                switch (attr.AttributeType.Value)
                {
                    case AttributeTypeCode.Memo:
                    case AttributeTypeCode.String:
                        {
                            condition.Attributes["value"].Value = searchTerm.Replace("*", "%") + "%";
                        }
                        break;
                    case AttributeTypeCode.Boolean:
                        {
                            if (searchTerm != "0" && searchTerm != "1")
                            {
                                node.RemoveChild(condition);
                                continue;
                            }

                            condition.Attributes["value"].Value = (searchTerm == "1").ToString();
                        }
                        break;
                    case AttributeTypeCode.Customer:
                    case AttributeTypeCode.Lookup:
                    case AttributeTypeCode.Owner:
                        {
                            if (
                                metadata.Attributes.FirstOrDefault(
                                    a => a.LogicalName == condition.Attributes["attribute"].Value + "name") == null)
                            {
                                node.RemoveChild(condition);

                                continue;
                            }


                            condition.Attributes["attribute"].Value += "name";
                            condition.Attributes["value"].Value = searchTerm.Replace("*", "%") + "%";
                        }
                        break;
                    case AttributeTypeCode.DateTime:
                        {
                            DateTime dt;
                            if (!DateTime.TryParse(searchTerm, out dt))
                            {
                                condition.Attributes["value"].Value = new DateTime(1754, 1, 1).ToString("yyyy-MM-dd");
                            }
                            else
                            {
                                condition.Attributes["value"].Value = dt.ToString("yyyy-MM-dd");
                            }
                        }
                        break;
                    case AttributeTypeCode.Decimal:
                    case AttributeTypeCode.Double:
                    case AttributeTypeCode.Money:
                        {
                            decimal d;
                            if (!decimal.TryParse(searchTerm, out d))
                            {
                                condition.Attributes["value"].Value = int.MinValue.ToString(CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                condition.Attributes["value"].Value = d.ToString(CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                    case AttributeTypeCode.Integer:
                        {
                            int d;
                            if (!int.TryParse(searchTerm, out d))
                            {
                                condition.Attributes["value"].Value = int.MinValue.ToString(CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                condition.Attributes["value"].Value = d.ToString(CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                    case AttributeTypeCode.Picklist:
                        {
                            var opt = ((PicklistAttributeMetadata)attr).OptionSet.Options.FirstOrDefault(
                                o => o.Label.UserLocalizedLabel.Label == searchTerm);

                            if (opt == null)
                            {
                                condition.Attributes["value"].Value = int.MinValue.ToString(CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                condition.Attributes["value"].Value = opt.Value.Value.ToString(CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                    case AttributeTypeCode.State:
                        {
                            var opt = ((StateAttributeMetadata)attr).OptionSet.Options.FirstOrDefault(
                                o => o.Label.UserLocalizedLabel.Label == searchTerm);

                            if (opt == null)
                            {
                                condition.Attributes["value"].Value = int.MinValue.ToString(CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                condition.Attributes["value"].Value = opt.Value.Value.ToString(CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                    case AttributeTypeCode.Status:
                        {
                            var opt = ((StatusAttributeMetadata)attr).OptionSet.Options.FirstOrDefault(
                                o => o.Label.UserLocalizedLabel.Label == searchTerm);

                            if (opt == null)
                            {
                                condition.Attributes["value"].Value = int.MinValue.ToString(CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                condition.Attributes["value"].Value = opt.Value.Value.ToString(CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                }

                #endregion
            }

            foreach (XmlNode filter in node.SelectNodes("filter"))
            {
                ProcessFilter(metadata, filter, searchTerm);
            }
        }

        #endregion Private Methods
    }
}
