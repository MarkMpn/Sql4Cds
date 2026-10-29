using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Converts <see cref="QueryExpression"/> and <see cref="QueryByAttribute"/> queries to FetchXML so they can
    /// be executed through the Web API
    /// </summary>
    public static class QueryExpressionConverter
    {
        private static readonly Dictionary<ConditionOperator, string> _operators = new Dictionary<ConditionOperator, string>
        {
            [ConditionOperator.Equal] = "eq",
            [ConditionOperator.NotEqual] = "ne",
            [ConditionOperator.GreaterThan] = "gt",
            [ConditionOperator.GreaterEqual] = "ge",
            [ConditionOperator.LessThan] = "lt",
            [ConditionOperator.LessEqual] = "le",
            [ConditionOperator.Like] = "like",
            [ConditionOperator.NotLike] = "not-like",
            [ConditionOperator.In] = "in",
            [ConditionOperator.NotIn] = "not-in",
            [ConditionOperator.Between] = "between",
            [ConditionOperator.NotBetween] = "not-between",
            [ConditionOperator.Null] = "null",
            [ConditionOperator.NotNull] = "not-null",
            [ConditionOperator.Last7Days] = "last-seven-days",
            [ConditionOperator.Next7Days] = "next-seven-days",
            [ConditionOperator.EqualUserId] = "eq-userid",
            [ConditionOperator.NotEqualUserId] = "ne-userid",
            [ConditionOperator.EqualBusinessId] = "eq-businessid",
            [ConditionOperator.NotEqualBusinessId] = "ne-businessid",
            [ConditionOperator.EqualUserLanguage] = "eq-userlanguage",
            [ConditionOperator.OlderThanXMonths] = "olderthan-x-months",
            [ConditionOperator.OlderThanXYears] = "olderthan-x-years",
            [ConditionOperator.OlderThanXWeeks] = "olderthan-x-weeks",
            [ConditionOperator.OlderThanXDays] = "olderthan-x-days",
            [ConditionOperator.OlderThanXHours] = "olderthan-x-hours",
            [ConditionOperator.OlderThanXMinutes] = "olderthan-x-minutes",
            [ConditionOperator.BeginsWith] = "begins-with",
            [ConditionOperator.DoesNotBeginWith] = "not-begin-with",
            [ConditionOperator.EndsWith] = "ends-with",
            [ConditionOperator.DoesNotEndWith] = "not-end-with",
            [ConditionOperator.EqualUserOrUserHierarchy] = "eq-useroruserhierarchy",
            [ConditionOperator.EqualUserOrUserHierarchyAndTeams] = "eq-useroruserhierarchyandteams",
            [ConditionOperator.EqualUserTeams] = "eq-userteams",
            [ConditionOperator.EqualUserOrUserTeams] = "eq-useroruserteams",
            [ConditionOperator.UnderOrEqual] = "eq-or-under",
            [ConditionOperator.AboveOrEqual] = "eq-or-above",
            [ConditionOperator.ContainValues] = "contain-values",
            [ConditionOperator.DoesNotContainValues] = "not-contain-values",
            [ConditionOperator.Contains] = "like",
            [ConditionOperator.DoesNotContain] = "not-like",
            [ConditionOperator.Mask] = "mask",
            [ConditionOperator.NotMask] = "not-mask",
            [ConditionOperator.MasksSelect] = "masks-select",
        };

        private static readonly HashSet<ConditionOperator> _multiValueOperators = new HashSet<ConditionOperator>
        {
            ConditionOperator.In,
            ConditionOperator.NotIn,
            ConditionOperator.Between,
            ConditionOperator.NotBetween,
            ConditionOperator.ContainValues,
            ConditionOperator.DoesNotContainValues,
            ConditionOperator.InFiscalPeriodAndYear,
            ConditionOperator.InOrBeforeFiscalPeriodAndYear,
            ConditionOperator.InOrAfterFiscalPeriodAndYear,
        };

        /// <summary>
        /// Converts a <see cref="QueryBase"/> to FetchXML
        /// </summary>
        public static string ToFetchXml(QueryBase query)
        {
            switch (query)
            {
                case QueryExpression qe:
                    return ToFetchXml(qe);

                case QueryByAttribute qba:
                    return ToFetchXml(qba);

                case FetchExpression fe:
                    return fe.Query;

                default:
                    throw new NotSupportedException($"Unsupported query type {query?.GetType().Name}");
            }
        }

        public static string ToFetchXml(QueryByAttribute query)
        {
            var qe = new QueryExpression(query.EntityName)
            {
                ColumnSet = query.ColumnSet,
                PageInfo = query.PageInfo,
                TopCount = query.TopCount
            };

            for (var i = 0; i < query.Attributes.Count; i++)
                qe.Criteria.AddCondition(query.Attributes[i], ConditionOperator.Equal, query.Values[i]);

            foreach (var order in query.Orders)
                qe.Orders.Add(order);

            return ToFetchXml(qe);
        }

        public static string ToFetchXml(QueryExpression query)
        {
            var fetch = new XElement("fetch");

            if (query.Distinct)
                fetch.SetAttributeValue("distinct", "true");

            if (query.NoLock)
                fetch.SetAttributeValue("no-lock", "true");

            if (query.TopCount != null)
            {
                fetch.SetAttributeValue("top", query.TopCount.Value.ToString(CultureInfo.InvariantCulture));
            }
            else if (query.PageInfo != null)
            {
                if (query.PageInfo.Count > 0)
                    fetch.SetAttributeValue("count", query.PageInfo.Count.ToString(CultureInfo.InvariantCulture));

                if (query.PageInfo.PageNumber > 0)
                    fetch.SetAttributeValue("page", query.PageInfo.PageNumber.ToString(CultureInfo.InvariantCulture));

                if (!String.IsNullOrEmpty(query.PageInfo.PagingCookie))
                    fetch.SetAttributeValue("paging-cookie", query.PageInfo.PagingCookie);

                if (query.PageInfo.ReturnTotalRecordCount)
                    fetch.SetAttributeValue("returntotalrecordcount", "true");
            }

            var entity = new XElement("entity", new XAttribute("name", query.EntityName));
            fetch.Add(entity);

            AddColumns(entity, query.ColumnSet);

            foreach (var order in query.Orders)
                entity.Add(ConvertOrder(order));

            AddFilter(entity, query.Criteria);

            foreach (var link in query.LinkEntities)
                entity.Add(ConvertLinkEntity(link));

            return fetch.ToString(SaveOptions.DisableFormatting);
        }

        private static void AddColumns(XElement element, ColumnSet columns)
        {
            if (columns == null)
                return;

            if (columns.AllColumns)
            {
                element.Add(new XElement("all-attributes"));
                return;
            }

            foreach (var col in columns.Columns)
                element.Add(new XElement("attribute", new XAttribute("name", col)));

            foreach (var col in columns.AttributeExpressions)
            {
                var attr = new XElement("attribute", new XAttribute("name", col.AttributeName));

                if (!String.IsNullOrEmpty(col.Alias))
                    attr.SetAttributeValue("alias", col.Alias);

                if (col.HasGroupBy)
                    attr.SetAttributeValue("groupby", "true");

                if (col.AggregateType != XrmAggregateType.None)
                    attr.SetAttributeValue("aggregate", col.AggregateType.ToString().ToLowerInvariant());

                if (col.DateTimeGrouping != XrmDateTimeGrouping.None)
                    attr.SetAttributeValue("dategrouping", col.DateTimeGrouping.ToString().ToLowerInvariant());

                element.Add(attr);
            }
        }

        private static XElement ConvertOrder(OrderExpression order)
        {
            var element = new XElement("order");

            if (!String.IsNullOrEmpty(order.Alias))
                element.SetAttributeValue("alias", order.Alias);
            else
                element.SetAttributeValue("attribute", order.AttributeName);

            if (!String.IsNullOrEmpty(order.EntityName))
                element.SetAttributeValue("entityname", order.EntityName);

            if (order.OrderType == OrderType.Descending)
                element.SetAttributeValue("descending", "true");

            return element;
        }

        private static XElement ConvertLinkEntity(LinkEntity link)
        {
            var element = new XElement("link-entity",
                new XAttribute("name", link.LinkToEntityName),
                new XAttribute("from", link.LinkToAttributeName),
                new XAttribute("to", link.LinkFromAttributeName));

            if (!String.IsNullOrEmpty(link.EntityAlias))
                element.SetAttributeValue("alias", link.EntityAlias);

            element.SetAttributeValue("link-type", ConvertJoinOperator(link.JoinOperator));

            AddColumns(element, link.Columns);

            foreach (var order in link.Orders)
                element.Add(ConvertOrder(order));

            AddFilter(element, link.LinkCriteria);

            foreach (var child in link.LinkEntities)
                element.Add(ConvertLinkEntity(child));

            return element;
        }

        private static string ConvertJoinOperator(JoinOperator op)
        {
            switch (op)
            {
                case JoinOperator.Inner:
                    return "inner";

                case JoinOperator.LeftOuter:
                    return "outer";

                case JoinOperator.Natural:
                    return "natural";

                case JoinOperator.MatchFirstRowUsingCrossApply:
                    return "matchfirstrowusingcrossapply";

                default:
                    return ToKebabCase(op.ToString());
            }
        }

        private static void AddFilter(XElement parent, FilterExpression filter)
        {
            if (filter == null || (filter.Conditions.Count == 0 && filter.Filters.Count == 0))
                return;

            parent.Add(ConvertFilter(filter));
        }

        private static XElement ConvertFilter(FilterExpression filter)
        {
            var element = new XElement("filter", new XAttribute("type", filter.FilterOperator == LogicalOperator.Or ? "or" : "and"));

            foreach (var condition in filter.Conditions)
                element.Add(ConvertCondition(condition));

            foreach (var child in filter.Filters)
            {
                if (child.Conditions.Count > 0 || child.Filters.Count > 0)
                    element.Add(ConvertFilter(child));
            }

            return element;
        }

        private static XElement ConvertCondition(ConditionExpression condition)
        {
            var element = new XElement("condition");

            if (!String.IsNullOrEmpty(condition.EntityName))
                element.SetAttributeValue("entityname", condition.EntityName);

            element.SetAttributeValue("attribute", condition.AttributeName);
            element.SetAttributeValue("operator", ConvertOperator(condition.Operator));

            var values = condition.Values.SelectMany(Flatten).ToList();

            if (condition.CompareColumns)
            {
                if (values.Count > 0)
                    element.SetAttributeValue("valueof", Convert.ToString(values[0], CultureInfo.InvariantCulture));

                return element;
            }

            var likeWrap = condition.Operator == ConditionOperator.Contains || condition.Operator == ConditionOperator.DoesNotContain;

            if (_multiValueOperators.Contains(condition.Operator))
            {
                foreach (var value in values)
                    element.Add(new XElement("value", FormatValue(value)));
            }
            else if (values.Count == 1)
            {
                var formatted = FormatValue(values[0]);

                if (likeWrap)
                    formatted = "%" + formatted + "%";

                element.SetAttributeValue("value", formatted);
            }
            else if (values.Count > 1)
            {
                foreach (var value in values)
                    element.Add(new XElement("value", FormatValue(value)));
            }

            return element;
        }

        private static IEnumerable<object> Flatten(object value)
        {
            if (value is string || value is byte[] || !(value is IEnumerable enumerable))
            {
                yield return value;
                yield break;
            }

            foreach (var item in enumerable)
                yield return item;
        }

        private static string ConvertOperator(ConditionOperator op)
        {
            if (_operators.TryGetValue(op, out var fetchOp))
                return fetchOp;

            return ToKebabCase(op.ToString());
        }

        private static string ToKebabCase(string name)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < name.Length; i++)
            {
                if (i > 0 && Char.IsUpper(name[i]))
                    sb.Append('-');

                sb.Append(Char.ToLowerInvariant(name[i]));
            }

            return sb.ToString();
        }

        /// <summary>
        /// Formats a value for use in a FetchXML condition
        /// </summary>
        public static string FormatValue(object value)
        {
            switch (value)
            {
                case null:
                    return "";

                case Guid g:
                    return g.ToString("D");

                case bool b:
                    return b ? "1" : "0";

                case DateTime dt:
                    return WebApiJson.FormatDateTime(dt, false);

                case OptionSetValue osv:
                    return osv.Value.ToString(CultureInfo.InvariantCulture);

                case EntityReference er:
                    return er.Id.ToString("D");

                case Money m:
                    return m.Value.ToString(CultureInfo.InvariantCulture);

                case Enum e:
                    return Convert.ToInt32(e, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

                case IFormattable f:
                    return f.ToString(null, CultureInfo.InvariantCulture);

                default:
                    return value.ToString();
            }
        }
    }
}
