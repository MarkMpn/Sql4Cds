using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    static class MetadataQueryExtensions
    {
        public static void AddFilter(this MetadataQueryExpression query, MetadataFilterExpression filter)
        {
            if (filter == null)
                return;
            
            if (query.Criteria == null || query.Criteria.Conditions.Count == 0 && query.Criteria.Filters.Count == 0)
                query.Criteria = filter;
            else if (query.Criteria.FilterOperator == LogicalOperator.And)
                query.Criteria.Filters.Add(filter);
            else
                query.Criteria = new MetadataFilterExpression { Filters = { query.Criteria, filter } };
        }

        public static EntityQueryExpression Clone(this EntityQueryExpression query)
        {
            if (query == null)
                return null;

            return new EntityQueryExpression
            {
                AttributeQuery = query.AttributeQuery.Clone(),
                Criteria = query.Criteria.Clone(),
                KeyQuery = query.KeyQuery.Clone(),
                LabelQuery = query.LabelQuery.Clone(),
                Properties = query.Properties.Clone(),
                RelationshipQuery = query.RelationshipQuery.Clone()
            };
        }

        private static AttributeQueryExpression Clone(this AttributeQueryExpression query)
        {
            if (query == null)
                return null;

            return new AttributeQueryExpression
            {
                Criteria = query.Criteria.Clone(),
                Properties = query.Properties.Clone()
            };
        }

        private static MetadataFilterExpression Clone(this MetadataFilterExpression filter)
        {
            if (filter == null)
                return null;

            var clone = new MetadataFilterExpression
            {
                FilterOperator = filter.FilterOperator,
            };

            foreach (var child in filter.Filters)
                clone.Filters.Add(child.Clone());

            foreach (var condition in filter.Conditions)
                clone.Conditions.Add(condition.Clone());

            return clone;
        }

        private static MetadataConditionExpression Clone(this MetadataConditionExpression condition)
        {
            if (condition == null)
                return null;

            return new MetadataConditionExpression
            {
                PropertyName = condition.PropertyName,
                ConditionOperator = condition.ConditionOperator,
                Value = condition.Value
            };
        }

        private static MetadataPropertiesExpression Clone(this MetadataPropertiesExpression properties)
        {
            if (properties == null)
                return null;

            return new MetadataPropertiesExpression(properties.PropertyNames.ToArray())
            {
                AllProperties = properties.AllProperties
            };
        }

        private static EntityKeyQueryExpression Clone(this EntityKeyQueryExpression query)
        {
            if (query == null)
                return null;

            return new EntityKeyQueryExpression
            {
                Criteria = query.Criteria.Clone(),
                Properties = query.Properties.Clone()
            };
        }

        private static LabelQueryExpression Clone(this LabelQueryExpression query)
        {
            if (query == null)
                return null;

            var clone = new LabelQueryExpression
            {
                MissingLabelBehavior = query.MissingLabelBehavior
            };

            foreach (var language in query.FilterLanguages)
                clone.FilterLanguages.Add(language);

            return clone;
        }

        private static RelationshipQueryExpression Clone(this RelationshipQueryExpression query)
        {
            if (query == null)
                return null;

            return new RelationshipQueryExpression
            {
                Criteria = query.Criteria.Clone(),
                Properties = query.Properties.Clone()
            };
        }
    }
}
