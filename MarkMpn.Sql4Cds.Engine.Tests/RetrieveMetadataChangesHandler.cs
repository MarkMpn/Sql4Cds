using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy.FakeMessageExecutors;
using FakeXrmEasy;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Data.SqlTypes;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    class RetrieveMetadataChangesHandler : IFakeMessageExecutor
    {
        private readonly IAttributeMetadataCache _metadata;

        public RetrieveMetadataChangesHandler(IAttributeMetadataCache metadata)
        {
            _metadata = metadata;
        }

        public bool CanExecute(OrganizationRequest request)
        {
            return request is RetrieveMetadataChangesRequest;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            var metadata = new EntityMetadataCollection
                {
                    _metadata["account"],
                    _metadata["contact"],
                    _metadata["new_customentity"]
                };

            foreach (var entity in metadata)
            {
                if (entity.MetadataId == null)
                    entity.MetadataId = Guid.NewGuid();

                foreach (var attribute in entity.Attributes)
                {
                    if (attribute.MetadataId == null)
                        attribute.MetadataId = Guid.NewGuid();
                }
            }

            var req = (RetrieveMetadataChangesRequest)request;
            var metadataParam = Expression.Parameter(typeof(EntityMetadata));
            var filter = ToExpression(req.Query.Criteria, metadataParam);
            var filterFunc = (Func<EntityMetadata, bool>)Expression.Lambda(filter, metadataParam).Compile();

            var result = new EntityMetadataCollection();

            foreach (var match in metadata.Where(e => filterFunc(e)))
                result.Add(match);

            var response = new RetrieveMetadataChangesResponse
            {
                Results = new ParameterCollection
                {
                    ["EntityMetadata"] = result
                }
            };

            return response;
        }

        private Expression ToExpression(MetadataFilterExpression filter, ParameterExpression param)
        {
            if (filter == null)
                return Expression.Constant(true);

            Expression expr = null;

            foreach (var condition in filter.Conditions)
            {
                var conditionExpr = ToExpression(condition, param);

                if (expr == null)
                    expr = conditionExpr;
                else if (filter.FilterOperator == LogicalOperator.And)
                    expr = Expression.AndAlso(expr, conditionExpr);
                else
                    expr = Expression.OrElse(expr, conditionExpr);
            }

            foreach (var subFilter in filter.Filters)
            {
                var filterExpr = ToExpression(subFilter, param);

                if (expr == null)
                    expr = filterExpr;
                else if (filter.FilterOperator == LogicalOperator.And)
                    expr = Expression.AndAlso(expr, filterExpr);
                else
                    expr = Expression.OrElse(expr, filterExpr);
            }

            return expr ?? Expression.Constant(true);
        }

        private Expression ToExpression(MetadataConditionExpression condition, ParameterExpression param)
        {
            var value = Expression.PropertyOrField(param, condition.PropertyName);
            var targetValue = (Expression)Expression.Constant(condition.Value);

            if (value.Type.IsGenericType &&
                value.Type.GetGenericTypeDefinition() == typeof(Nullable<>) &&
                value.Type.GetGenericArguments()[0] == targetValue.Type)
            {
                targetValue = Expression.Convert(targetValue, value.Type);
            }

            switch (condition.ConditionOperator)
            {
                case MetadataConditionOperator.Equals:
                    return Expression.Equal(value, targetValue);

                case MetadataConditionOperator.NotEquals:
                    return Expression.NotEqual(value, targetValue);

                case MetadataConditionOperator.LessThan:
                    return Expression.LessThan(value, targetValue);

                case MetadataConditionOperator.GreaterThan:
                    return Expression.GreaterThan(value, targetValue);

                default:
                    throw new NotImplementedException();
            }
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(RetrieveMetadataChangesRequest);
        }
    }
}
