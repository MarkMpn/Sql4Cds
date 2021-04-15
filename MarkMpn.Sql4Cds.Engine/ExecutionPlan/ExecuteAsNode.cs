using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class ExecuteAsNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The column in the data source that provides the user ID to impersonate
        /// </summary>
        [Category("Execute As")]
        [Description("The column in the data source that provides the user ID to impersonate")]
        public string UserIdSource { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(UserIdSource))
                requiredColumns.Add(UserIdSource);

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override string Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            try
            {
                _timer.Resume();

                var entities = GetDmlSourceEntities(org, metadata, options, parameterTypes, parameterValues, out var schema);

                if (entities.Count == 0)
                    throw new QueryExecutionException("Cannot find user to impersonate");

                if (entities.Count > 1)
                    throw new QueryExecutionException("Ambiguous user");

                // Precompile mappings with type conversions
                var meta = metadata["systemuser"];
                var attributes = meta.Attributes.ToDictionary(a => a.LogicalName);
                var attributeAccessors = CompileColumnMappings(new Dictionary<string, string> { ["systemuserid"] = UserIdSource }, schema, attributes, DateTimeKind.Unspecified);
                var userIdAccessor = attributeAccessors["systemuserid"];

                var userId = (Guid)userIdAccessor(entities[0]);

                if (org is Microsoft.Xrm.Sdk.Client.OrganizationServiceProxy svcProxy)
                    svcProxy.CallerId = userId;
                else if (org is Microsoft.Xrm.Sdk.WebServiceClient.OrganizationWebProxyClient webProxy)
                    webProxy.CallerId = userId;
                else if (org is CrmServiceClient svc)
                    svc.CallerId = userId;
                else
                    throw new QueryExecutionException("Unexpected organization service type");

                return $"Impersonated user {userId}";
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
            finally
            {
                _timer.Pause();
            }
        }

        public override string ToString()
        {
            return "EXECUTE AS";
        }
    }
}
