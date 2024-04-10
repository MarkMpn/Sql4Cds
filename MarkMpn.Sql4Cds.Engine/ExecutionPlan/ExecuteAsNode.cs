﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Net.PeerToPeer;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class ExecuteAsNode : BaseDmlNode, IImpersonateRevertExecutionPlanNode
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
        [DisplayName("UserId Source")]
        public string UserIdSource { get; set; }

        /// <summary>
        /// The column in the data source that provides the value to use to search for users
        /// </summary>
        [Category("Execute As")]
        [Description("The column in the data source that provides the value to use to search for users")]
        [DisplayName("Filter Value Source")]
        public string FilterValueSource { get; set; }

        /// <summary>
        /// The column in the data source that provides the number of matching users
        /// </summary>
        [Category("Execute As")]
        [Description("The column in the data source that provides the number of matching users")]
        [DisplayName("Count Source")]
        public string CountSource { get; set; }

        [Browsable(false)]
        public override int MaxDOP { get; set; }

        [Browsable(false)]
        public override int BatchSize { get; set; }

        [Browsable(false)]
        public override bool BypassCustomPluginExecution { get; set; }

        [Browsable(false)]
        public override bool ContinueOnError { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(UserIdSource))
                requiredColumns.Add(UserIdSource);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                using (_timer.Run())
                {
                    if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                        throw new QueryExecutionException("Missing datasource " + DataSource);

                    var entities = GetDmlSourceEntities(context, out var schema);

                    if (entities.Count != 1)
                        throw new QueryExecutionException(Sql4CdsError.ImpersonationError("(null)"), new ApplicationException($"Unexpected error retrieving user details: retrieved {entities.Count} values"));

                    var count = entities[0].GetAttributeValue<SqlInt32>(CountSource).Value;
                    var username = entities[0].GetAttributeValue<SqlString>(FilterValueSource).IsNull ? "(null)" : entities[0].GetAttributeValue<SqlString>(FilterValueSource).Value;

                    if (count == 0)
                        throw new QueryExecutionException(Sql4CdsError.ImpersonationError(username));

                    if (count > 1)
                        throw new QueryExecutionException(Sql4CdsError.ImpersonationError(username), new ApplicationException("Ambiguous username"));

                    // Precompile mappings with type conversions
                    var attributeAccessors = CompileColumnMappings(dataSource, "systemuser", new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["systemuserid"] = UserIdSource }, schema, DateTimeKind.Unspecified, entities);
                    var userIdAccessor = attributeAccessors["systemuserid"];

                    var userId = (Guid)userIdAccessor(entities[0]);

#if NETCOREAPP
                    if (dataSource.Connection is ServiceClient svc)
                        svc.CallerId = userId;
#else
                    if (dataSource.Connection is Microsoft.Xrm.Sdk.Client.OrganizationServiceProxy svcProxy)
                        svcProxy.CallerId = userId;
                    else if (dataSource.Connection is Microsoft.Xrm.Sdk.WebServiceClient.OrganizationWebProxyClient webProxy)
                        webProxy.CallerId = userId;
                    else if (dataSource.Connection is CrmServiceClient svc)
                        svc.CallerId = userId;
#endif
                    else
                        throw new QueryExecutionException("Unexpected organization service type");

                    recordsAffected = -1;
                    message = $"Impersonated user {userId}";
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        protected override void RenameSourceColumns(IDictionary<string, string> columnRenamings)
        {
            if (columnRenamings.TryGetValue(UserIdSource, out var userIdSourceRenamed))
                UserIdSource = userIdSourceRenamed;
        }

        public override string ToString()
        {
            return "EXECUTE AS";
        }

        public override object Clone()
        {
            var clone = new ExecuteAsNode
            {
                BatchSize = BatchSize,
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                ContinueOnError = ContinueOnError,
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                MaxDOP = MaxDOP,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql,
                UserIdSource = UserIdSource,
                FilterValueSource = FilterValueSource,
                CountSource = CountSource
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
