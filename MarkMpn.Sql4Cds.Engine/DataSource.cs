﻿using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Holds all the required information about the connection to an instance
    /// </summary>
    public class DataSource
    {
        private Collation _defaultCollation;

        /// <summary>
        /// Creates a new <see cref="DataSource"/> using default values based on an existing connection.
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> that provides the connection to the instance</param>
        public DataSource(IOrganizationService org)
        {
            string name = null;

#if NETCOREAPP
            if (org is ServiceClient svc)
            {
                name = svc.ConnectedOrgUniqueName;
            }
#else
            if (org is CrmServiceClient svc)
            {
                name = svc.ConnectedOrgUniqueName;
            }
#endif
            
            if (name == null)
            {
                var orgDetails = org.RetrieveMultiple(new QueryExpression("organization") { ColumnSet = new ColumnSet("name") }).Entities[0];
                name = orgDetails.GetAttributeValue<string>("name");
            }

            Connection = org;
            Metadata = new AttributeMetadataCache(org);
            Name = name;
            TableSizeCache = new TableSizeCache(org, Metadata);
            MessageCache = new MessageCache(org, Metadata);
        }

        /// <summary>
        /// Creates a new <see cref="DataSource"/>
        /// </summary>
        public DataSource()
        {
        }

        /// <summary>
        /// The identifier for this instance
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The connection to the instance to use to retrieve or modify data
        /// </summary>
        public IOrganizationService Connection { get; set; }

        /// <summary>
        /// A cache of the metadata of the instance
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// A cache of the number of records in each table in the instance
        /// </summary>
        public ITableSizeCache TableSizeCache { get; set; }

        /// <summary>
        /// A cache of the messages that the instance supports
        /// </summary>
        public IMessageCache MessageCache { get; set; }

        /// <summary>
        /// The session token to use for Elastic table consistency
        /// </summary>
        public string SessionToken { get; set; }

        /// <summary>
        /// Returns the default collation used by this instance
        /// </summary>
        internal Collation DefaultCollation
        {
            get
            {
                if (_defaultCollation == null)
                    _defaultCollation = LoadDefaultCollation();

                return _defaultCollation;
            }
            set
            {
                _defaultCollation = value;
            }
        }

        /// <summary>
        /// Executes a request against the data source
        /// </summary>
        /// <param name="request">The request to execute</param>
        /// <returns>The response to the request</returns>
        /// <remarks>
        /// This method automatically preserves the session token for Elastic table consistency
        /// </remarks>
        internal OrganizationResponse Execute(OrganizationRequest request)
        {
            return Execute(Connection, request);
        }

        /// <summary>
        /// Executes a request against the data source
        /// </summary>
        /// <param name="org">The connection to use to execute the request</param>
        /// <param name="request">The request to execute</param>
        /// <returns>The response to the request</returns>
        /// <remarks>
        /// This method automatically preserves the session token for Elastic table consistency
        /// </remarks>
        internal OrganizationResponse Execute(IOrganizationService org, OrganizationRequest request)
        {
            if (!String.IsNullOrEmpty(SessionToken))
                request["SessionToken"] = SessionToken;

            var resp = org.Execute(request);

            if (resp.Results.TryGetValue("x-ms-session-token", out var newToken) && newToken != null)
                SessionToken = newToken.ToString();

            return resp;
        }

        private Collation LoadDefaultCollation()
        {
            var qry = new QueryExpression("organization")
            {
                ColumnSet = new ColumnSet("localeid")
            };
            var org = Connection.RetrieveMultiple(qry).Entities[0];
            var lcid = org.GetAttributeValue<int>("localeid");

            // Collation options are set based on the default language. Most are CI/AI but a few are not
            // https://learn.microsoft.com/en-us/power-platform/admin/language-collations#language-and-associated-collation-used-with-dataverse
            // On-prem databases may be configured with any default collation, but this is not exposed through any API.
            var ci = true;
            var ai = true;

            switch (lcid)
            {
                case 1035:
                case 1048:
                case 1050:
                case 1051:
                case 1053:
                case 1054:
                case 1057:
                case 1058:
                case 1061:
                case 1062:
                case 1063:
                case 1066:
                case 1069:
                case 1081:
                case 1086:
                case 1087:
                case 1110:
                case 2074:
                    ai = false;
                    break;
            }

            return new Collation(lcid, !ci, !ai);
        }
    }
}
