using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    public class TableSizeCache : ITableSizeCache
    {
        private readonly IDictionary<string, int> _tableSize;
        private readonly IOrganizationService _org;
        private readonly Version _version;
        private readonly IAttributeMetadataCache _metadata;

        // Entites that return 0 for RetrieveTotalRecordCountRequest but actually have values, found through trial and error
        // Some of these can have large numbers of records, so returning an estimated count of 0 can lead to some very slow
        // execution plans
        private static readonly string[] _unreliableRetrieveTotalRecordCountEntities = new[]
        {
            "attribute",
            "attributeimageconfig",
            "commitment",
            "connector",
            "entity",
            "entityimageconfig",
            "entitykey",
            "entityrelationship",
            "environmentvariabledefinition",
            "environmentvariablevalue",
            "relationship"
        };

        public TableSizeCache(IOrganizationService org, IAttributeMetadataCache metadata)
        {
            _tableSize = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            _org = org;
            _metadata = metadata;

#if NETCOREAPP
            if (org is ServiceClient svc)
                _version = svc.ConnectedOrgVersion;
            else
#else
            if (org is CrmServiceClient svc)
                _version = svc.ConnectedOrgVersion;
            else
#endif

                _version = new Version(((RetrieveVersionResponse)_org.Execute(new RetrieveVersionRequest())).Version);
        }

        private bool UseRetrieveTotalRecordCountRequest => _version.Major >= 9;

        public int this[string logicalName]
        {
            get
            {
                if (_tableSize.TryGetValue(logicalName, out var count))
                    return count;

                var useFetch = true;

                if (_metadata[logicalName].DataProviderId != null)
                {
                    count = 1000;
                    useFetch = false;
                }
                else if (UseRetrieveTotalRecordCountRequest && !_unreliableRetrieveTotalRecordCountEntities.Contains(logicalName))
                {
                    try
                    {
                        count = (int)((RetrieveTotalRecordCountResponse)_org.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { logicalName } })).EntityRecordCountCollection[logicalName];
                        useFetch = false;
                    }
                    catch
                    {
                        // Some entities trigger errors with RetrieveTotalRecordCountRequest - try again using a fetch query
                    }
                }
                
                if (useFetch)
                {
                    var fetch = $@"
                        <fetch aggregate='true'>
                            <entity name='{logicalName}'>
                                <attribute name='{_metadata[logicalName].PrimaryIdAttribute}' aggregate='count' alias='count' />
                            </entity>
                        </fetch>";

                    try
                    {
                        var countEntity = _org.RetrieveMultiple(new FetchExpression(fetch)).Entities[0];
                        var countValue = countEntity.GetAttributeValue<AliasedValue>("count");
                        count = (int)countValue.Value;
                    }
                    catch (FaultException<OrganizationServiceFault> fault)
                    {
                        if (fault.Detail.ErrorCode == -2147164125)
                            count = 50000;
                        else
                            throw;
                    }
                }

                _tableSize[logicalName] = count;
                return count;
            }
        }
    }
}
