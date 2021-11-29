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

namespace MarkMpn.Sql4Cds.Engine
{
    public class TableSizeCache : ITableSizeCache
    {
        private readonly IDictionary<string, int> _tableSize;
        private readonly IOrganizationService _org;
        private readonly Version _version;
        private readonly IAttributeMetadataCache _metadata;

        public TableSizeCache(IOrganizationService org, IAttributeMetadataCache metadata)
        {
            _tableSize = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            _org = org;
            _metadata = metadata;

            _version = new Version(((RetrieveVersionResponse)_org.Execute(new RetrieveVersionRequest())).Version);
        }

        private bool UseRetrieveTotalRecordCountRequest => _version.Major >= 9;

        public int this[string logicalName]
        {
            get
            {
                if (_tableSize.TryGetValue(logicalName, out var count))
                    return count;

                if (_metadata[logicalName].DataProviderId != null)
                {
                    count = 1000;
                }
                else if (UseRetrieveTotalRecordCountRequest)
                {
                    count = (int) ((RetrieveTotalRecordCountResponse)_org.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { logicalName } })).EntityRecordCountCollection[logicalName];
                }
                else
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
