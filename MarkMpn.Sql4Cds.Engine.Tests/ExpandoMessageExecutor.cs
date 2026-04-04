using System;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    internal class ExpandoMessageExecutor : IFakeMessageExecutor
    {
        public static string MessageName => "ExpandoMessage";

        public bool CanExecute(OrganizationRequest request)
        {
            return request.RequestName == MessageName;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            var user1 = new Entity
            {
                ["OneDriveRoot_Error"] = false,
                ["OneDriveMSFP_Error"] = false,
                ["user"] = new Entity("systemuser", new Guid("a81ca1d0-0b4f-ef11-a316-000d3a0cd126"))
                {
                    ["__DisplayName__"] = "John Doe"
                }
            };

            var user2 = new Entity
            {
                ["OneDriveRoot_Error"] = false,
                ["OneDriveMSFP_Error"] = false,
                ["user"] = new Entity("systemuser", new Guid("450b303e-ff46-ef11-a317-0022481b5eda"))
                {
                    ["__DisplayName__"] = "Jane Doe"
                }
            };

            var results = new EntityCollection(new[] { user1, user2 });

            return new OrganizationResponse
            {
                Results = new ParameterCollection
                {
                    ["Results"] = results
                }
            };
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(OrganizationRequest);
        }
    }
}
