using System;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    internal class SampleMessageExecutor : IFakeMessageExecutor
    {
        public static string MessageName => "SampleMessage";

        public bool CanExecute(OrganizationRequest request)
        {
            return request.RequestName == MessageName;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            return new OrganizationResponse
            {
                Results = new ParameterCollection
                {
                    ["OutputParam1"] = request.Parameters["StringParam"],
                    ["OutputParam2"] = Int32.Parse((string) request.Parameters["StringParam"])
                }
            };
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(OrganizationRequest);
        }
    }
}