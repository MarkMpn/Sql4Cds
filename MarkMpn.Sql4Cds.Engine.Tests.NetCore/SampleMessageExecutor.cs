using System;
using FakeXrmEasy;
using FakeXrmEasy.Abstractions;
using FakeXrmEasy.Abstractions.FakeMessageExecutors;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests.NetCore
{
    internal class SampleMessageExecutor : IFakeMessageExecutor
    {
        public static string MessageName => "SampleMessage";

        public bool CanExecute(OrganizationRequest request)
        {
            return request.RequestName == MessageName;
        }

        public OrganizationResponse Execute(OrganizationRequest request, IXrmFakedContext ctx)
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