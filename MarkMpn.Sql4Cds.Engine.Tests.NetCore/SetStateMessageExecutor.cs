using System;
using FakeXrmEasy;
using FakeXrmEasy.Abstractions;
using FakeXrmEasy.Abstractions.FakeMessageExecutors;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests.NetCore
{
    internal class SetStateMessageExecutor : IFakeMessageExecutor
    {
        public static string MessageName => "SetState";

        public bool CanExecute(OrganizationRequest request)
        {
            return request.RequestName == MessageName;
        }

        public OrganizationResponse Execute(OrganizationRequest request, IXrmFakedContext ctx)
        {
            var lookup = (EntityReference)request.Parameters["EntityMoniker"];
            var e = ctx.GetEntityById(lookup.LogicalName, lookup.Id);
            e["statecode"] = ((OptionSetValue)request.Parameters["State"]).Value;
            e["statuscode"] = ((OptionSetValue)request.Parameters["Status"]).Value;
            return new OrganizationResponse();
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(OrganizationRequest);
        }
    }
}