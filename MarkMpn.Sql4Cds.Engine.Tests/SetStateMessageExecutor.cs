using System;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    internal class SetStateMessageExecutor : IFakeMessageExecutor
    {
        public static string MessageName => "SetState";

        public bool CanExecute(OrganizationRequest request)
        {
            return request.RequestName == MessageName;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            var lookup = (EntityReference)request.Parameters["EntityMoniker"];
            var e = ctx.Data[lookup.LogicalName][lookup.Id];
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