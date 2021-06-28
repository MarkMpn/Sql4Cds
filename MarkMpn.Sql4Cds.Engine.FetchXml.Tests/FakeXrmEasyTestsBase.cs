using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.FetchXml.Tests
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly XrmFakedContext _context;

        public FakeXrmEasyTestsBase()
        {
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());
            _context.AddFakeMessageExecutor<WhoAmIRequest>(new WhoAmIHandler());

            _service = _context.GetOrganizationService();
        }
    }

    class WhoAmIHandler : IFakeMessageExecutor
    {
        public static readonly Guid OrganizationId = new Guid("{79E88435-F8FA-44DD-946B-3BA82D3108E2}");
        public static readonly Guid BusinessUnitId = new Guid("{87A741C8-5344-4A8E-B594-AF5A88E0CFB8}");
        public static readonly Guid UserId = new Guid("{CE1FE91F-605C-4E84-94FB-5AD94BE5996C}");

        public bool CanExecute(OrganizationRequest request)
        {
            return true;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            return new WhoAmIResponse
            {
                Results = new ParameterCollection
                {
                    [nameof(OrganizationId)] = OrganizationId,
                    [nameof(BusinessUnitId)] = BusinessUnitId,
                    [nameof(UserId)] = UserId
                }
            };
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(WhoAmIRequest);
        }
    }
}
