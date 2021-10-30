using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly XrmFakedContext _context;
        protected readonly IOrganizationService _service2;
        protected readonly XrmFakedContext _context2;

        public FakeXrmEasyTestsBase()
        {
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service = _context.GetOrganizationService();

            _context2 = new XrmFakedContext();
            _context2.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service2 = _context2.GetOrganizationService();
        }
    }
}
