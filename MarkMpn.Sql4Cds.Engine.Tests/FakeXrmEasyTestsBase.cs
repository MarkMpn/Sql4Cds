using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly XrmFakedContext _context;
        protected readonly DataSource _dataSource;
        protected readonly IOrganizationService _service2;
        protected readonly XrmFakedContext _context2;
        protected readonly DataSource _dataSource2;
        protected readonly IDictionary<string, DataSource> _dataSources;
        protected readonly IDictionary<string, DataSource> _localDataSource;

        public FakeXrmEasyTestsBase()
        {
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service = _context.GetOrganizationService();
            _dataSource = new DataSource { Name = "uat", Connection = _service, Metadata = new AttributeMetadataCache(_service), TableSizeCache = new StubTableSizeCache() };

            _context2 = new XrmFakedContext();
            _context2.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service2 = _context2.GetOrganizationService();
            _dataSource2 = new DataSource { Name = "prod", Connection = _service2, Metadata = new AttributeMetadataCache(_service2), TableSizeCache = new StubTableSizeCache() };

            _dataSources = new[] { _dataSource, _dataSource2 }.ToDictionary(ds => ds.Name);
            _localDataSource = new Dictionary<string, DataSource>
            {
                ["local"] = _dataSource
            };

            SetPrimaryIdAttributes(_context);
            SetPrimaryIdAttributes(_context2);
        }

        private void SetPrimaryIdAttributes(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryIdAttribute)).SetValue(entity, entity.LogicalName + "id");
                var attr = entity.Attributes.Single(a => a.LogicalName == entity.LogicalName + "id");
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsPrimaryId)).SetValue(attr, true);
                context.SetEntityMetadata(entity);
            }
        }
    }
}
