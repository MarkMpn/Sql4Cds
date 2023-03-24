using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Crm.Sdk.Messages;
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
        protected readonly IOrganizationService _service3;
        protected readonly XrmFakedContext _context3;
        protected readonly DataSource _dataSource3;
        protected readonly IDictionary<string, DataSource> _dataSources;
        protected readonly IDictionary<string, DataSource> _localDataSource;

        public FakeXrmEasyTestsBase()
        {
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());
            _context.CallerId = new EntityReference("systemuser", Guid.NewGuid());
            _context.AddFakeMessageExecutor<RetrieveVersionRequest>(new RetrieveVersionRequestExecutor());
            _context.AddGenericFakeMessageExecutor(SampleMessageExecutor.MessageName, new SampleMessageExecutor());

            _service = _context.GetOrganizationService();
            _dataSource = new DataSource { Name = "uat", Connection = _service, Metadata = new AttributeMetadataCache(_service), TableSizeCache = new StubTableSizeCache(), MessageCache = new StubMessageCache(), DefaultCollation = Collation.USEnglish };

            _context2 = new XrmFakedContext();
            _context2.InitializeMetadata(Assembly.GetExecutingAssembly());
            _context2.CallerId = _context.CallerId;
            _context2.AddFakeMessageExecutor<RetrieveVersionRequest>(new RetrieveVersionRequestExecutor());
            _context2.AddGenericFakeMessageExecutor(SampleMessageExecutor.MessageName, new SampleMessageExecutor());

            _service2 = _context2.GetOrganizationService();
            _dataSource2 = new DataSource { Name = "prod", Connection = _service2, Metadata = new AttributeMetadataCache(_service2), TableSizeCache = new StubTableSizeCache(), MessageCache = new StubMessageCache(), DefaultCollation = Collation.USEnglish };

            _context3 = new XrmFakedContext();
            _context3.InitializeMetadata(Assembly.GetExecutingAssembly());
            _context3.CallerId = _context.CallerId;
            _context3.AddFakeMessageExecutor<RetrieveVersionRequest>(new RetrieveVersionRequestExecutor());
            _context3.AddGenericFakeMessageExecutor(SampleMessageExecutor.MessageName, new SampleMessageExecutor());

            _service3 = _context3.GetOrganizationService();
            Collation.TryParse("French_CI_AI", out var frenchCIAI);
            _dataSource3 = new DataSource { Name = "french", Connection = _service3, Metadata = new AttributeMetadataCache(_service3), TableSizeCache = new StubTableSizeCache(), MessageCache = new StubMessageCache(), DefaultCollation = frenchCIAI };

            _dataSources = new[] { _dataSource, _dataSource2, _dataSource3 }.ToDictionary(ds => ds.Name);
            _localDataSource = new Dictionary<string, DataSource>
            {
                ["local"] = new DataSource { Name = "local", Connection = _service, Metadata = _dataSource.Metadata, TableSizeCache = _dataSource.TableSizeCache, MessageCache = _dataSource.MessageCache, DefaultCollation = Collation.USEnglish }
            };

            SetPrimaryIdAttributes(_context);
            SetPrimaryIdAttributes(_context2);

            SetPrimaryNameAttributes(_context);
            SetPrimaryNameAttributes(_context2);

            SetLookupTargets(_context);
            SetLookupTargets(_context2);

            SetAttributeOf(_context);
            SetAttributeOf(_context2);

            SetMaxLength(_context);
            SetMaxLength(_context2);
        }

        private void SetPrimaryNameAttributes(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                if (entity.LogicalName != "contact")
                    continue;

                // Set the primary name attribute on contact
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryNameAttribute)).SetValue(entity, "fullname");
            }
        }

        private void SetPrimaryIdAttributes(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryIdAttribute)).SetValue(entity, entity.LogicalName + "id");
                var attr = entity.Attributes.Single(a => a.LogicalName == entity.LogicalName + "id");
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsPrimaryId)).SetValue(attr, true);
                attr.RequiredLevel = new AttributeRequiredLevelManagedProperty(AttributeRequiredLevel.SystemRequired);
                context.SetEntityMetadata(entity);
            }
        }

        private void SetLookupTargets(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                var ownerAttr = (LookupAttributeMetadata)entity.Attributes.SingleOrDefault(a => a.LogicalName == "ownerid");
                if (ownerAttr != null)
                {
                    ownerAttr.Targets = new[] { "systemuser", "team" };
                    context.SetEntityMetadata(entity);
                }

                if (entity.LogicalName == "account")
                {
                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ObjectTypeCode)).SetValue(entity, 1);
                    var primaryContactId = (LookupAttributeMetadata)entity.Attributes.Single(a => a.LogicalName == "primarycontactid");
                    primaryContactId.Targets = new[] { "contact" };
                    context.SetEntityMetadata(entity);
                }

                if (entity.LogicalName != "contact")
                    continue;

                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ObjectTypeCode)).SetValue(entity, 2);

                var attr = (LookupAttributeMetadata)entity.Attributes.Single(a => a.LogicalName == "parentcustomerid");
                attr.Targets = new[] { "account", "contact" };

                var nameAttr = new StringAttributeMetadata { LogicalName = attr.LogicalName + "name" };
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(nameAttr, attr.LogicalName);

                var typeAttr = new EntityNameAttributeMetadata { LogicalName = attr.LogicalName + "type" };
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(typeAttr, attr.LogicalName);

                var attributes = entity.Attributes.Concat(new AttributeMetadata[] { nameAttr, typeAttr }).ToArray();
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity, attributes);
                context.SetEntityMetadata(entity);
            }
        }

        private void SetAttributeOf(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                if (entity.LogicalName == "new_customentity")
                {
                    var attr = entity.Attributes.Single(a => a.LogicalName == "new_optionsetvaluename");
                    typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(attr, "new_optionsetvalue");

                    var valueAttr = (EnumAttributeMetadata)entity.Attributes.Single(a => a.LogicalName == "new_optionsetvalue");
                    valueAttr.OptionSet = new OptionSetMetadata
                    {
                        Options =
                        {
                            new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value1.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value1),
                            new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value2.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value2),
                            new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value3.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value3)
                        }
                    };
                }
                else if (entity.LogicalName == "account")
                {
                    // Add metadata for primarycontactidname virtual attribute
                    var nameAttr = entity.Attributes.Single(a => a.LogicalName == "primarycontactidname");
                    nameAttr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(nameAttr, "primarycontactid");
                }
                else
                {
                    continue;
                }

                context.SetEntityMetadata(entity);
            }
        }

        private void SetMaxLength(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                foreach (var attr in entity.Attributes.OfType<StringAttributeMetadata>())
                {
                    attr.MaxLength = 100;
                    typeof(StringAttributeMetadata).GetProperty(nameof(StringAttributeMetadata.DatabaseLength)).SetValue(attr, 100);
                }

                context.SetEntityMetadata(entity);
            }
        }
    }
}
