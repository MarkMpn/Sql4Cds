using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.Abstractions;
using FakeXrmEasy.Extensions;
using FakeXrmEasy.FakeMessageExecutors;
using FakeXrmEasy.Middleware;
using FakeXrmEasy.Middleware.Crud;
using FakeXrmEasy.Middleware.Messages;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Testing.Platform.Extensions.TestFramework;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Tests.NetCore
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly IXrmFakedContext _context;
        protected readonly FakeXrmDataSource _dataSource;
        protected readonly IOrganizationService _service2;
        protected readonly IXrmFakedContext _context2;
        protected readonly FakeXrmDataSource _dataSource2;
        protected readonly IOrganizationService _service3;
        protected readonly IXrmFakedContext _context3;
        protected readonly FakeXrmDataSource _dataSource3;
        protected readonly IDictionary<string, DataSource> _dataSources;
        protected readonly FakeXrmDataSource _localDataSource;
        protected readonly IDictionary<string, DataSource> _localDataSources;

        static FakeXrmEasyTestsBase()
        {
            // Microsoft.Xrm.Sdk has a reference to System.Text.Json 6.0.0.2 but the NuGet package pulls in 6.0.0.7,
            // which causes a runtime error. Redirect the assembly to the newer version.
            //RedirectAssembly("System.Text.Json", new Version("6.0.0.7"), "cc7b13ffcd2ddd51");
        }

        // https://stackoverflow.com/questions/5646306/is-it-possible-to-create-a-binding-redirect-at-runtime
        private static void RedirectAssembly(string shortName, Version targetVersion, string publicKeyToken)
        {
            ResolveEventHandler handler = null;

            handler = (sender, args) => {
                // Use latest strong name & version when trying to load SDK assemblies
                var requestedAssembly = new AssemblyName(args.Name);
                if (requestedAssembly.Name != shortName)
                    return null;

                Debug.WriteLine("Redirecting assembly load of " + args.Name
                              + ",\tloaded by " + (args.RequestingAssembly == null ? "(unknown)" : args.RequestingAssembly.FullName));

                requestedAssembly.Version = targetVersion;
                requestedAssembly.SetPublicKeyToken(new AssemblyName("x, PublicKeyToken=" + publicKeyToken).GetPublicKeyToken());
                requestedAssembly.CultureInfo = CultureInfo.InvariantCulture;

                AppDomain.CurrentDomain.AssemblyResolve -= handler;

                return Assembly.Load(requestedAssembly);
            };
            AppDomain.CurrentDomain.AssemblyResolve += handler;
        }

        public FakeXrmEasyTestsBase()
        {
            _context = CreateContext();
            AddOrganization(_context, "uat", 1033);

            _service = _context.GetOrganizationService();
            _dataSource = new FakeXrmDataSource(_service, new AttributeMetadataCache(_service), new StubTableSizeCache(), new StubMessageCache());

            _context2 = CreateContext();
            AddOrganization(_context2, "prod", 1033);

            _service2 = _context2.GetOrganizationService();
            _dataSource2 = new FakeXrmDataSource(_service2, new AttributeMetadataCache(_service2), new StubTableSizeCache(), new StubMessageCache());

            _context3 = CreateContext();
            AddOrganization(_context3, "french", 1036);

            _service3 = _context3.GetOrganizationService();
            _dataSource3 = new FakeXrmDataSource(_service3, new AttributeMetadataCache(_service3), new StubTableSizeCache(), new StubMessageCache());

            _dataSources = new[] { _dataSource, _dataSource2, _dataSource3 }.ToDictionary(ds => ds.Name, ds => (DataSource)ds);

            _localDataSource = new FakeXrmDataSource(_service, new AttributeMetadataCache(_service), new StubTableSizeCache(), new StubMessageCache()) { Name = "local" };
            _localDataSources = new Dictionary<string, DataSource>
            {
                ["local"] = _localDataSource
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

            AddVersionNumberAttribute(_context);
            AddVersionNumberAttribute(_context2);

            SetColumnNumber(_context);
            SetColumnNumber(_context2);

            SetRelationships(_context);
            SetRelationships(_context2);
        }

        private IXrmFakedContext CreateContext()
        {
            var context = MiddlewareBuilder
                .New()
                .AddCrud()
                .UseCrud()
                .AddFakeMessageExecutor<RetrieveVersionRequest>(new RetrieveVersionRequestExecutor())
                .AddFakeMessageExecutor<RetrieveAllOptionSetsRequest>(new RetrieveAllOptionSetsHandler())
                .AddGenericFakeMessageExecutor(SampleMessageExecutor.MessageName, new SampleMessageExecutor())
                .AddGenericFakeMessageExecutor(SetStateMessageExecutor.MessageName, new SetStateMessageExecutor())
                .AddGenericFakeMessageExecutor(ExpandoMessageExecutor.MessageName, new ExpandoMessageExecutor())
                .UseMessages()
                .SetLicense(FakeXrmEasy.Abstractions.Enums.FakeXrmEasyLicense.RPL_1_5)
                .Build();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());
            return context;
        }

        private void AddOrganization(IXrmFakedContext context, string name, int lcid)
        {
            var org = new Entity("organization")
            {
                Id = Guid.NewGuid(),
                ["name"] = name,
                ["localeid"] = lcid
            };
            org["organizationid"] = org.Id;

            context.Initialize(org);
        }

        private void SetPrimaryNameAttributes(IXrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                if (entity.LogicalName != "contact")
                    continue;

                // Set the primary name attribute on contact
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryNameAttribute)).SetValue(entity, "fullname");
            }
        }

        private void AddVersionNumberAttribute(IXrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                var attributes = entity.Attributes
                    .Concat(new AttributeMetadata[]
                    {
                        new BigIntAttributeMetadata
                        {
                            LogicalName = "versionnumber",
                        }
                    })
                    .ToArray();

                entity.SetSealedPropertyValue(nameof(entity.Attributes), attributes);
                context.SetEntityMetadata(entity);
            }
        }

        private void SetPrimaryIdAttributes(IXrmFakedContext context)
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

        private void SetLookupTargets(IXrmFakedContext context)
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
                    var parentAccountId = (LookupAttributeMetadata)entity.Attributes.Single(a => a.LogicalName == "parentaccountid");
                    parentAccountId.Targets = new[] { "account" };
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

        private void SetAttributeOf(IXrmFakedContext context)
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

                    // Add metadata for owneridname virtual attribute
                    nameAttr = entity.Attributes.Single(a => a.LogicalName == "owneridname");
                    nameAttr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(nameAttr, "ownerid");
                }
                else
                {
                    continue;
                }

                context.SetEntityMetadata(entity);
            }
        }

        private void SetMaxLength(IXrmFakedContext context)
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

        private void SetColumnNumber(IXrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                var index = 0;

                foreach (var attr in entity.Attributes)
                    typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.ColumnNumber)).SetValue(attr, index++);

                context.SetEntityMetadata(entity);
            }
        }

        private void SetRelationships(IXrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                if (entity.OneToManyRelationships == null)
                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.OneToManyRelationships)).SetValue(entity, Array.Empty<OneToManyRelationshipMetadata>());

                if (entity.ManyToOneRelationships == null)
                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ManyToOneRelationships)).SetValue(entity, Array.Empty<OneToManyRelationshipMetadata>());

                if (entity.LogicalName == "account")
                {
                    // Add parentaccountid relationship
                    var relationship = new OneToManyRelationshipMetadata
                    {
                        SchemaName = "account_parentaccount",
                        ReferencedEntity = "account",
                        ReferencedAttribute = "accountid",
                        ReferencingEntity = "account",
                        ReferencingAttribute = "parentaccountid",
                        IsHierarchical = true
                    };

                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.OneToManyRelationships)).SetValue(entity, entity.OneToManyRelationships.Concat(new[] { relationship }).ToArray());
                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ManyToOneRelationships)).SetValue(entity, entity.ManyToOneRelationships.Concat(new[] { relationship }).ToArray());
                }

                context.SetEntityMetadata(entity);
            }
        }
    }
}
