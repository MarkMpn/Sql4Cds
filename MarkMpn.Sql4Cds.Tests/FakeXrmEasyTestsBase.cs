using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using FakeXrmEasy.Extensions;
using MarkMpn.Sql4Cds.Tests.Metadata;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Tests
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly XrmFakedContext _context;

        static FakeXrmEasyTestsBase()
        {
            // Microsoft.Xrm.Sdk has a reference to System.Text.Json 6.0.0.2 but the NuGet package pulls in 6.0.0.7,
            // which causes a runtime error. Redirect the assembly to the newer version.
            RedirectAssembly("System.Text.Json", new Version("6.0.0.7"), "cc7b13ffcd2ddd51");
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
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service = _context.GetOrganizationService();

            SetLookupTargets(_context);
            SetDisplayNames(_context);
        }

        private void SetDisplayNames(XrmFakedContext context)
        {
            foreach (var entity in context.CreateMetadataQuery())
            {
                if (entity.LogicalName == "new_customentity")
                {
                    entity.DisplayName = new Label("Custom Entity", 1033);
                }

                if (entity.LogicalName == "account")
                {
                    entity.DisplayName = new Label("Account", 1033);
                    var name = entity.Attributes.Single(a => a.LogicalName == "name");
                    name.DisplayName = new Label("Account Name", 1033);
                }

                if (entity.LogicalName == "contact")
                {
                    entity.DisplayName = new Label("Contact", 1033);
                    var fullname = entity.Attributes.Single(a => a.LogicalName == "fullname");
                    fullname.DisplayName = new Label("Full Name", 1033);
                }

                entity.DisplayName.UserLocalizedLabel = entity.DisplayName.LocalizedLabels.SingleOrDefault(l => l.LanguageCode == 1033);

                foreach (var attr in entity.Attributes)
                {
                    if (attr.DisplayName != null)
                        attr.DisplayName.UserLocalizedLabel = attr.DisplayName.LocalizedLabels.SingleOrDefault(l => l.LanguageCode == 1033);
                }

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

                if (entity.LogicalName == "new_customentity")
                {
                    var new_optionsetvalue = (EnumAttributeMetadata)entity.Attributes.Single(a => a.LogicalName == "new_optionsetvalue");
                    new_optionsetvalue.OptionSet = new OptionSetMetadata(
                        new OptionMetadataCollection(
                            Enum.GetValues(typeof(New_OptionSet))
                            .Cast<New_OptionSet>()
                            .Select(o => new OptionMetadata(new Label(o.ToString(), 1033), (int)o))
                            .ToList()
                            )
                        );
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
    }
}
