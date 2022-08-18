using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Tests
{
    public class FakeXrmEasyTestsBase
    {
        protected readonly IOrganizationService _service;
        protected readonly XrmFakedContext _context;

        public FakeXrmEasyTestsBase()
        {
            _context = new XrmFakedContext();
            _context.InitializeMetadata(Assembly.GetExecutingAssembly());

            _service = _context.GetOrganizationService();

            SetLookupTargets(_context);
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
    }
}
