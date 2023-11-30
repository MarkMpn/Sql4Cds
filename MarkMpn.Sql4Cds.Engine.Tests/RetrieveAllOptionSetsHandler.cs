using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy.FakeMessageExecutors;
using FakeXrmEasy;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    class RetrieveAllOptionSetsHandler : IFakeMessageExecutor
    {
        public bool CanExecute(OrganizationRequest request)
        {
            return request is RetrieveAllOptionSetsRequest;
        }

        public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
        {
            var labels = new[]
            {
                    new LocalizedLabel("TestGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Test", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("FooGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Foo", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("BarGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Bar", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("Value1", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("Value2", 1033) { MetadataId = Guid.NewGuid() }
                };

            return new RetrieveAllOptionSetsResponse
            {
                Results = new ParameterCollection
                {
                    ["OptionSetMetadata"] = new OptionSetMetadataBase[]
                    {
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "test",
                                DisplayName = new Label(
                                    labels[0],
                                    new[]
                                    {
                                        labels[0],
                                        labels[1]
                                    })
                            },
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "foo",
                                DisplayName = new Label(
                                    labels[2],
                                    new[]
                                    {
                                        labels[2],
                                        labels[3]
                                    })
                            },
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "bar",
                                DisplayName = new Label(
                                    labels[4],
                                    new[]
                                    {
                                        labels[4],
                                        labels[5]
                                    })
                            }
                    }
                }
            };
        }

        public Type GetResponsibleRequestType()
        {
            return typeof(RetrieveAllOptionSetsRequest);
        }
    }
}
