using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.FetchXml.Tests
{
    [TestClass]
    public class FetchXml2SqlTests
    {
        [TestMethod]
        public void SimpleSelect()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Filter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = 'Mark'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Joins()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid'>
                            <attribute name='name' />
                        </link-entity>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname, account.name FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void JoinFilter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid'>
                            <attribute name='name' />
                            <filter>
                                <condition attribute='name' operator='eq' value='data8' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname, account.name FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid AND account.name = 'data8' WHERE contact.firstname = 'Mark'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Order()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <order attribute='firstname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact ORDER BY firstname ASC", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void OrderDescending()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <order attribute='firstname' descending='true' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact ORDER BY firstname DESC", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NoLock()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch no-lock='true'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WITH (NOLOCK)", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Top()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch top='10'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT TOP 10 firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Distinct()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT DISTINCT firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void CustomOperator()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='createdon' operator='last-x-days' value='2' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE createdon = lastxdays(2)", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void LastXDaysConversion()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='createdon' operator='last-x-days' value='2' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions { PreserveFetchXmlOperatorsAsFunctions = false }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE createdon >= '{DateTime.Today.AddDays(-2):s}' AND createdon < '{DateTime.Now:s}'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NextXYearsConversion()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='createdon' operator='next-x-years' value='2' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions { PreserveFetchXmlOperatorsAsFunctions = false }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE createdon >= '{DateTime.Now:s}' AND createdon < '{DateTime.Today.AddDays(1).AddYears(2):s}'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void ParameterConversion()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions { UseParametersForLiterals = true }, out var parameters);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = @firstname", NormalizeWhitespace(converted));
            Assert.AreEqual("Mark", parameters["@firstname"]);
        }

        [TestMethod]
        public void AndOr()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                            <filter type='or'>
                                <condition attribute='lastname' operator='eq' value='Carrington' />
                                <condition attribute='lastname' operator='eq' value='Twain' />
                            </filter>
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = 'Mark' AND (lastname = 'Carrington' OR lastname = 'Twain')", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void JoinFilterOr()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid'>
                            <filter type='or'>
                                <condition attribute='name' operator='eq' value='Data8' />
                                <condition attribute='name' operator='eq' value='Microsoft' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid AND (account.name = 'Data8' OR account.name = 'Microsoft')", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Disconnected()
        {
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                            <filter type='or'>
                                <condition attribute='lastname' operator='eq' value='Carrington' />
                                <condition attribute='lastname' operator='eq' value='Twain' />
                            </filter>
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(null, null, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = 'Mark' AND (lastname = 'Carrington' OR lastname = 'Twain')", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void EqBusinessId()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());
            context.AddFakeMessageExecutor<WhoAmIRequest>(new WhoAmIHandler());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='eq-businessid' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(org, metadata, fetch, new FetchXml2SqlOptions { PreserveFetchXmlOperatorsAsFunctions = false }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE parentcustomerid = '{WhoAmIHandler.BusinessUnitId:D}'", NormalizeWhitespace(converted));
        }

        private static string NormalizeWhitespace(string s)
        {
            return Regex.Replace(s, "\\s+", " ");
        }

        private class WhoAmIHandler : IFakeMessageExecutor
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
}
