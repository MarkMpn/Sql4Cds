using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;
using FakeXrmEasy;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions { PreserveFetchXmlOperatorsAsFunctions = false }, out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions { PreserveFetchXmlOperatorsAsFunctions = false }, out _);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions { UseParametersForLiterals = true }, out var parameters);

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

            var converted = FetchXml2Sql.Convert(metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE (firstname = 'Mark') AND (lastname = 'Carrington' OR lastname = 'Twain')", NormalizeWhitespace(converted));
        }

        private static string NormalizeWhitespace(string s)
        {
            return Regex.Replace(s, "\\s+", " ");
        }
    }
}
