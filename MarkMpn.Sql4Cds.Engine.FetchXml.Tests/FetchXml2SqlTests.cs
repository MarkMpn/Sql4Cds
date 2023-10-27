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
    public class FetchXml2SqlTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void SimpleSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Filter()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = 'Mark'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Joins()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname, account.name FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void JoinFilter()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname, account.name FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid AND account.name = 'data8' WHERE contact.firstname = 'Mark'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Order()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <order attribute='firstname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact ORDER BY firstname ASC", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void OrderDescending()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <order attribute='firstname' descending='true' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact ORDER BY firstname DESC", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NoLock()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch no-lock='true'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WITH (NOLOCK)", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Top()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch top='10'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT TOP 10 firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Distinct()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT DISTINCT firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void CustomOperator()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE createdon = lastxdays(2)", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void LastXDaysConversion()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.Literals }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE createdon >= '{DateTime.Today.AddDays(-2):s}' AND createdon < '{DateTime.Now:s}'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NextXYearsConversion()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.Literals }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE createdon >= '{DateTime.Now:s}' AND createdon < '{DateTime.Today.AddDays(1).AddYears(2):s}'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void ParameterConversion()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { UseParametersForLiterals = true }, out var parameters);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = @firstname", NormalizeWhitespace(converted));
            Assert.AreEqual("Mark", parameters["@firstname"]);
        }

        [TestMethod]
        public void AndOr()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT firstname, lastname FROM contact WHERE firstname = 'Mark' AND (lastname = 'Carrington' OR lastname = 'Twain')", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void JoinFilterOr()
        {
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

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
            var metadata = new AttributeMetadataCache(_service);
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

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.Literals }, out _);

            Assert.AreEqual($"SELECT firstname, lastname FROM contact WHERE parentcustomerid = '{WhoAmIHandler.BusinessUnitId:D}'", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void EqUserId()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='ownerid' operator='eq-userid' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual("SELECT name FROM account WHERE ownerid = CURRENT_USER", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NoAttributes()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='ownerid' operator='eq-userid' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual("SELECT * FROM account WHERE ownerid = CURRENT_USER", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Archive()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch datasource='archive'>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='ownerid' operator='eq-userid' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual("SELECT name FROM archive.account WHERE ownerid = CURRENT_USER", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void ArchiveJoins()
        {
            // Not actually supported by Dataverse, but test it anyway in case it becomes supported in the future
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch datasource='archive'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid'>
                            <attribute name='name' />
                        </link-entity>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions(), out _);

            Assert.AreEqual("SELECT contact.firstname, contact.lastname, account.name FROM archive.contact INNER JOIN archive.account ON contact.parentcustomerid = account.accountid", NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Under()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='accountid' operator='under' value='E2218046-F778-42F6-A8A7-772D0653349B' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual(NormalizeWhitespace(@"
                WITH account_hierarchical(accountid) AS (
                    SELECT accountid FROM account WHERE parentaccountid = 'e2218046-f778-42f6-a8a7-772d0653349b'
                    UNION ALL
                    SELECT account.accountid FROM account INNER JOIN account_hierarchical ON account.parentaccountid = account_hierarchical.accountid
                )
                SELECT * FROM account WHERE accountid IN ( SELECT accountid FROM account_hierarchical )"), NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void EqOrUnder()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='accountid' operator='eq-or-under' value='E2218046-F778-42F6-A8A7-772D0653349B' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual(NormalizeWhitespace(@"
                WITH account_hierarchical(accountid) AS (
                    SELECT accountid FROM account WHERE accountid = 'e2218046-f778-42f6-a8a7-772d0653349b'
                    UNION ALL
                    SELECT account.accountid FROM account INNER JOIN account_hierarchical ON account.parentaccountid = account_hierarchical.accountid
                )
                SELECT * FROM account WHERE accountid IN ( SELECT accountid FROM account_hierarchical )"), NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void NotUnder()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='accountid' operator='not-under' value='E2218046-F778-42F6-A8A7-772D0653349B' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual(NormalizeWhitespace(@"
                WITH account_hierarchical(accountid) AS (
                    SELECT accountid FROM account WHERE accountid = 'e2218046-f778-42f6-a8a7-772d0653349b'
                    UNION ALL
                    SELECT account.accountid FROM account INNER JOIN account_hierarchical ON account.parentaccountid = account_hierarchical.accountid
                )
                SELECT * FROM account WHERE (accountid IS NULL OR accountid NOT IN ( SELECT accountid FROM account_hierarchical ))"), NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void Above()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='accountid' operator='above' value='E2218046-F778-42F6-A8A7-772D0653349B' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual(NormalizeWhitespace(@"
                WITH account_hierarchical(accountid, parentaccountid) AS (
                    SELECT account.accountid, account.parentaccountid FROM account INNER JOIN account AS anchor ON account.accountid = anchor.parentaccountid WHERE anchor.accountid = 'e2218046-f778-42f6-a8a7-772d0653349b'
                    UNION ALL
                    SELECT account.accountid, account.parentaccountid FROM account INNER JOIN account_hierarchical ON account.accountid = account_hierarchical.parentaccountid
                )
                SELECT * FROM account WHERE accountid IN ( SELECT accountid FROM account_hierarchical )"), NormalizeWhitespace(converted));
        }

        [TestMethod]
        public void EqOrAbove()
        {
            var metadata = new AttributeMetadataCache(_service);
            var fetch = @"
                <fetch>
                    <entity name='account'>
                        <filter>
                            <condition attribute='accountid' operator='eq-or-above' value='E2218046-F778-42F6-A8A7-772D0653349B' />
                        </filter>
                    </entity>
                </fetch>";

            var converted = FetchXml2Sql.Convert(_service, metadata, fetch, new FetchXml2SqlOptions { ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations }, out _);

            Assert.AreEqual(NormalizeWhitespace(@"
                WITH account_hierarchical(accountid, parentaccountid) AS (
                    SELECT accountid, parentaccountid FROM account WHERE accountid = 'e2218046-f778-42f6-a8a7-772d0653349b'
                    UNION ALL
                    SELECT account.accountid, account.parentaccountid FROM account INNER JOIN account_hierarchical ON account.accountid = account_hierarchical.parentaccountid
                )
                SELECT * FROM account WHERE accountid IN ( SELECT accountid FROM account_hierarchical )"), NormalizeWhitespace(converted));
        }

        private static string NormalizeWhitespace(string s)
        {
            return Regex.Replace(s, "\\s+", " ").Trim();
        }
    }
}
