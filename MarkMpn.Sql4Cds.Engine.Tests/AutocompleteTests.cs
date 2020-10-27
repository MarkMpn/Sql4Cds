using FakeXrmEasy;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class AutocompleteTests
    {
        private Autocomplete _autocomplete;

        public AutocompleteTests()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var a = metadata["account"];
            var c = metadata["contact"];
            var n = metadata["new_customentity"];

            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForUpdate)).SetValue(a.Attributes.Single(attr => attr.LogicalName == "primarycontactidname"), false);
            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForUpdate)).SetValue(c.Attributes.Single(attr => attr.LogicalName == "fullname"), false);

            _autocomplete = new Autocomplete(new[] { a, c, n }, metadata);
        }

        [TestMethod]
        public void FromClause()
        {
            var sql = "SELECT * FROM a";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(1, currentLength);
            CollectionAssert.AreEqual(new[] { "account?4" }, suggestions);
        }

        [TestMethod]
        public void Insert()
        {
            var sql = "INSERT ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(0, currentLength);
            CollectionAssert.AreEqual(new[] { "account?4", "contact?4", "new_customentity?4" }, suggestions);
        }

        [TestMethod]
        public void InsertInto()
        {
            var sql = "INSERT INTO ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(0, currentLength);
            CollectionAssert.AreEqual(new[] { "account?4", "contact?4", "new_customentity?4" }, suggestions);
        }

        [TestMethod]
        public void Join()
        {
            var sql = "SELECT * FROM account le";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(Array.Empty<string>(), suggestions);
        }

        /*
         * FakeXrmEasy seems to get the relationship metadata the wrong way round, so removing these tests for now
         * 
        [TestMethod]
        public void JoinClause()
        {
            var sql = "SELECT * FROM account left outer join ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "contact ON account.accountid = contact.parentcustomerid?19", "account?4", "contact?4" }, suggestions);
        }

        [TestMethod]
        public void OnRelationship()
        {
            var sql = "SELECT * FROM account a left outer join contact c on ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(0, currentLength);
            Assert.AreEqual("a.accountid = c.parentcustomerid?19", suggestions.First());
        }
        */

        [TestMethod]
        public void OnClause()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.p";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(1, currentLength);
            CollectionAssert.AreEqual(new[] { "parentcustomerid?9" }, suggestions);
        }

        [TestMethod]
        public void UniqueAttributeName()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid where ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "a?4", "accountid?14", "c?4", "contactid?14", "employees?8", "firstname?13", "fullname?13", "lastname?13", "name?13", "parentcustomerid?9", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void AllAttributesInEntity()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid where a.";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void SelectClause()
        {
            var sql = "SELECT a. FROM account a left outer join contact c on a.accountid = c.parentcustomerid";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf("."), out _).ToList();

            CollectionAssert.AreEqual(new[] { "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void MultipleQueries()
        {
            var sql1 = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid";
            var sql2 = "SELECT  FROM account left outer join contact on account.accountid = contact.parentcustomerid";
            var sql = sql1 + "\r\n" + sql2;
            var suggestions = _autocomplete.GetSuggestions(sql, sql1.Length + 2 + sql2.IndexOf(" ") + 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "account?4", "accountid?14", "contact?4", "contactid?14", "employees?8", "firstname?13", "fullname?13", "lastname?13", "name?13", "parentcustomerid?9", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void Function()
        {
            var sql = "SELECT count( FROM account";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf("("), out _).ToList();

            CollectionAssert.AreEqual(new[] { "account?4", "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void Update()
        {
            var sql = "UPDATE ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "account?4", "contact?4", "new_customentity?4" }, suggestions);
        }

        [TestMethod]
        public void UpdateFrom()
        {
            var sql = "UPDATE  FROM account a";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf(" ") + 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "a?4" }, suggestions);
        }

        [TestMethod]
        public void Set()
        {
            var sql = "UPDATE account SET ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void Set2()
        {
            var sql = "UPDATE account SET name = 'test', ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void Top()
        {
            var sql = "SELECT TOP 10 n FROM account";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf("n"), out _).ToList();

            CollectionAssert.AreEqual(new[] { "name?13" }, suggestions);
        }

        [TestMethod]
        public void InsertColumns()
        {
            var sql = "INSERT INTO account (";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            CollectionAssert.AreEqual(new[] { "accountid?14", "createdon?2", "employees?8", "name?13", "primarycontactid?9", "primarycontactidname?13", "turnover?0" }, suggestions);
        }

        [TestMethod]
        public void Custom()
        {
            var sql = @"
                select count(distinct p.new_customentityid)
                from
                    new_customentity n
                    inner join new_customentity p on n";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out _).ToList();

            // FakeXrmEasy seems to miss the referencing attribute from the metadata, so these aren't what we'd actually like to
            // see but it's what's currently available.
            CollectionAssert.AreEqual(new[] { "n. = p.new_parentid?18", "n.new_parentid = p.?19", "n?4" }, suggestions);
        }
    }
}
