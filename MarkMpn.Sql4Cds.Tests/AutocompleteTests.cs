using FakeXrmEasy;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Tests
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
            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(a.Attributes.Single(attr => attr.LogicalName == "primarycontactidname"), "primarycontactid");
            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForUpdate)).SetValue(c.Attributes.Single(attr => attr.LogicalName == "fullname"), false);

            _autocomplete = new Autocomplete(new[] { a, c, n }, metadata);
        }

        [TestMethod]
        public void FromClause()
        {
            var sql = "SELECT * FROM a";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "account" }, suggestions);
        }

        [TestMethod]
        public void Insert()
        {
            var sql = "INSERT ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "account", "contact", "new_customentity" }, suggestions);
        }

        [TestMethod]
        public void InsertInto()
        {
            var sql = "INSERT INTO ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "account", "contact", "new_customentity" }, suggestions);
        }

        [TestMethod]
        public void Join()
        {
            var sql = "SELECT * FROM account le";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(Array.Empty<string>(), suggestions);
        }

        /*
         * FakeXrmEasy seems to get the relationship metadata the wrong way round, so removing these tests for now
         * 
        [TestMethod]
        public void JoinClause()
        {
            var sql = "SELECT * FROM account left outer join ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).ToList();

            CollectionAssert.AreEqual(new[] { "contact ON account.accountid = contact.parentcustomerid", "account", "contact" }, suggestions);
        }

        [TestMethod]
        public void OnRelationship()
        {
            var sql = "SELECT * FROM account a left outer join contact c on ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1, out var currentLength).ToList();

            Assert.AreEqual(0, currentLength);
            Assert.AreEqual("a.accountid = c.parentcustomerid", suggestions.First());
        }
        */

        [TestMethod]
        public void OnClause()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.p";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "parentcustomerid", "parentcustomeridname" }, suggestions);
        }

        [TestMethod]
        public void UniqueAttributeName()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid where ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).Where(s => !s.Contains("(")).ToList();

            CollectionAssert.AreEqual(new[] { "a", "accountid", "c", "contactid", "employees", "firstname", "fullname", "lastname", "name", "parentcustomerid", "parentcustomeridname", "primarycontactid", "primarycontactidname", "turnover" }, suggestions);
        }

        [TestMethod]
        public void AllAttributesInEntity()
        {
            var sql = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid where a.";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "accountid", "createdon", "employees", "name", "primarycontactid", "primarycontactidname", "turnover" }, suggestions);
        }

        [TestMethod]
        public void SelectClause()
        {
            var sql = "SELECT a. FROM account a left outer join contact c on a.accountid = c.parentcustomerid";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf(".")).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "accountid", "createdon", "employees", "name", "primarycontactid", "primarycontactidname", "turnover" }, suggestions);
        }

        [TestMethod]
        public void MultipleQueries()
        {
            var sql1 = "SELECT * FROM account a left outer join contact c on a.accountid = c.parentcustomerid";
            var sql2 = "SELECT  FROM account left outer join contact on account.accountid = contact.parentcustomerid";
            var sql = sql1 + "\r\n" + sql2;
            var suggestions = _autocomplete.GetSuggestions(sql, sql1.Length + 2 + sql2.IndexOf(" ") + 1).Select(s => s.Text).Where(s => !s.Contains("(")).ToList();

            CollectionAssert.AreEqual(new[] { "account", "accountid", "contact", "contactid", "employees", "firstname", "fullname", "lastname", "name", "parentcustomerid", "parentcustomeridname", "primarycontactid", "primarycontactidname", "turnover" }, suggestions);
        }

        [TestMethod]
        public void Function()
        {
            var sql = "SELECT count( FROM account";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf("(")).Select(s => s.Text).Where(s => !s.Contains("(")).ToList();

            CollectionAssert.AreEqual(new[] { "account", "accountid", "createdon", "employees", "name", "primarycontactid", "primarycontactidname", "turnover" }, suggestions);
        }

        [TestMethod]
        public void Update()
        {
            var sql = "UPDATE ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "account", "contact", "new_customentity" }, suggestions);
        }

        [TestMethod]
        public void UpdateFrom()
        {
            var sql = "UPDATE  FROM account a";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf(" ") + 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "a" }, suggestions);
        }

        [TestMethod]
        public void Set()
        {
            var sql = "UPDATE account SET ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "accountid", "createdon", "employees", "name", "primarycontactid", "turnover" }, suggestions);
        }

        [TestMethod]
        public void Set2()
        {
            var sql = "UPDATE account SET name = 'test', ";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "accountid", "createdon", "employees", "name", "primarycontactid", "turnover" }, suggestions);
        }

        [TestMethod]
        public void Top()
        {
            var sql = "SELECT TOP 10 n FROM account";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.IndexOf("n")).Select(s => s.Text).ToList();

            CollectionAssert.AreEqual(new[] { "name" }, suggestions);
        }

        [TestMethod]
        public void InsertColumns()
        {
            var sql = "INSERT INTO account (";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).Where(s => !s.Contains("(")).ToList();

            CollectionAssert.AreEqual(new[] { "accountid", "createdon", "employees", "name", "primarycontactid", "turnover" }, suggestions);
        }

        //[TestMethod]
        public void Custom()
        {
            var sql = @"
                select count(distinct p.new_customentityid)
                from
                    new_customentity n
                    inner join new_customentity p on n";
            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            // FakeXrmEasy seems to miss the referencing attribute from the metadata, so these aren't what we'd actually like to
            // see but it's what's currently available.
            CollectionAssert.AreEqual(new[] { "n. = p.new_parentid", "n.new_parentid = p.", "n" }, suggestions);
        }

        [TestMethod]
        public void DateFilterOperator()
        {
            var sql = "SELECT * FROM account WHERE account.createdon = ";

            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.Text).ToList();

            CollectionAssert.Contains(suggestions, "today()");
            CollectionAssert.DoesNotContain(suggestions, "under(value)");
        }

        [TestMethod]
        public void GuidFilterOperator()
        {
            var sql = "SELECT * FROM account WHERE accountid = ";

            var suggestions = _autocomplete.GetSuggestions(sql, sql.Length - 1).Select(s => s.MenuText).ToList();

            CollectionAssert.DoesNotContain(suggestions, "today()");
            CollectionAssert.Contains(suggestions, "under(value)");
        }
    }
}
