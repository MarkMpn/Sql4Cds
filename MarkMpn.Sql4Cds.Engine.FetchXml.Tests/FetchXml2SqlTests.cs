using System;
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

            var converted = FetchXml2Sql.Convert(metadata, fetch);

            Assert.AreEqual("SELECT firstname, lastname FROM contact", NormalizeWhitespace(converted));
        }

        private static string NormalizeWhitespace(string s)
        {
            return Regex.Replace(s, "\\s+", " ");
        }
    }
}
