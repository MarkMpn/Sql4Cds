using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class CollationTests
    {
        [DataRow("Latin1_General")] // Missing case & accent sensitivity
        [DataRow("Latin1_General_CI")] // Missing accent sensitivity
        [DataRow("Latin1_General_AI")] // Missing case sensitivity
        [DataRow("Latin1_General_CS_CI_AI")] // Conflicting case sensitivity
        [DataRow("Latin1_General_CS_AS_AI")] // Conflicting accent sensitivity
        [DataRow("Latin1_General_BIN_CS_AS")] // Binary comparision can't be combined
        [DataRow("Latin1_General_BIN2_CS_AS")] // Binary comparision can't be combined
        [DataRow("Latin2_General_CS_AS")] // Invalid name
        [DataTestMethod]
        public void InvalidCollations(string name)
        {
            Assert.IsFalse(Collation.TryParse(name, out _));
        }

        [DataRow(true, true)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(false, false)]
        [DataTestMethod]
        public void Latin1_General(bool cs, bool @as)
        {
            Assert.IsTrue(Collation.TryParse($"Latin1_General_{(cs?"CS":"CI")}_{(@as ? "AS" : "AI")}", out var coll));

            var s1 = coll.ToSqlString("hello");
            var s2 = coll.ToSqlString("Héllo");
            var s3 = coll.ToSqlString("héllo");
            var s4 = coll.ToSqlString("Hello");

            if (!cs && !@as)
                Assert.AreEqual(s1, s2);
            else
                Assert.AreNotEqual(s1, s2);

            if (!@as)
            {
                Assert.AreEqual(s1, s3);
                Assert.AreEqual(s2, s4);
            }
            else
            {
                Assert.AreNotEqual(s1, s3);
                Assert.AreNotEqual(s2, s4);
            }

            if (!cs)
            {
                Assert.AreEqual(s1, s4);
                Assert.AreEqual(s2, s3);
            }
            else
            {
                Assert.AreNotEqual(s1, s4);
                Assert.AreNotEqual(s2, s3);
            }
        }
    }
}
