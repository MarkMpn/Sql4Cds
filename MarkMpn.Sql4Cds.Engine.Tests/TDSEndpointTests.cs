using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class TDSEndpointTests
    {
        [DataTestMethod]
        [DataRow("org.crm.dynamics.com", "org.crm.dynamics.com")]
        [DataRow("org.crm4.dynamics.com,5558", "org.crm4.dynamics.com")]
        [DataRow("tcp:org.crm.dynamics.com,5558", "org.crm.dynamics.com")]
        [DataRow(" TCP:ORG.CRM.DYNAMICS.COM. , 5558 ", "org.crm.dynamics.com")]
        public void TryGetDataverseHostAcceptsValidDataSources(string dataSource, string expected)
        {
            Assert.IsTrue(TDSEndpoint.TryGetDataverseHost(dataSource, out var host));
            Assert.AreEqual(expected, host);
        }

        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow("localhost")]
        [DataRow("org.crm.dynamics.com,1433")]
        [DataRow("org.crm.dynamics.com,5558,5558")]
        [DataRow("np:org.crm.dynamics.com")]
        [DataRow(@"org.crm.dynamics.com\instance")]
        [DataRow("dynamics.com.example.com")]
        public void TryGetDataverseHostRejectsInvalidDataSources(string dataSource)
        {
            Assert.IsFalse(TDSEndpoint.TryGetDataverseHost(dataSource, out var host));
            Assert.IsNull(host);
        }
    }
}
