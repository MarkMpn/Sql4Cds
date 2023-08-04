using System;
using System.ComponentModel;
using McTools.Xrm.Connection;

namespace MarkMpn.Sql4Cds.XTB
{
    class ConnectionPropertiesWrapper
    {
        private readonly ConnectionDetail _connection;

        public ConnectionPropertiesWrapper(ConnectionDetail connection)
        {
            _connection = connection;
        }

        [Category("Connection")]
        [DisplayName("Connection Name")]
        public string ConnectionName => _connection.ConnectionName;

        [Category("Connection")]
        [DisplayName("Connection Id")]
        public Guid? ConnectionId => _connection.ConnectionId;

        [Category("Organization")]
        [DisplayName("Organization Name")]
        public string Organization => _connection.Organization;

        [Category("Organization")]
        [DisplayName("Version")]
        public string OrganizationVersion => _connection.OrganizationVersion;

        [Category("Organization")]
        [DisplayName("URL")]
        public string OrganizationServiceUrl => _connection.OrganizationServiceUrl;
    }
}