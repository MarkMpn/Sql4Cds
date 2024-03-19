using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.XTB
{
    class XtbDataSource : DataSource
    {
        public XtbDataSource(ConnectionDetail connection, Func<ConnectionDetail, IOrganizationService> connect, IAttributeMetadataCache metadata, ITableSizeCache tableSize, IMessageCache messages) : base(connect(connection), metadata, tableSize, messages)
        {
            ConnectionDetail = connection;
            Name = connection.ConnectionName;
        }

        public ConnectionDetail ConnectionDetail { get; set; }
    }
}
