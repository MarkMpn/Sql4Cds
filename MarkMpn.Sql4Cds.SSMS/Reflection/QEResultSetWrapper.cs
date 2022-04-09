using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.SSMS
{
    class QEResultSetWrapper : ReflectionObjectBase
    {
        private static readonly Type QEResultSet;

        static QEResultSetWrapper()
        {
            QEResultSet = GetType("Microsoft.SqlServer.Management.QueryExecution.QEResultSet, SQLEditors");
        }

        public QEResultSetWrapper(object obj) : base(obj)
        {
        }

        public static QEResultSetWrapper Create(IDataReader dr)
        {
            return new QEResultSetWrapper(Activator.CreateInstance(QEResultSet, dr));
        }

        public void Initialize(bool forwardOnly)
        {
            InvokeMethod(Target, nameof(Initialize), forwardOnly);
        }
    }
}
