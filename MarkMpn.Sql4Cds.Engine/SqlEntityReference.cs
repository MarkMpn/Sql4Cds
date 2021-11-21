using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    public struct SqlEntityReference : INullable, IComparable
    {
        private SqlGuid _guid;

        public SqlEntityReference(string dataSource, string logicalName, SqlGuid id)
        {
            DataSource = dataSource;
            LogicalName = logicalName;
            _guid = id;
        }

        public SqlEntityReference(string dataSource, EntityReference entityReference)
        {
            DataSource = dataSource;

            if (entityReference == null)
            {
                _guid = SqlGuid.Null;
                LogicalName = null;
            }
            else
            {
                _guid = new SqlGuid(entityReference.Id);
                LogicalName = entityReference.LogicalName;
            }
        }

        public static readonly SqlEntityReference Null = new SqlEntityReference(null, null);

        public string DataSource { get; }

        public string LogicalName { get; }

        public Guid Id => _guid.Value;

        public bool IsNull => ((INullable)_guid).IsNull;

        public int CompareTo(object obj)
        {
            if (obj is SqlEntityReference er)
                obj = er._guid;

            return _guid.CompareTo(obj);
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlEntityReference er)
                obj = er._guid;

            return _guid.Equals(obj);
        }

        public override int GetHashCode()
        {
            return _guid.GetHashCode();
        }

        public override string ToString()
        {
            return _guid.ToString();
        }

        public static SqlBoolean operator ==(SqlEntityReference x, SqlGuid y) => x._guid == y;

        public static SqlBoolean operator !=(SqlEntityReference x, SqlGuid y) => x._guid != y;

        public static SqlBoolean operator <(SqlEntityReference x, SqlGuid y) => x._guid < y;

        public static SqlBoolean operator >(SqlEntityReference x, SqlGuid y) => x._guid > y;

        public static SqlBoolean operator <=(SqlEntityReference x, SqlGuid y) => x._guid <= y;

        public static SqlBoolean operator >=(SqlEntityReference x, SqlGuid y) => x._guid >= y;

        public static SqlBoolean operator ==(SqlEntityReference x, SqlEntityReference y) => x._guid == y._guid;

        public static SqlBoolean operator !=(SqlEntityReference x, SqlEntityReference y) => x._guid != y._guid;

        public static SqlBoolean operator <(SqlEntityReference x, SqlEntityReference y) => x._guid < y._guid;

        public static SqlBoolean operator >(SqlEntityReference x, SqlEntityReference y) => x._guid > y._guid;

        public static SqlBoolean operator <=(SqlEntityReference x, SqlEntityReference y) => x._guid <= y._guid;

        public static SqlBoolean operator >=(SqlEntityReference x, SqlEntityReference y) => x._guid >= y._guid;

        public static SqlBoolean operator ==(SqlGuid x, SqlEntityReference y) => x == y._guid;

        public static SqlBoolean operator !=(SqlGuid x, SqlEntityReference y) => x != y._guid;

        public static SqlBoolean operator <(SqlGuid x, SqlEntityReference y) => x < y._guid;

        public static SqlBoolean operator >(SqlGuid x, SqlEntityReference y) => x > y._guid;

        public static SqlBoolean operator <=(SqlGuid x, SqlEntityReference y) => x <= y._guid;

        public static SqlBoolean operator >=(SqlGuid x, SqlEntityReference y) => x >= y._guid;

        public static implicit operator SqlGuid(SqlEntityReference er)
        {
            return er._guid;
        }

        public static explicit operator SqlString(SqlEntityReference er)
        {
            return (SqlString) er._guid;
        }

        public static implicit operator EntityReference(SqlEntityReference er)
        {
            if (er.IsNull)
                return null;

            return new EntityReference(er.LogicalName, er._guid.Value);
        }
    }
}
