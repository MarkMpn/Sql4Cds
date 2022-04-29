using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    public struct SqlDate : INullable, IComparable
    {
        private SqlDateTime _dt;

        public SqlDate(SqlDateTime dt)
        {
            _dt = dt.IsNull ? dt : dt.Value.Date;
        }

        public static readonly SqlDate Null = new SqlDate(SqlDateTime.Null);

        public bool IsNull => _dt.IsNull;

        public int CompareTo(object obj)
        {
            if (obj is SqlDate dt)
                obj = dt._dt;

            return _dt.CompareTo(obj);
        }

        public override int GetHashCode()
        {
            return _dt.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlDate dt)
                obj = dt._dt;

            return _dt.Equals(obj);
        }

        public DateTime Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDate x, SqlDate y) => x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDate x, SqlDate y) => x._dt  != y._dt;

        public static SqlBoolean operator <(SqlDate x, SqlDate y) => x._dt < y._dt;

        public static SqlBoolean operator >(SqlDate x, SqlDate y) => x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDate x, SqlDate y) => x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDate x, SqlDate y) => x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDate dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTime2(SqlDate dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTimeOffset(SqlDate dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDate(SqlDateTime dt)
        {
            return new SqlDate(dt);
        }

        public static implicit operator SqlDate(SqlDateTime2 dt)
        {
            return new SqlDate(dt);
        }

        public static implicit operator SqlDate(SqlDateTimeOffset dt)
        {
            return new SqlDate(dt);
        }

        public static implicit operator SqlDate(SqlString str)
        {
            return new SqlDate((SqlDateTime)str);
        }
    }

    public struct SqlTime : INullable, IComparable
    {
        private SqlDateTime _dt;

        public SqlTime(SqlDateTime dt)
        {
            _dt = dt.IsNull ? dt : new DateTime(1900, 1, 1).AddTicks(dt.TimeTicks * TimeSpan.TicksPerSecond / SqlDateTime.SQLTicksPerSecond);
        }

        public SqlTime(TimeSpan? ts)
        {
            _dt = ts == null ? SqlDateTime.Null : new DateTime(1900, 1, 1).Add(ts.Value);
        }

        public static readonly SqlTime Null = new SqlTime(SqlDateTime.Null);

        public bool IsNull => _dt.IsNull;

        public int CompareTo(object obj)
        {
            if (obj is SqlTime dt)
                obj = dt._dt;

            return _dt.CompareTo(obj);
        }

        public override int GetHashCode()
        {
            return _dt.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlTime dt)
                obj = dt._dt;

            return _dt.Equals(obj);
        }

        public TimeSpan Value => _dt.Value.TimeOfDay;

        public static SqlBoolean operator ==(SqlTime x, SqlTime y) => x._dt == y._dt;

        public static SqlBoolean operator !=(SqlTime x, SqlTime y) => x._dt != y._dt;

        public static SqlBoolean operator <(SqlTime x, SqlTime y) => x._dt < y._dt;

        public static SqlBoolean operator >(SqlTime x, SqlTime y) => x._dt > y._dt;

        public static SqlBoolean operator <=(SqlTime x, SqlTime y) => x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlTime x, SqlTime y) => x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlTime dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTime2(SqlTime dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTimeOffset(SqlTime dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlTime(SqlDateTime dt)
        {
            return new SqlTime(dt);
        }

        public static implicit operator SqlTime(SqlDateTime2 dt)
        {
            return new SqlTime(dt);
        }

        public static implicit operator SqlTime(SqlDateTimeOffset dt)
        {
            return new SqlTime(dt);
        }

        public static implicit operator SqlTime(SqlString str)
        {
            return new SqlTime((SqlDateTime)str);
        }
    }

    public struct SqlDateTime2 : INullable, IComparable
    {
        private SqlDateTime _dt;

        public SqlDateTime2(SqlDateTime dt)
        {
            _dt = dt;
        }

        public static readonly SqlDateTime2 Null = new SqlDateTime2(SqlDateTime.Null);

        public bool IsNull => _dt.IsNull;

        public int CompareTo(object obj)
        {
            if (obj is SqlDateTime2 dt)
                obj = dt._dt;

            return _dt.CompareTo(obj);
        }

        public override int GetHashCode()
        {
            return _dt.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlDateTime2 dt)
                obj = dt._dt;

            return _dt.Equals(obj);
        }

        public DateTime Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDateTime2 x, SqlDateTime2 y) => x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDateTime2 x, SqlDateTime2 y) => x._dt != y._dt;

        public static SqlBoolean operator <(SqlDateTime2 x, SqlDateTime2 y) => x._dt < y._dt;

        public static SqlBoolean operator >(SqlDateTime2 x, SqlDateTime2 y) => x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDateTime2 x, SqlDateTime2 y) => x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDateTime2 x, SqlDateTime2 y) => x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDateTime2 dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDate(SqlDateTime2 dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlTime(SqlDateTime2 dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTimeOffset(SqlDateTime2 dt)
        {
            return dt._dt;
        }

        public static implicit operator SqlDateTime2(SqlDateTime dt)
        {
            return new SqlDateTime2(dt);
        }

        public static implicit operator SqlDateTime2(SqlDate dt)
        {
            return new SqlDateTime2(dt);
        }

        public static implicit operator SqlDateTime2(SqlTime dt)
        {
            return new SqlDateTime2(dt);
        }

        public static implicit operator SqlDateTime2(SqlDateTimeOffset dt)
        {
            return new SqlDateTime2(dt);
        }

        public static implicit operator SqlDateTime2(SqlString str)
        {
            return new SqlDateTime2((SqlDateTime)str);
        }
    }

    public struct SqlDateTimeOffset : INullable, IComparable
    {
        private DateTimeOffset? _dt;

        public SqlDateTimeOffset(SqlDateTime dt)
        {
            _dt = dt.IsNull ? (DateTimeOffset?)null : dt.Value;
        }

        public SqlDateTimeOffset(DateTimeOffset? dt)
        {
            _dt = dt;
        }

        public static readonly SqlDateTimeOffset Null = new SqlDateTimeOffset(null);

        public bool IsNull => _dt == null;

        public int CompareTo(object obj)
        {
            if (obj is SqlDateTimeOffset dt)
                obj = dt._dt;

            if (_dt == null)
                return obj == null ? 0 : -1;

            if (obj == null)
                return 1;

            return _dt.Value.CompareTo((DateTimeOffset) obj);
        }

        public override int GetHashCode()
        {
            return _dt.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlDateTimeOffset dt)
                obj = dt._dt;

            return _dt.Equals(obj);
        }

        public DateTimeOffset Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt != y._dt;

        public static SqlBoolean operator <(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt < y._dt;

        public static SqlBoolean operator >(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDateTimeOffset x, SqlDateTimeOffset y) => x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDateTimeOffset dt)
        {
            return dt._dt == null ? SqlDateTime.Null : (SqlDateTime)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlDate(SqlDateTimeOffset dt)
        {
            return dt._dt == null ? SqlDate.Null : (SqlDate)(SqlDateTime)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlTime(SqlDateTimeOffset dt)
        {
            return dt._dt == null ? SqlTime.Null : (SqlTime)(SqlDateTime)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlDateTime2(SqlDateTimeOffset dt)
        {
            return dt._dt == null ? SqlDateTime2.Null : (SqlDateTime2)(SqlDateTime)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlDateTimeOffset(SqlDateTime dt)
        {
            return new SqlDateTimeOffset(dt);
        }

        public static implicit operator SqlDateTimeOffset(SqlDate dt)
        {
            return new SqlDateTimeOffset(dt);
        }

        public static implicit operator SqlDateTimeOffset(SqlTime dt)
        {
            return new SqlDateTimeOffset(dt);
        }

        public static implicit operator SqlDateTimeOffset(SqlDateTime2 dt)
        {
            return new SqlDateTimeOffset(dt);
        }

        public static implicit operator SqlDateTimeOffset(SqlString str)
        {
            if (str.IsNull)
                return SqlDateTimeOffset.Null;

            var dto = DateTimeOffset.Parse(str.Value);
            return new SqlDateTimeOffset(dto);
        }
    }
}
