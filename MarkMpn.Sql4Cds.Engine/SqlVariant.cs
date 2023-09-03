using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    struct SqlVariant : INullable, IComparable
    {
        private SqlVariant(bool @null)
        {
            BaseType = DataTypeHelpers.Variant;
            Value = null;
        }

        public SqlVariant(DataTypeReference baseType, INullable value)
        {
            BaseType = baseType ?? throw new ArgumentNullException(nameof(baseType));
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        public static readonly SqlVariant Null = new SqlVariant(true);

        public DataTypeReference BaseType { get; }

        public INullable Value { get; }

        public bool IsNull => Value == null || Value.IsNull;

        public int CompareTo(object obj)
        {
            if (!(obj is SqlVariant sqlVariant))
                throw new ArgumentException();
            
            // Null sorts before all other values
            if (IsNull)
            {
                if (!sqlVariant.IsNull)
                    return -1;

                return 0;
            }

            if (sqlVariant.IsNull)
                return 1;

            // sql_variant sorting is based on data type family hierarchy order
            // https://learn.microsoft.com/en-us/sql/t-sql/data-types/sql-variant-transact-sql?view=sql-server-ver16#comparing-sql_variant-values
            var family1 = BaseType.GetDataTypeFamily();
            var family2 = sqlVariant.BaseType.GetDataTypeFamily();

            // If the types are in different families, use the precedence order of the families
            if (family1 != family2)
                return -family1.CompareTo(family2);

            // Character values should be compared first based on their collations
            if (BaseType is SqlDataTypeReferenceWithCollation coll1 &&
                sqlVariant.BaseType is SqlDataTypeReferenceWithCollation coll2 &&
                !coll1.Collation.Equals(coll2.Collation))
            {
                var comparison = coll1.Collation.LCID.CompareTo(coll2.Collation.LCID);

                if (comparison == 0)
                    comparison = coll1.Collation.CompareOptions.CompareTo(coll2.Collation.CompareOptions);

                // This doesn't quite match the specification as we don't have LCID version or Sort ID available
                if (comparison != 0)
                    return comparison;
            }

            // If the types are different but in the same family, convert them to the same type based
            // on the precedence order and then compare them
            if (!SqlTypeConverter.CanMakeConsistentTypes(BaseType, sqlVariant.BaseType, null, out var consistentType))
                throw new ArgumentException();

            var value1 = SqlTypeConverter.GetConversion(BaseType, consistentType)(Value);
            var value2 = SqlTypeConverter.GetConversion(sqlVariant.BaseType, consistentType)(sqlVariant.Value);

            if (!(value1 is IComparable comparable1))
                throw new ArgumentException();

            return comparable1.CompareTo(value2);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlVariant sqlVariant))
                return false;

            if (sqlVariant.IsNull || IsNull)
            {
                if (sqlVariant.IsNull)
                    return IsNull;

                return false;
            }

            return Value.Equals(sqlVariant.Value);
        }

        public override int GetHashCode()
        {
            return IsNull ? 0 : Value.GetHashCode();
        }

        public override string ToString()
        {
            if (IsNull)
                return "Null";

            return Value.ToString();
        }

        public static SqlBoolean operator ==(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) == 0;

        public static SqlBoolean operator !=(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) != 0;

        public static SqlBoolean operator <(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) < 0;

        public static SqlBoolean operator >(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) > 0;

        public static SqlBoolean operator <=(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) <= 0;

        public static SqlBoolean operator >=(SqlVariant x, SqlVariant y) => !x.IsNull && x.CompareTo(y) >= 0;
    }
}
