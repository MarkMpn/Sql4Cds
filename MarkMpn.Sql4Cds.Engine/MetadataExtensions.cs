using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    static class MetadataExtensions
    {
        public static Type GetAttributeType(this AttributeMetadata attrMetadata)
        {
            if (attrMetadata is MultiSelectPicklistAttributeMetadata)
                return typeof(OptionSetValueCollection);

            var typeCode = attrMetadata.AttributeType;

            if (attrMetadata is ManagedPropertyAttributeMetadata managedProp)
                typeCode = managedProp.ValueAttributeTypeCode;

            if (attrMetadata is BooleanAttributeMetadata || typeCode == AttributeTypeCode.Boolean)
                return typeof(bool?);

            if (attrMetadata is DateTimeAttributeMetadata || typeCode == AttributeTypeCode.DateTime)
                return typeof(DateTime?);

            if (attrMetadata is DecimalAttributeMetadata || typeCode == AttributeTypeCode.Decimal)
                return typeof(decimal?);

            if (attrMetadata is DoubleAttributeMetadata || typeCode == AttributeTypeCode.Double)
                return typeof(double?);

            if (attrMetadata is EntityNameAttributeMetadata || typeCode == AttributeTypeCode.EntityName)
                return typeof(int?);

            if (attrMetadata is ImageAttributeMetadata)
                return typeof(byte[]);

            if (attrMetadata is IntegerAttributeMetadata || typeCode == AttributeTypeCode.Integer)
                return typeof(int?);

            if (attrMetadata is BigIntAttributeMetadata || typeCode == AttributeTypeCode.BigInt)
                return typeof(long?);

            if (typeCode == AttributeTypeCode.PartyList)
                return typeof(EntityCollection);

            if (attrMetadata is LookupAttributeMetadata || typeCode == AttributeTypeCode.Lookup || typeCode == AttributeTypeCode.Customer || typeCode == AttributeTypeCode.Owner)
                return typeof(Guid?);

            if (attrMetadata is MemoAttributeMetadata || typeCode == AttributeTypeCode.Memo)
                return typeof(string);

            if (attrMetadata is MoneyAttributeMetadata || typeCode == AttributeTypeCode.Money)
                return typeof(decimal?);

            if (attrMetadata is PicklistAttributeMetadata || typeCode == AttributeTypeCode.Picklist)
                return typeof(int?);

            if (attrMetadata is StateAttributeMetadata || typeCode == AttributeTypeCode.State)
                return typeof(int?);

            if (attrMetadata is StatusAttributeMetadata || typeCode == AttributeTypeCode.Status)
                return typeof(int?);

            if (attrMetadata is StringAttributeMetadata || typeCode == AttributeTypeCode.String)
                return typeof(string);

            if (attrMetadata is UniqueIdentifierAttributeMetadata || typeCode == AttributeTypeCode.Uniqueidentifier)
                return typeof(Guid?);

            if (attrMetadata.AttributeType == AttributeTypeCode.Virtual)
                return typeof(string);

            throw new ApplicationException("Unknown attribute type " + attrMetadata.GetType());
        }

        public static Type GetAttributeSqlType(this AttributeMetadata attrMetadata)
        {
            if (attrMetadata is MultiSelectPicklistAttributeMetadata)
                return typeof(SqlString);

            var typeCode = attrMetadata.AttributeType;

            if (attrMetadata is ManagedPropertyAttributeMetadata managedProp)
                typeCode = managedProp.ValueAttributeTypeCode;

            if (attrMetadata is BooleanAttributeMetadata || typeCode == AttributeTypeCode.Boolean)
                return typeof(SqlBoolean);

            if (attrMetadata is DateTimeAttributeMetadata || typeCode == AttributeTypeCode.DateTime)
                return typeof(SqlDateTime);

            if (attrMetadata is DecimalAttributeMetadata || typeCode == AttributeTypeCode.Decimal)
                return typeof(SqlDecimal);

            if (attrMetadata is DoubleAttributeMetadata || typeCode == AttributeTypeCode.Double)
                return typeof(SqlDouble);

            if (attrMetadata is EntityNameAttributeMetadata || typeCode == AttributeTypeCode.EntityName)
                return typeof(SqlString);

            if (attrMetadata is ImageAttributeMetadata)
                return typeof(SqlBinary);

            if (attrMetadata is IntegerAttributeMetadata || typeCode == AttributeTypeCode.Integer)
                return typeof(SqlInt32);

            if (attrMetadata is BigIntAttributeMetadata || typeCode == AttributeTypeCode.BigInt)
                return typeof(SqlInt64);

            if (typeCode == AttributeTypeCode.PartyList)
                return typeof(SqlString);

            if (attrMetadata is LookupAttributeMetadata || attrMetadata.IsPrimaryId == true || typeCode == AttributeTypeCode.Lookup || typeCode == AttributeTypeCode.Customer || typeCode == AttributeTypeCode.Owner)
                return typeof(SqlEntityReference);

            if (attrMetadata is MemoAttributeMetadata || typeCode == AttributeTypeCode.Memo)
                return typeof(SqlString);

            if (attrMetadata is MoneyAttributeMetadata || typeCode == AttributeTypeCode.Money)
                return typeof(SqlMoney);

            if (attrMetadata is PicklistAttributeMetadata || typeCode == AttributeTypeCode.Picklist)
                return typeof(SqlInt32);

            if (attrMetadata is StateAttributeMetadata || typeCode == AttributeTypeCode.State)
                return typeof(SqlInt32);

            if (attrMetadata is StatusAttributeMetadata || typeCode == AttributeTypeCode.Status)
                return typeof(SqlInt32);

            if (attrMetadata is StringAttributeMetadata || typeCode == AttributeTypeCode.String)
                return typeof(SqlString);

            if (attrMetadata is UniqueIdentifierAttributeMetadata || typeCode == AttributeTypeCode.Uniqueidentifier)
                return typeof(SqlGuid);

            if (attrMetadata.AttributeType == AttributeTypeCode.Virtual)
                return typeof(SqlString);

            throw new ApplicationException("Unknown attribute type " + attrMetadata.GetType());
        }
    }
}
