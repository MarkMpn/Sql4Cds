using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    static class MetadataExtensions
    {
        public static int EntityLogicalNameMaxLength { get; } = 50;

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

        public static DataTypeReference GetAttributeSqlType(this AttributeMetadata attrMetadata)
        {
            if (attrMetadata is MultiSelectPicklistAttributeMetadata)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new MaxLiteral() } };

            var typeCode = attrMetadata.AttributeType;

            if (attrMetadata is ManagedPropertyAttributeMetadata managedProp)
                typeCode = managedProp.ValueAttributeTypeCode;

            if (attrMetadata is BooleanAttributeMetadata || typeCode == AttributeTypeCode.Boolean)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Bit };

            if (attrMetadata is DateTimeAttributeMetadata || typeCode == AttributeTypeCode.DateTime)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime };

            if (attrMetadata is DecimalAttributeMetadata || typeCode == AttributeTypeCode.Decimal)
            {
                var scale = 2;

                if (attrMetadata is DecimalAttributeMetadata dec && dec.Precision != null)
                    scale = dec.Precision.Value; // Precision property is actually scale (number of decimal places)

                var precision = 12 + scale; // Max value is 100 Billion, which is 12 digits
                
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Decimal, Parameters = { new IntegerLiteral { Value = precision.ToString(CultureInfo.InvariantCulture) }, new IntegerLiteral { Value = scale.ToString(CultureInfo.InvariantCulture) } } };
            }

            if (attrMetadata is DoubleAttributeMetadata || typeCode == AttributeTypeCode.Double)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Float };

            if (attrMetadata is EntityNameAttributeMetadata || typeCode == AttributeTypeCode.EntityName)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new IntegerLiteral { Value = "50" } } };

            if (attrMetadata is ImageAttributeMetadata)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.VarBinary, Parameters = { new MaxLiteral() } };

            if (attrMetadata is IntegerAttributeMetadata || typeCode == AttributeTypeCode.Integer)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

            if (attrMetadata is BigIntAttributeMetadata || typeCode == AttributeTypeCode.BigInt)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.BigInt };

            if (typeCode == AttributeTypeCode.PartyList)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new MaxLiteral() } };

            if (attrMetadata is LookupAttributeMetadata || attrMetadata.IsPrimaryId == true || typeCode == AttributeTypeCode.Lookup || typeCode == AttributeTypeCode.Customer || typeCode == AttributeTypeCode.Owner)
                return new UserDataTypeReference { Name = new SchemaObjectName { Identifiers = { new Identifier { Value = typeof(SqlEntityReference).FullName } } } };

            if (attrMetadata is MemoAttributeMetadata || typeCode == AttributeTypeCode.Memo)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { attrMetadata is MemoAttributeMetadata memo && memo.MaxLength != null ? (Literal) new IntegerLiteral { Value = memo.MaxLength.Value.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };

            if (attrMetadata is MoneyAttributeMetadata || typeCode == AttributeTypeCode.Money)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Money };

            if (attrMetadata is PicklistAttributeMetadata || typeCode == AttributeTypeCode.Picklist)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

            if (attrMetadata is StateAttributeMetadata || typeCode == AttributeTypeCode.State)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

            if (attrMetadata is StatusAttributeMetadata || typeCode == AttributeTypeCode.Status)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

            if (attrMetadata is StringAttributeMetadata || typeCode == AttributeTypeCode.String)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { attrMetadata is StringAttributeMetadata str && str.MaxLength != null ? (Literal)new IntegerLiteral { Value = str.MaxLength.Value.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };

            if (attrMetadata is UniqueIdentifierAttributeMetadata || typeCode == AttributeTypeCode.Uniqueidentifier)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };

            if (attrMetadata.AttributeTypeName == AttributeTypeDisplayName.FileType)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };

            if (attrMetadata.AttributeType == AttributeTypeCode.Virtual)
                return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new MaxLiteral() } };

            throw new ApplicationException("Unknown attribute type " + attrMetadata.GetType());
        }
    }
}
