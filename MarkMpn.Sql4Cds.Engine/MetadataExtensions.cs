using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Extension methods to convert from Dataverse metadata to SQL schema
    /// </summary>
    public static class MetadataExtensions
    {
        /// <summary>
        /// The maximum length of an attribute which contains an entity logical name
        /// </summary>
        public static int EntityLogicalNameMaxLength { get; } = 64;

        /// <summary>
        /// The maximum length of an optionset label value
        /// </summary>
        private const int LabelMaxLength = 200;

        /// <summary>
        /// The suffixes that can be used for virtual attributes
        /// </summary>
        public static string[] VirtualLookupAttributeSuffixes { get; } = new[] { "name", "type", "pid" };

        /// <summary>
        /// Gets the base attribute (optionset, lookup etc.) from the name of a virtual attribute (___name, ___type etc.)
        /// </summary>
        /// <param name="entity">The entity the attribute is in</param>
        /// <param name="virtualAttributeLogicalName">The name of the virtual attribute to get the details for</param>
        /// <param name="suffix">The suffix of the virtual attribute</param>
        /// <returns>The metadata of the underlying attribute, or <see langword="null"/> if no virtual attribute is found</returns>
        public static AttributeMetadata FindBaseAttributeFromVirtualAttribute(this EntityMetadata entity, string virtualAttributeLogicalName, out string suffix)
        {
            var matchingSuffix = VirtualLookupAttributeSuffixes.SingleOrDefault(s => virtualAttributeLogicalName.EndsWith(s, StringComparison.OrdinalIgnoreCase));
            suffix = matchingSuffix;

            if (suffix == null)
                return null;

            return entity.Attributes
                .SingleOrDefault(a => a.LogicalName.Equals(virtualAttributeLogicalName.Substring(0, virtualAttributeLogicalName.Length - matchingSuffix.Length), StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Gets the virtual attributes from a base attribute
        /// </summary>
        /// <param name="attrMetadata">The underlying base attribute to get the virtual attributes for</param>
        /// <param name="dataSource">The datasource that the attribute is from</param>
        /// <param name="writeable">Indicates whether to get readable or writeable virtual attributes</param>
        /// <returns>A sequence of virtual attributes that are based on this attribute</returns>
        public static IEnumerable<VirtualAttribute> GetVirtualAttributes(this AttributeMetadata attrMetadata, DataSource dataSource, bool writeable)
        {
            if (!writeable)
            {
                if (attrMetadata is MultiSelectPicklistAttributeMetadata)
                    yield return new VirtualAttribute("name", DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.Implicit), null);
                else if (attrMetadata is EnumAttributeMetadata || attrMetadata is BooleanAttributeMetadata)
                    yield return new VirtualAttribute("name", DataTypeHelpers.NVarChar(LabelMaxLength, dataSource.DefaultCollation, CollationLabel.Implicit), null);
            }

            if (attrMetadata is LookupAttributeMetadata lookup)
            {
                // solutioncomponent entity doesn't return the names of lookup values
                // https://github.com/MarkMpn/Sql4Cds/issues/524
                if (!writeable && attrMetadata.EntityLogicalName != "solutioncomponent")
                    yield return new VirtualAttribute("name", DataTypeHelpers.NVarChar(lookup.Targets == null || lookup.Targets.Length == 0 ? 100 : lookup.Targets.Select(e => (dataSource.Metadata[e].Attributes.SingleOrDefault(a => a.LogicalName == dataSource.Metadata[e].PrimaryNameAttribute) as StringAttributeMetadata)?.MaxLength ?? 100).Max(), dataSource.DefaultCollation, CollationLabel.Implicit), null);

                if (lookup.Targets?.Length != 1 && lookup.AttributeType != AttributeTypeCode.PartyList)
                    yield return new VirtualAttribute("type", DataTypeHelpers.NVarChar(EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.Implicit), null);

                if (lookup.Targets != null && lookup.Targets.Any(logicalName => dataSource.Metadata[logicalName].DataProviderId == DataProviders.ElasticDataProvider))
                    yield return new VirtualAttribute("pid", DataTypeHelpers.NVarChar(100, dataSource.DefaultCollation, CollationLabel.Implicit), false);
            }
        }

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

        public static DataTypeReference GetAttributeSqlType(this AttributeMetadata attrMetadata, DataSource dataSource, bool write)
        {
            if (attrMetadata is MultiSelectPicklistAttributeMetadata)
                return DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.Implicit);

            var typeCode = attrMetadata.AttributeType;

            if (attrMetadata is ManagedPropertyAttributeMetadata managedProp)
                typeCode = managedProp.ValueAttributeTypeCode;

            if (attrMetadata is BooleanAttributeMetadata || typeCode == AttributeTypeCode.Boolean)
                return DataTypeHelpers.Bit;

            if (attrMetadata is DateTimeAttributeMetadata || typeCode == AttributeTypeCode.DateTime)
                return DataTypeHelpers.DateTime;

            if (attrMetadata is DecimalAttributeMetadata || typeCode == AttributeTypeCode.Decimal)
            {
                short scale = 2;

                if (attrMetadata is DecimalAttributeMetadata dec && dec.Precision != null)
                    scale = (short)dec.Precision.Value; // Precision property is actually scale (number of decimal places)

                var precision = (short)(12 + scale); // Max value is 100 Billion, which is 12 digits
                
                return DataTypeHelpers.Decimal(precision, scale);
            }

            if (attrMetadata is DoubleAttributeMetadata || typeCode == AttributeTypeCode.Double)
                return DataTypeHelpers.Float;

            if (attrMetadata is EntityNameAttributeMetadata || typeCode == AttributeTypeCode.EntityName)
                return DataTypeHelpers.NVarChar(EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.Implicit);

            if (attrMetadata is ImageAttributeMetadata)
                return DataTypeHelpers.VarBinary(Int32.MaxValue);

            if (attrMetadata is IntegerAttributeMetadata || typeCode == AttributeTypeCode.Integer)
                return DataTypeHelpers.Int;

            if (attrMetadata is BigIntAttributeMetadata || typeCode == AttributeTypeCode.BigInt)
                return DataTypeHelpers.BigInt;

            if (typeCode == AttributeTypeCode.PartyList)
                return DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.Implicit);

            if (attrMetadata is LookupAttributeMetadata || attrMetadata.IsPrimaryId == true || typeCode == AttributeTypeCode.Lookup || typeCode == AttributeTypeCode.Customer || typeCode == AttributeTypeCode.Owner)
                return DataTypeHelpers.EntityReference;

            if (attrMetadata is MemoAttributeMetadata || typeCode == AttributeTypeCode.Memo)
                return DataTypeHelpers.NVarChar(write && attrMetadata is MemoAttributeMetadata memo && memo.MaxLength != null ? memo.MaxLength.Value : Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.CoercibleDefault);
            
            if (attrMetadata is MoneyAttributeMetadata || typeCode == AttributeTypeCode.Money)
                return DataTypeHelpers.Money;

            if (attrMetadata is PicklistAttributeMetadata || typeCode == AttributeTypeCode.Picklist)
                return DataTypeHelpers.Int;

            if (attrMetadata is StateAttributeMetadata || typeCode == AttributeTypeCode.State)
                return DataTypeHelpers.Int;

            if (attrMetadata is StatusAttributeMetadata || typeCode == AttributeTypeCode.Status)
                return DataTypeHelpers.Int;

            if (attrMetadata is StringAttributeMetadata || typeCode == AttributeTypeCode.String)
            {
                if (attrMetadata.LogicalName.StartsWith("address"))
                {
                    var parts = attrMetadata.LogicalName.Split('_');
                    if (parts.Length == 2 && Int32.TryParse(parts[0].Substring(7), out _) && dataSource.Metadata.TryGetValue("customeraddress", out var addressMetadata))
                    {
                        // Attribute is e.g. address1_postalcode. Get the equivalent attribute from the customeraddress
                        // entity as it can have very different max length
                        attrMetadata = addressMetadata.Attributes.SingleOrDefault(a => a.LogicalName == parts[1]) as StringAttributeMetadata ?? attrMetadata;
                    }
                }

                var maxLength = Int32.MaxValue;

                if (attrMetadata is StringAttributeMetadata str)
                {
                    // MaxLength validation is applied on write, but existing values could be up to DatabaseLength / 2
                    maxLength = str.MaxLength ?? maxLength;

                    if (!write && str.DatabaseLength != null && str.DatabaseLength.Value / 2 > maxLength)
                        maxLength = str.DatabaseLength.Value / 2;
                }

                return DataTypeHelpers.NVarChar(maxLength, dataSource.DefaultCollation, CollationLabel.Implicit);
            }

            if (attrMetadata is UniqueIdentifierAttributeMetadata || typeCode == AttributeTypeCode.Uniqueidentifier)
                return DataTypeHelpers.UniqueIdentifier;

            if (attrMetadata.AttributeTypeName == AttributeTypeDisplayName.FileType)
                return DataTypeHelpers.UniqueIdentifier;

            if (attrMetadata.AttributeType == AttributeTypeCode.Virtual)
                return DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.Implicit);

            throw new ApplicationException("Unknown attribute type " + attrMetadata.GetType());
        }

        /// <summary>
        /// Converts a constant string value to an expression that generates the value of the appropriate type for use in a DML operation
        /// </summary>
        /// <param name="attribute">The attribute to convert the value for</param>
        /// <param name="value">The string representation of the value</param>
        /// <param name="isVariable">Indicates if the value is a variable name</param>
        /// <param name="context">The context that the expression is being compiled in</param>
        /// <param name="dataSource">The data source that the operation will be performed in</param>
        /// <returns>An expression that returns the value in the appropriate type</returns>
        internal static ScalarExpression GetDmlValue(this AttributeMetadata attribute, string value, bool isVariable, ExpressionCompilationContext context, DataSource dataSource)
        {
            var expr = (ScalarExpression)new StringLiteral { Value = value };

            if (isVariable)
                expr = new VariableReference { Name = value };

            expr.GetType(context, out var exprType);
            var attrType = attribute.GetAttributeSqlType(dataSource, false);

            if (DataTypeHelpers.IsSameAs(exprType, attrType))
                return expr;

            if (attribute.IsPrimaryId == true)
            {
                expr = new FunctionCall
                {
                    FunctionName = new Identifier { Value = nameof(ExpressionFunctions.CreateLookup) },
                    Parameters =
                    {
                        new StringLiteral { Value = attribute.EntityLogicalName },
                        expr
                    }
                };
            }
            else
            {
                expr = new CastCall
                {
                    Parameter = expr,
                    DataType = attrType
                };
            }

            return expr;
        }
    }

    /// <summary>
    /// Contains the details of a virtual attribute
    /// </summary>
    public class VirtualAttribute
    {
        /// <summary>
        /// Creates a new <see cref="VirtualAttribute"/>
        /// </summary>
        /// <param name="suffix">The suffix for this virtual attribute</param>
        /// <param name="dataType">The SQL data type of this virtual attribute</param>
        /// <param name="notNull">Indicates if this virtual attribute is known to be nullable or not-nullable</param>
        internal VirtualAttribute(string suffix, DataTypeReference dataType, bool? notNull)
        {
            Suffix = suffix;
            DataType = dataType;
            NotNull = notNull;
        }

        /// <summary>
        /// The suffix for this virtual attribute, e.g. "name", "type", "pid"
        /// </summary>
        public string Suffix { get; }

        /// <summary>
        /// The SQL data type for this virtual attribute
        /// </summary>
        public DataTypeReference DataType { get; }

        /// <summary>
        /// Indicates if this attribute is known to be nullable, not-nullable or whether it should inherit its nullability from the base attribute
        /// </summary>
        public bool? NotNull { get; }
    }
}
