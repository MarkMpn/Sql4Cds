using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides methods for converting DML statements to entity objects ready to execute
    /// </summary>
    class EntityReader
    {
        private readonly EntityMetadata _metadata;
        private readonly DataTable _dataTable;
        private readonly NodeCompilationContext _context;
        private readonly DataSource _dataSource;
        private readonly DataModificationStatement _statement;
        private readonly NamedTableReference _target;
        private readonly INodeSchema _schema;
        private readonly Dictionary<string, string> _columnRenaming;

        class DmlOperationDetails
        {
            private DmlOperationDetails()
            {
            }

            /// <summary>
            /// The name of the SQL clause that entities are being read for, e.g. "UPDATE"
            /// </summary>
            public string ClauseName { get; private set; }

            /// <summary>
            /// The name of the operation to include at the start of a log message, e.g. "Updating"
            /// </summary>
            public string InProgressUppercase { get; set; }

            /// <summary>
            /// The name of the operation to include in the middle of a log message, e.g. "updating"
            /// </summary>
            public string InProgressLowercase { get; set; }

            public Func<AttributeMetadata, bool> ValidAttributeFilter { get; private set; }

            public static DmlOperationDetails Insert { get; } = new DmlOperationDetails
            {
                ClauseName = "INSERT",
                InProgressUppercase = "Inserting",
                InProgressLowercase = "inserting",
                ValidAttributeFilter = attr => attr.IsValidForCreate != false
            };

            public static DmlOperationDetails UpdatePrimaryKey { get; } = new DmlOperationDetails
            {
                ClauseName = "UPDATE",
                InProgressUppercase = "Updating",
                InProgressLowercase = "updating",
                ValidAttributeFilter = attr => attr.IsValidForRead != false
            };

            public static DmlOperationDetails UpdateNewValues { get; } = new DmlOperationDetails
            {
                ClauseName = "UPDATE",
                InProgressUppercase = "Updating",
                InProgressLowercase = "updating",
                ValidAttributeFilter = attr => attr.IsValidForUpdate != false
            };

            public static DmlOperationDetails UpdateExistingValues { get; } = new DmlOperationDetails
            {
                ClauseName = "UPDATE",
                InProgressUppercase = "Updating",
                InProgressLowercase = "updating",
                ValidAttributeFilter = attr => attr.IsValidForRead != false
            };

            public static DmlOperationDetails Delete { get; } = new DmlOperationDetails
            {
                ClauseName = "DELETE",
                InProgressUppercase = "Deleting",
                InProgressLowercase = "deleting",
                ValidAttributeFilter = attr => attr.IsValidForRead != false
            };
        }

        class ColumnMapping
        {
            public ColumnReferenceExpression TargetColumn { get; set; }
            public string TargetColumnName { get; set; }
            public IColumnDefinition SourceColumn { get; set; }
            public string SourceColumnName { get; set; }
        }

        class ComplexLookupAttribute
        {
            public AttributeMetadata Metadata { get; set; }
            public ComplexLookupAttributeComponent Id { get; set; }
            public ComplexLookupAttributeComponent Type { get; set; }
            public ComplexLookupAttributeComponent Pid { get; set; }
        }

        class ComplexLookupAttributeComponent
        {
            public string[] SourceColumns { get; set; }
            public ColumnReferenceExpression TargetColumn { get; set; }
            public string TargetColumnName { get; set; }
            public Expression Accessor { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="EntityReader"/>
        /// </summary>
        /// <param name="metadata">The metadata of the entity type to read</param>
        /// <param name="context">The context the operation is being compiled in</param>
        /// <param name="dataSource">The data source the DML operation will be performed in </param>
        /// <param name="statement">The DML statement that is being validated/executed</param>
        /// <param name="target">The table that is being modified</param>
        public EntityReader(EntityMetadata metadata,
            NodeCompilationContext context,
            DataSource dataSource,
            DataModificationStatement statement,
            NamedTableReference target,
            IExecutionPlanNodeInternal source)
        {
            _metadata = metadata;
            _context = context;
            _dataSource = dataSource;
            _statement = statement;
            _target = target;
            _columnRenaming = new Dictionary<string, string>();
            _schema = GetSourceSchema(source);

            if (source is SelectNode select)
            {
                source = select.Source;
                _columnRenaming = select.ColumnSet.ToDictionary(c => c.OutputColumn, c => c.SourceColumn, StringComparer.OrdinalIgnoreCase);
            }

            if (source is AliasNode alias)
            {
                source = alias.Source;
                var aliasCols = alias.ColumnSet.ToDictionary(col => alias.Alias + "." + col.OutputColumn, col => col.SourceColumn, StringComparer.OrdinalIgnoreCase);

                if (_columnRenaming.Count > 0)
                    _columnRenaming = _columnRenaming.ToDictionary(kvp => kvp.Key, kvp => aliasCols[kvp.Value], StringComparer.OrdinalIgnoreCase);
                else
                    _columnRenaming = aliasCols;
            }

            Source = (IDataExecutionPlanNodeInternal)source;
        }

        /// <summary>
        /// Creates a new <see cref="EntityReader"/>
        /// </summary>
        /// <param name="metadata">The metadata of the entity type to read</param>
        /// <param name="context">The context the operation is being compiled in</param>
        /// <param name="dataSource">The data source the DML operation will be performed in </param>
        /// <param name="statement">The DML statement that is being validated/executed</param>
        /// <param name="target">The table that is being modified</param>
        public EntityReader(DataTable dataTable,
            NodeCompilationContext context,
            DataSource dataSource,
            DataModificationStatement statement,
            NamedTableReference target,
            IExecutionPlanNodeInternal source)
            : this((EntityMetadata)null, context, dataSource, statement, target, source)
        {
            _dataTable = dataTable;
        }

        /// <summary>
        /// The source node that will provide the data to populate the entities from
        /// </summary>
        public IDataExecutionPlanNodeInternal Source { get; }

        /// <summary>
        /// Returns the primary key fields that need to be present in the data source
        /// </summary>
        /// <returns></returns>
        public string[] GetPrimaryKeyFields(out bool isIntersect)
        {
            return GetPrimaryKeyFields(_metadata, _dataTable, out isIntersect);
        }

        public static string[] GetPrimaryKeyFields(EntityMetadata metadata, DataTable dataTable, out bool isIntersect)
        {
            if (dataTable != null)
            {
                isIntersect = false;
                return dataTable.PrimaryKey.Select(col => col.ColumnName).ToArray();
            }

            if (metadata.LogicalName == "listmember")
            {
                isIntersect = true;
                return new[] { "listid", "entityid" };
            }

            if (metadata.IsIntersect == true)
            {
                var relationship = metadata.ManyToManyRelationships.Single();
                isIntersect = true;
                return new[] { relationship.Entity1IntersectAttribute, relationship.Entity2IntersectAttribute };
            }

            if (metadata.LogicalName == "principalobjectaccess")
            {
                isIntersect = true;
                return new[] { "objectid", "objecttypecode", "principalid", "principaltypecode" };
            }

            if (metadata.LogicalName == "solutioncomponent")
            {
                isIntersect = true;
                return new[] { "objectid", "componenttype", "solutionid" };
            }

            isIntersect = false;

            if (metadata.DataProviderId == DataProviders.ElasticDataProvider)
            {
                // Elastic tables need the partitionid as part of the primary key
                return new[] { metadata.PrimaryIdAttribute, "partitionid" };
            }
            
            if (metadata.LogicalName == "activitypointer")
                return new[] { "activityid", "activitytypecode" };

            return new[] { metadata.PrimaryIdAttribute };
        }

        public List<AttributeAccessor> ValidateInsertColumnMapping(IList<ColumnReferenceExpression> targetColumns, string[] sourceColumns)
        {
            // Validate the total number of columns (can only mismatch for INSERT)
            if (targetColumns.Count != sourceColumns.Length)
            {
                if (targetColumns.Count > sourceColumns.Length)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InsertTooManyColumns((InsertStatement)_statement));
                else
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InsertTooFewColumns((InsertStatement)_statement));
            }

            // Match the source and target columns by index
            var colMappings = targetColumns
                .Select((col, i) => new ColumnMapping
                {
                    TargetColumn = col,
                    TargetColumnName = col.MultiPartIdentifier.Identifiers.Last().Value,
                    SourceColumnName = sourceColumns[i],
                    SourceColumn = _schema.Schema[sourceColumns[i]]
                })
                .ToArray();

            var accessors = ValidateInsertUpdateColumnMapping(DmlOperationDetails.Insert, colMappings, out var attributeNames);

            var requiredAttributes = Array.Empty<string>();

            // Special case: inserting into intersect tables requires the primary key columns to be set
            var primaryKeyFields = GetPrimaryKeyFields(out var isIntersect);

            if (isIntersect)
                requiredAttributes = primaryKeyFields;

            // Specialer case: lookup fields on principalobjectaccess could be set to EntityReference values,
            // so typecode fields do not necessarily need to be set.
            if (_metadata?.LogicalName == "principalobjectaccess")
                requiredAttributes = new[] { "objectid", "principalid", "accessrightsmask" };

            var missingRequiredAttributeErrors = requiredAttributes
                .Where(attr => !attributeNames.Contains(attr))
                .Select(attr => new { Error = Sql4CdsError.NotNullInsert(new Identifier { Value = attr }, new Identifier { Value = _metadata.LogicalName }, "Insert", _target), Suggestion = $"Inserting values into the {_metadata.LogicalName} table requires the {attr} column to be set" })
                .ToArray();

            if (missingRequiredAttributeErrors.Any())
                throw new NotSupportedQueryFragmentException(missingRequiredAttributeErrors.Select(e => e.Error).ToArray(), null) { Suggestion = String.Join(Environment.NewLine, missingRequiredAttributeErrors.Select(e => e.Suggestion)) };

            return accessors;
        }

        public List<AttributeAccessor> ValidateUpdatePrimaryKeyColumnMapping(IDictionary<string, string> mappings)
        {
            var accessors = ValidateInsertUpdateColumnMapping(DmlOperationDetails.UpdatePrimaryKey, mappings.Select(m => new ColumnMapping { SourceColumn = _schema.Schema[m.Value], SourceColumnName = m.Value, TargetColumnName = m.Key }).ToArray(), out var attributeNames);

            // Updating any record except for intersect entities requires all the primary key attributes
            foreach (var attribute in GetPrimaryKeyFields(out var isIntersect))
            {
                if (!attributeNames.Contains(attribute) && !isIntersect)
                    throw new NotSupportedQueryFragmentException($"UPDATE requires a value for primary key attribute {attribute}");
            }

            return accessors;
        }

        public List<AttributeAccessor> ValidateUpdateNewValueColumnMapping(IDictionary<ColumnReferenceExpression, string> mappings)
        {
            var errors = new List<Sql4CdsError>();
            var suggestions = new HashSet<string>();

            // partitionid is not writable even though it appears so in the metadata
            if (_metadata?.DataProviderId == DataProviders.ElasticDataProvider)
            {
                var partitionId = mappings.Keys.FirstOrDefault(col => col.MultiPartIdentifier.Identifiers.Last().Value.Equals("partitionid", StringComparison.OrdinalIgnoreCase));

                if (partitionId != null)
                {
                    errors.Add(Sql4CdsError.ReadOnlyColumn(partitionId));
                    suggestions.Add("The column \"partitionid\" cannot be modified");
                }
            }

            var primaryKeyFields = GetPrimaryKeyFields(out var isIntersect);
            if (isIntersect)
            {
                // Intersect tables can only have their primary key columns updated
                foreach (var col in mappings.Keys)
                {
                    if (primaryKeyFields.Contains(col.MultiPartIdentifier.Identifiers.Last().Value, StringComparer.OrdinalIgnoreCase))
                        continue;

                    // Special case: solutioncomponent can have its rootcomponentbehavior column updated
                    if (_metadata.LogicalName == "solutioncomponent" && col.MultiPartIdentifier.Identifiers.Last().Value.Equals("rootcomponentbehavior", StringComparison.OrdinalIgnoreCase))
                        continue;

                    errors.Add(Sql4CdsError.ReadOnlyColumn(col));
                    var primaryKeyFieldNames = string.Join(", ", primaryKeyFields.Take(primaryKeyFields.Length - 1).Select(f => f)) + " and " + primaryKeyFields.Last();
                    suggestions.Add($"Only the {primaryKeyFieldNames} columns can be used when updating values in the {_metadata.LogicalName} table");
                }
            }

            return ValidateInsertUpdateColumnMapping(DmlOperationDetails.UpdateNewValues, mappings.Select(m => new ColumnMapping { SourceColumn = _schema.Schema[m.Value], SourceColumnName = m.Value, TargetColumn = m.Key, TargetColumnName = m.Key.MultiPartIdentifier.Identifiers.Last().Value }).ToArray(), out var attributeNames);
        }

        public List<AttributeAccessor> ValidateUpdateExistingValueColumnMapping(IDictionary<string, string> mappings)
        {
            // Updating any intersect record requires all the primary key attributes
            foreach (var attribute in GetPrimaryKeyFields(out var isIntersect))
            {
                if (!mappings.ContainsKey(attribute) && isIntersect)
                    throw new NotSupportedQueryFragmentException($"UPDATE requires an existing value for primary key attribute {attribute}");
            }

            return ValidateInsertUpdateColumnMapping(DmlOperationDetails.UpdateExistingValues, mappings.Select(m => new ColumnMapping { SourceColumn = _schema.Schema[m.Value], SourceColumnName = m.Value, TargetColumnName = m.Key }).ToArray(), out var attributeNames);
        }

        public List<AttributeAccessor> ValidateDeleteColumnMapping(IDictionary<string, string> mappings)
        {
            return ValidateInsertUpdateColumnMapping(DmlOperationDetails.Delete, mappings.Select(m => new ColumnMapping { SourceColumn = _schema.Schema[m.Value], SourceColumnName = m.Value, TargetColumnName = m.Key }).ToArray(), out var attributeNames);
        }

        private INodeSchema GetSourceSchema(IExecutionPlanNodeInternal source)
        {
            if (source is SelectNode select)
            {
                var schema = select.Source.GetSchema(_context);
                var mappedSchema = new ColumnList();

                foreach (var col in select.ColumnSet)
                    mappedSchema.Add(col.OutputColumn, schema.Schema[col.SourceColumn]);

                return new NodeSchema(mappedSchema, null, null, null);
            }

            if (source is IDataExecutionPlanNodeInternal dataNode)
                return dataNode.GetSchema(_context);
            else if (source is SqlNode sql)
                return sql.GetSchema(_context);
            else
                throw new NotSupportedException();
        }

        private List<AttributeAccessor> ValidateInsertUpdateColumnMapping(DmlOperationDetails operation, ColumnMapping[] colMappings, out HashSet<string> attributeNames)
        {
            foreach (var col in colMappings)
            {
                if (_columnRenaming.TryGetValue(col.SourceColumnName, out var renamed))
                    col.SourceColumnName = renamed;
            }

            var attributes = _metadata?.Attributes.ToDictionary(attr => attr.LogicalName, StringComparer.OrdinalIgnoreCase);
            var tableName = _metadata?.LogicalName ?? _dataTable.TableName;
            attributeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var primaryKeyFields = GetPrimaryKeyFields(out var isIntersect);

            // Special case: solutioncomponent can have its rootcomponentbehavior column inserted/updated
            if (tableName == "solutioncomponent" && (operation == DmlOperationDetails.Insert || operation == DmlOperationDetails.UpdateNewValues || operation == DmlOperationDetails.UpdateExistingValues))
                primaryKeyFields = primaryKeyFields.Concat(new[] { "rootcomponentbehavior" }).ToArray();

            var contextParam = Expression.Parameter(typeof(ExpressionExecutionContext));
            var entityParam = Expression.Property(contextParam, nameof(ExpressionExecutionContext.Entity));
            var accessors = new List<AttributeAccessor>();
            var entityReferenceRawAccessors = new Dictionary<string, Expression>(StringComparer.OrdinalIgnoreCase);
            var complexLookupAttributes = new Dictionary<string, ComplexLookupAttribute>(StringComparer.OrdinalIgnoreCase);

            var errors = new List<Sql4CdsError>();
            var suggestions = new HashSet<string>();

            // Check all target columns are valid for create/update. Check the type conversions and build the accessors
            // as we go.
            foreach (var colMapping in colMappings)
            {
                var col = colMapping.TargetColumn;
                var colName = colMapping.TargetColumnName;
                DataTypeReference targetType;
                Type targetClrType;

                // Handle virtual __type and __pid attributes.
                string suffix = null;
                var attr = _metadata?.FindBaseAttributeFromVirtualAttribute(colName, out suffix);
                if (attr != null)
                {
                    var virtualAttr = attr.GetVirtualAttributes(_dataSource, true)
                        .SingleOrDefault(va => va.Suffix == suffix);

                    if (virtualAttr == null)
                    {
                        errors.Add(Sql4CdsError.InvalidColumnName(col));
                        continue;
                    }

                    colName = attr.LogicalName + virtualAttr.Suffix;
                    targetType = virtualAttr.DataType();
                    targetClrType = typeof(string);

                    if (!complexLookupAttributes.TryGetValue(attr.LogicalName, out var lookupDetails))
                    {
                        lookupDetails = new ComplexLookupAttribute { Metadata = attr };
                        complexLookupAttributes[attr.LogicalName] = lookupDetails;
                    }

                    if (suffix == "type")
                        lookupDetails.Type = new ComplexLookupAttributeComponent { SourceColumns = new[] { colMapping.SourceColumnName }, TargetColumn = col, TargetColumnName = attr.LogicalName + suffix };
                    else if (suffix == "pid")
                        lookupDetails.Pid = new ComplexLookupAttributeComponent { SourceColumns = new[] { colMapping.SourceColumnName }, TargetColumn = col, TargetColumnName = attr.LogicalName + suffix };
                }
                else if (_dataTable != null)
                {
                    var dataCol = _dataTable.Columns[colName];

                    if (dataCol == null)
                    {
                        errors.Add(Sql4CdsError.InvalidColumnName(col));
                        continue;
                    }
                    else
                    {
                        colName = dataCol.ColumnName;
                        var netSqlType = dataCol.DataType;

                        if (netSqlType == typeof(long))
                        {
                            // Special case for auto-increment fields
                            targetType = DataTypeHelpers.BigInt;
                            targetClrType = typeof(long?);
                        }
                        else
                        {
                            targetType = netSqlType.ToSqlType(_dataSource);
                            targetClrType = dataCol.DataType;
                        }
                    }
                }
                else if (!attributes.TryGetValue(colName, out attr))
                {
                    errors.Add(Sql4CdsError.InvalidColumnName(col));
                    continue;
                }
                else if (((operation == DmlOperationDetails.Insert && attr.IsPrimaryId == true) || isIntersect) && attr is LookupAttributeMetadata)
                {
                    // When writing a primary key, treat is as a raw guid instead of an EntityReference
                    colName = attr.LogicalName;
                    targetType = DataTypeHelpers.UniqueIdentifier;
                    targetClrType = typeof(Guid?);
                }
                else
                {
                    colName = attr.LogicalName;
                    targetType = attr.GetAttributeSqlType(_dataSource, true);
                    targetClrType = attr.GetAttributeType();
                }

                if (!attributeNames.Add(colName))
                {
                    errors.Add(Sql4CdsError.DuplicateInsertUpdateColumn(col));
                    continue;
                }

                if (tableName == "principalobjectaccess")
                {
                    if (attr.LogicalName == "objecttypecode" || attr.LogicalName == "principaltypecode")
                    {
                        var baseAttr = colName.Replace("typecode", "id");
                        if (!complexLookupAttributes.TryGetValue(baseAttr, out var lookupDetails))
                        {
                            lookupDetails = new ComplexLookupAttribute { Metadata = _metadata.Attributes.Single(a => a.LogicalName.Equals(baseAttr, StringComparison.OrdinalIgnoreCase)) };
                            complexLookupAttributes[baseAttr] = lookupDetails;
                        }

                        lookupDetails.Type = new ComplexLookupAttributeComponent { SourceColumns = new[] { colMapping.SourceColumnName }, TargetColumn = col, TargetColumnName = attr.LogicalName };
                    }
                    else if (attr.LogicalName == "objectid" || attr.LogicalName == "principalid")
                    {
                        // Special case: principalobjectaccess.objectid and principalobjectaccess.principalid are defined as guid
                        // fields but act as polymorphic lookup fields in coordination with the associated typecode field
                        targetType = DataTypeHelpers.EntityReference;
                        targetClrType = typeof(EntityReference);
                    }
                    else if (attr.LogicalName != "accessrightsmask")
                    {
                        errors.Add(Sql4CdsError.ReadOnlyColumn(col));
                        suggestions.Add($"Only the objectid, principalid and accessrightsmask columns can be used when {operation.InProgressLowercase} values into the principalobjectaccess table");
                        continue;
                    }
                }
                else if (isIntersect)
                {
                    if (!primaryKeyFields.Contains(attr.LogicalName))
                    {
                        errors.Add(Sql4CdsError.ReadOnlyColumn(col));
                        var primaryKeyFieldNames = string.Join(", ", primaryKeyFields.Take(primaryKeyFields.Length - 1).Select(f => f)) + " and " + primaryKeyFields.Last();
                        suggestions.Add($"Only the {primaryKeyFieldNames} columns can be used when {operation.InProgressLowercase} values into the {_metadata.LogicalName} table");
                        continue;
                    }
                }
                else
                {
                    if (attr != null && !operation.ValidAttributeFilter(attr))
                    {
                        errors.Add(Sql4CdsError.ReadOnlyColumn(col));
                        suggestions.Add($"Column is not valid for {operation.ClauseName}");
                        continue;
                    }
                }

                // Check we can convert values from the source column to the target attribute.
                // For lookup fields, we may also be setting the associated type/pid field so allow guid -> EntityReference conversion as well
                var sourceType = colMapping.SourceColumn.Type;

                if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetType) &&
                    targetType.IsEntityReference() &&
                    SqlTypeConverter.CanChangeTypeExplicit(sourceType, DataTypeHelpers.UniqueIdentifier))
                {
                    // Special case: listmember.entityid does not require the virtual __type field as its type is defined by the list
                    if (tableName != "list" && attr.LogicalName != "entityid")
                    {
                        if (!complexLookupAttributes.TryGetValue(attr.LogicalName, out var lookupDetails))
                        {
                            lookupDetails = new ComplexLookupAttribute { Metadata = attr };
                            complexLookupAttributes[attr.LogicalName] = lookupDetails;
                        }

                        lookupDetails.Id = new ComplexLookupAttributeComponent { SourceColumns = new[] { colMapping.SourceColumnName }, TargetColumn = col, TargetColumnName = attr.LogicalName };

                        if (lookupDetails.Type == null && attr.GetVirtualAttributes(_dataSource, true).Any(va => va.Suffix == "type"))
                            lookupDetails.Type = new ComplexLookupAttributeComponent { TargetColumnName = attr.LogicalName + "type" };
                        else if (lookupDetails.Type == null && tableName == "principalobjectaccess")
                            lookupDetails.Type = new ComplexLookupAttributeComponent { TargetColumnName = attr.LogicalName.Replace("id", "typecode") };

                        if (lookupDetails.Pid == null && attr.GetVirtualAttributes(_dataSource, true).Any(va => va.Suffix == "pid"))
                            lookupDetails.Pid = new ComplexLookupAttributeComponent { TargetColumnName = attr.LogicalName + "pid" };
                    }

                    // Change the expected type for this column to guid so we can create the accessor
                    targetType = DataTypeHelpers.UniqueIdentifier;
                    targetClrType = typeof(Guid?);
                }

                // Check the source data type can be mapped to the target attribute type
                if (SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetType))
                {
                    // Create the accessor for this attribute
                    var sourceNetType = sourceType.ToNetType(out _);
                    var targetNetType = targetType.ToNetType(out _);

                    Expression accessor;
                    Expression rawAccessor;

                    if (sourceType == DataTypeHelpers.ImplicitIntForNullLiteral)
                    {
                        accessor = Expression.Constant(null, targetClrType);
                        rawAccessor = Expression.Constant(SqlTypeConverter.GetNullValue(targetNetType));
                    }
                    else
                    {
                        // Get the value from the source entity
                        Expression expr = Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(colMapping.SourceColumnName));

                        // Unbox it as the expected type
                        expr = Expression.Convert(expr, sourceNetType);

                        var convertedExpr = expr;

                        if (!sourceType.IsEntityReference() ||
                            !(attr is LookupAttributeMetadata partyListAttr) ||
                            partyListAttr.AttributeType != AttributeTypeCode.PartyList)
                        {
                            // Convert to destination SQL type - don't do this if we're converting from an EntityReference to a PartyList so
                            // we don't lose the entity name during the conversion via a string
                            convertedExpr = SqlTypeConverter.Convert(convertedExpr, contextParam, sourceType, targetType, throwOnTruncate: true, table: tableName, column: colName);
                        }

                        // Convert to final .NET SDK type
                        rawAccessor = convertedExpr;
                        convertedExpr = SqlTypeConverter.Convert(convertedExpr, contextParam, targetClrType);

                        if (attr is EnumAttributeMetadata && !(attr is MultiSelectPicklistAttributeMetadata) && !(attr is EntityNameAttributeMetadata))
                        {
                            convertedExpr = Expression.New(
                                typeof(OptionSetValue).GetConstructor(new[] { typeof(int) }),
                                Expression.Convert(convertedExpr, typeof(int))
                            );
                            targetClrType = typeof(OptionSetValue);
                        }
                        else if (attr is MoneyAttributeMetadata)
                        {
                            convertedExpr = Expression.New(
                                typeof(Money).GetConstructor(new[] { typeof(decimal) }),
                                Expression.Convert(convertedExpr, typeof(decimal))
                            );
                            targetClrType = typeof(Money);
                        }
                        else if (attr is DateTimeAttributeMetadata)
                        {
                            convertedExpr = Expression.Convert(
                                Expr.Call(() => DateTime.SpecifyKind(Expr.Arg<DateTime>(), Expr.Arg<DateTimeKind>()),
                                    convertedExpr,
                                    Expression.Constant(_context.Options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc)
                                ),
                                typeof(DateTime?)
                            );
                        }

                        // Check for null on the value BEFORE converting from the SQL to BCL type to avoid e.g. SqlDateTime.Null being converted to 1900-01-01
                        if (typeof(INullable).IsAssignableFrom(targetClrType))
                        {
                            accessor = convertedExpr;
                        }
                        else
                        {
                            accessor = Expression.Condition(
                                SqlTypeConverter.NullCheck(expr),
                                Expression.Constant(null, targetClrType),
                                convertedExpr);
                        }
                    }

                    var usedAccessor = false;

                    foreach (var lookupMapping in complexLookupAttributes.Values)
                    {
                        if (lookupMapping.Id?.TargetColumnName == colName)
                            lookupMapping.Id.Accessor = rawAccessor;
                        else if (lookupMapping.Type?.TargetColumnName == colName)
                            lookupMapping.Type.Accessor = rawAccessor;
                        else if (lookupMapping.Pid?.TargetColumnName == colName)
                            lookupMapping.Pid.Accessor = rawAccessor;
                        else
                            continue;

                        usedAccessor = true;
                        break;
                    }

                    if (rawAccessor.Type == typeof(SqlEntityReference))
                        entityReferenceRawAccessors[attr.LogicalName] = rawAccessor;

                    if (!usedAccessor)
                    {
                        // Ensure we can return the value as an object
                        accessor = Expr.Box(accessor);

                        accessors.Add(new AttributeAccessor
                        {
                            SourceAttributes = new[] { colMapping.SourceColumnName },
                            TargetAttribute = colName,
                            Accessor = Expression.Lambda<Func<ExpressionExecutionContext, object>>(accessor, contextParam).Compile()
                        });
                    }

                    continue;
                }

                errors.Add(Sql4CdsError.TypeClash(col, sourceType, targetType));
            }

            // Validate the combinations of lookup fields and virtual type/pid fields
            // * If setting a type/pid field, the associated lookup field MUST also be set
            // * If setting a lookup field to an EntityReference, the associated type/pid field MAY be set. If it is set, the EntityReference is converted to a guid
            // * If setting a lookup field to any other type, the associated type/pid field MUST be set
            foreach (var lookupMapping in complexLookupAttributes.Values)
            {
                if (lookupMapping.Id?.Accessor == null)
                {
                    if (entityReferenceRawAccessors.TryGetValue(lookupMapping.Metadata.LogicalName, out var entityReferenceRawAccessor))
                    {
                        var entityReferenceAccessor = accessors.Single(a => a.TargetAttribute == lookupMapping.Metadata.LogicalName);
                        accessors.Remove(entityReferenceAccessor);
                        lookupMapping.Id = new ComplexLookupAttributeComponent
                        {
                            Accessor = Expression.Convert(entityReferenceRawAccessor, typeof(SqlGuid)),
                            SourceColumns = entityReferenceAccessor.SourceAttributes,
                            TargetColumn = null,
                            TargetColumnName = entityReferenceAccessor.TargetAttribute
                        };
                    }
                }

                if (lookupMapping.Id?.Accessor == null)
                {
                    if (lookupMapping.Type?.TargetColumn != null)
                    {
                        errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Type.TargetColumn));

                        if (accessors.Any(a => a.TargetAttribute == lookupMapping.Metadata.LogicalName))
                            suggestions.Add($"{operation.InProgressUppercase} values in the polymorphic lookup field {lookupMapping.Metadata.LogicalName} is using an EntityReference type. There is no need to also set the {lookupMapping.Type.TargetColumnName} field");
                        else
                            suggestions.Add($"{operation.InProgressUppercase} values in a polymorphic lookup field requires setting the associated ID column as well\r\nAdd a value for the {lookupMapping.Metadata.LogicalName} column");
                    }

                    if (lookupMapping.Pid?.TargetColumn != null)
                    {
                        errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Pid.TargetColumn));

                        if (accessors.Any(a => a.TargetAttribute == lookupMapping.Metadata.LogicalName))
                            suggestions.Add($"{operation.InProgressUppercase} values in the elastic lookup field {lookupMapping.Metadata.LogicalName} is using an EntityReference type. There is no need to also set the {lookupMapping.Pid.TargetColumnName} field");
                        else
                            suggestions.Add($"{operation.InProgressUppercase} values in an elastic lookup field requires setting the associated ID column as well\r\nAdd a value for the {lookupMapping.Metadata.LogicalName} column");
                    }
                }
                else
                {
                    if (lookupMapping.Type?.TargetColumnName != null && lookupMapping.Type.SourceColumns == null)
                    {
                        if (lookupMapping.Type.TargetColumn != null)
                            errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Type.TargetColumn));
                        else
                            errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Type.TargetColumnName));

                        suggestions.Add($"{operation.InProgressUppercase} values in a polymorphic lookup field requires setting the associated type column as well\r\nAdd a value for the {lookupMapping.Type.TargetColumnName} column");
                    }

                    if (lookupMapping.Pid?.TargetColumnName != null && lookupMapping.Pid.SourceColumns == null)
                    {
                        if (lookupMapping.Pid.TargetColumn != null)
                            errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Pid.TargetColumn));
                        else
                            errors.Add(Sql4CdsError.ReadOnlyColumn(lookupMapping.Pid.TargetColumnName));

                        suggestions.Add($"{operation.InProgressUppercase} values in a polymorphic lookup field requires setting the associated pid column as well\r\nAdd a value for the {lookupMapping.Pid.TargetColumnName} column");
                    }
                }
            }

            // We've done all the validation, so throw any errors we've found
            if (errors.Count > 0)
                throw new NotSupportedQueryFragmentException(errors.ToArray(), null) { Suggestion = String.Join("\r\n", suggestions) };

            // Create the accessors for any complex lookup fields, using the accessors for the individual parts
            foreach (var lookupMapping in complexLookupAttributes)
            {
                var attr = lookupMapping.Value.Metadata;
                var guidAccessor = lookupMapping.Value.Id.Accessor;
                var typeAccessor = lookupMapping.Value.Type?.Accessor;
                var pidAccessor =  lookupMapping.Value.Pid?.Accessor;

                // If we're creating an elastic lookup for a fixed type, we still need to know the type
                if (typeAccessor == null && attr is LookupAttributeMetadata lookupAttr)
                {
                    if (lookupAttr.Targets.Length != 1)
                        throw new NotSupportedException();

                    typeAccessor = Expression.Convert(Expression.Constant(lookupAttr.Targets[0]), typeof(SqlString));
                }

                // Use the CreateLookup or CreateElasticLookup functions to actually create the value
                Expression accessor;

                if (pidAccessor == null)
                    accessor = Expr.Call(() => ExpressionFunctions.CreateLookup(Expr.Arg<SqlString>(), Expr.Arg<SqlGuid>(), Expr.Arg<ExpressionExecutionContext>()), typeAccessor, guidAccessor, contextParam);
                else
                    accessor = Expr.Call(() => ExpressionFunctions.CreateElasticLookup(Expr.Arg<SqlString>(), Expr.Arg<SqlGuid>(), Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), typeAccessor, guidAccessor, pidAccessor, contextParam);

                accessor = Expression.Convert(accessor, typeof(EntityReference));

                accessors.Add(new AttributeAccessor
                {
                    SourceAttributes = new[] { lookupMapping.Value.Id.SourceColumns, lookupMapping.Value.Type?.SourceColumns, lookupMapping.Value.Pid?.SourceColumns }.Where(s => s != null).SelectMany(s => s).ToArray(),
                    TargetAttribute = attr.LogicalName,
                    Accessor = Expression.Lambda<Func<ExpressionExecutionContext, object>>(accessor, contextParam).Compile()
                });
            }

            // Everything looks OK, so return the attribute accessors.
            return accessors;
        }
    }

    /// <summary>
    /// Provides the details for how the values for a single attribute will be populated in a DML operation
    /// </summary>
    class AttributeAccessor
    {
        /// <summary>
        /// The logical name of the attribute that is being populated
        /// </summary>
        [DictionaryKey]
        public string TargetAttribute { get; set; }

        /// <summary>
        /// The name of the column in the source data that will be used to populate the attribute
        /// </summary>
        /// <remarks>
        /// The value in this property is used for display and debugging purposes only. The actual
        /// value is retrieved by the <see cref="Accessor"/> property
        /// </remarks>
        [DictionaryValue]
        public string[] SourceAttributes { get; set; }

        /// <summary>
        /// A function that retrieves the value for the attribute from the source data
        /// </summary>
        /// <remarks>
        /// The accessor can perform type conversion and combine values from multiple columns in the source data
        /// to produce the final value.
        /// </remarks>
        public Func<ExpressionExecutionContext, object> Accessor { get; set; }
    }
}
