using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides metadata about metadata objects, allowing them to be converted into regular <see cref="Entity"/>
    /// objects that can be queried using FetchXML
    /// </summary>
    class MetaMetadata
    {
        private static readonly IDictionary<Type, MetaMetadata> _metaMetadata;

        static MetaMetadata()
        {
            _metaMetadata = new Dictionary<Type, MetaMetadata>
            {
                [typeof(Label)] = new LabelMetadata(),
                [typeof(LocalizedLabel)] = new LocalizedLabelMetadata(),
                [typeof(OptionSetMetadataBase)] = new MetaMetadata(typeof(OptionSetMetadataBase), "globaloptionset"),
                [typeof(OptionSetMetadata)] = new MetaMetadata(typeof(OptionSetMetadata), "optionset"),
                [typeof(BooleanOptionSetMetadata)] = new MetaMetadata(typeof(BooleanOptionSetMetadata), "booloptionset"),
                [typeof(OptionMetadata)] = new OptionMetadataMetadata(),
                [typeof(EntityMetadata)] = new MetaMetadata(typeof(EntityMetadata), "entity"),
                [typeof(AttributeMetadata)] = new MetaMetadata(typeof(AttributeMetadata), "attribute"),
                [typeof(OneToManyRelationshipMetadata)] = new MetaMetadata(typeof(OneToManyRelationshipMetadata), "relationship_1_n"),
                [typeof(ManyToManyRelationshipMetadata)] = new MetaMetadata(typeof(ManyToManyRelationshipMetadata), "relationship_n_n")
            };
        }

        public static MetaMetadata GetMetadataProvider(Type type)
        {
            return _metaMetadata[type];
        }

        public static IEnumerable<MetaMetadata> GetMetadata()
        {
            return _metaMetadata.Values;
        }

        public static IDictionary<string, IDictionary<Guid, Entity>> GetData<T>(T[] objects)
        {
            var data = new Dictionary<string, IDictionary<Guid, Entity>>();

            foreach (var obj in objects)
            {
                ExtractEntitiesFromObjectTree(typeof(T), obj, null, data, null);

                if (obj.GetType() != typeof(T) && _metaMetadata.ContainsKey(obj.GetType()))
                    ExtractEntitiesFromObjectTree(obj.GetType(), obj, null, data, null);
            }

            return data;
        }

        private static void ExtractEntitiesFromObjectTree(Type type, object obj, Entity parent, Dictionary<string, IDictionary<Guid, Entity>> data, Guid? id)
        {
            if (!_metaMetadata.TryGetValue(type, out var metadata))
                return;

            if (!data.TryGetValue(metadata.LogicalName, out var collection))
            {
                collection = new Dictionary<Guid, Entity>();
                data[metadata.LogicalName] = collection;
            }

            var entity = metadata.GetEntity(obj, id, parent);
            collection[entity.Id] = entity;

            foreach (var prop in type.GetProperties())
            {
                if (!prop.CanRead)
                    continue;

                if (_metaMetadata.ContainsKey(prop.PropertyType))
                {
                    var value = prop.GetValue(obj);

                    if (value == null)
                        continue;

                    var subId = (Guid?)null;

                    if (!(value is MetadataBase))
                        subId = GetGuid(entity.Id.ToString() + prop.Name);

                    ExtractEntitiesFromObjectTree(prop.PropertyType, value, entity, data, subId);
                }

                Type elementType = null;

                if (prop.PropertyType.IsArray)
                {
                    elementType = prop.PropertyType.GetElementType();
                }
                else
                {
                    var interfaces = prop.PropertyType.GetInterfaces();

                    foreach (var @interface in interfaces)
                    {
                        if (@interface.IsGenericType && @interface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                        {
                            elementType = @interface.GetGenericArguments()[0];
                            break;
                        }
                    }
                }

                if (elementType != null)
                {
                    if (_metaMetadata.ContainsKey(elementType))
                    {
                        var array = (System.Collections.IEnumerable)prop.GetValue(obj);

                        if (array == null)
                            continue;

                        foreach (var element in array)
                        {
                            ExtractEntitiesFromObjectTree(elementType, element, entity, data, null);

                            if (element.GetType() != elementType && _metaMetadata.ContainsKey(element.GetType()))
                                ExtractEntitiesFromObjectTree(element.GetType(), element, entity, data, null);
                        }
                    }
                }
            }
        }

        protected MetaMetadata(Type type) : this(type, type.Name.ToLower())
        {
        }

        protected MetaMetadata(Type type, string logicalName)
        {
            Type = type;
            LogicalName = logicalName;
        }

        public Type Type { get; }

        public string LogicalName { get; }

        public EntityMetadata GetEntityMetadata()
        {
            var metadata = new EntityMetadata();
            metadata.LogicalName = LogicalName;

            SetSealedProperty(metadata, nameof(metadata.Attributes), GetAttributes());

            // TODO: Populate relationship metadata

            return metadata;
        }

        private static void SetSealedProperty(object target, string prop, object value)
        {
            target.GetType().GetProperty(prop).SetValue(target, value, null);
        }

        protected virtual AttributeMetadata[] GetAttributes()
        {
            return Type.GetProperties()
                .SelectMany(prop => GetAttributeMetadata(prop))
                .Where(attr => attr != null)
                .ToArray();
        }

        private IEnumerable<AttributeMetadata> GetAttributeMetadata(PropertyInfo prop)
        {
            if (!prop.CanRead)
                yield break;

            var type = prop.PropertyType;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                type = type.GetGenericArguments()[0];

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(string) || type == typeof(Microsoft.Xrm.Sdk.Label) || type.IsEnum)
                yield return new StringAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

            if (type == typeof(int))
                yield return new IntegerAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

            if (type == typeof(bool))
                yield return new BooleanAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

            if (type == typeof(Guid))
            {
                yield return new UniqueIdentifierAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

                if (typeof(MetadataBase).IsAssignableFrom(Type) && prop.Name == nameof(MetadataBase.MetadataId))
                    yield return new UniqueIdentifierAttributeMetadata(LogicalName + "id") { LogicalName = LogicalName + "id" };
            }

            if (_metaMetadata.TryGetValue(type, out var lookupMetadata))
                yield return new LookupAttributeMetadata { SchemaName = prop.Name + "Id", LogicalName = prop.Name.ToLower() + "id", Targets = new[] { lookupMetadata.LogicalName } };

            // TODO: Add support for more property types
        }

        public virtual Entity GetEntity(object obj, Guid? id, Entity parent)
        {
            var entity = new Entity(LogicalName, id ?? GetId(obj));

            foreach (var prop in Type.GetProperties())
            {
                if (!prop.CanRead)
                    continue;

                var attrName = prop.Name.ToLower();
                var value = prop.GetValue(obj, null);
                var type = prop.PropertyType;

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    type = type.GetGenericArguments()[0];

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                {
                    type = type.GetGenericArguments()[0];

                    if (value != null)
                        value = value.GetType().GetProperty("Value").GetValue(value);
                }

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                {
                    type = type.GetGenericArguments()[0];

                    if (value != null)
                        value = value.GetType().GetProperty("Value").GetValue(value);
                }

                if (type == typeof(Label))
                {
                    entity[attrName] = ((Label)value)?.UserLocalizedLabel?.Label;
                }
                else if (type.IsEnum)
                {
                    entity[attrName] = value?.ToString();
                }
                else if (type == typeof(string) || type == typeof(int) || type == typeof(bool) || type == typeof(Guid))
                {
                    entity[attrName] = value;

                    if (obj is MetadataBase && prop.Name == nameof(MetadataBase.MetadataId))
                        entity[LogicalName + "id"] = value;
                }

                if (_metaMetadata.TryGetValue(type, out var lookupMetadata))
                {
                    if (value == null)
                    {
                        entity[attrName + "id"] = null;
                    }
                    else
                    {
                        Guid lookupId;

                        if (value is MetadataBase mb && mb.MetadataId != null)
                            lookupId = mb.MetadataId.Value;
                        else
                            lookupId = GetGuid(entity.Id.ToString() + prop.Name).Value;

                        entity[attrName + "id"] = new EntityReference(lookupMetadata.LogicalName, lookupId);
                    }
                }
            }

            return entity;
        }

        protected Guid GetId(object obj)
        {
            if (obj is MetadataBase mb && mb.MetadataId != null)
                return mb.MetadataId.Value;

            return Guid.Empty;
        }

        private static readonly Guid GuidNamespace = new Guid("{8F466553-A780-4FCB-8DA7-329EEB30416F}");

        protected static Guid? GetGuid(string name)
        {
            if (String.IsNullOrEmpty(name))
                return null;

            return CreateGuid(GuidNamespace, Encoding.UTF8.GetBytes(name), 3);
        }

        /// <summary>
		/// Creates a name-based UUID using the algorithm from RFC 4122 §4.3.
		/// </summary>
		/// <param name="namespaceId">The ID of the namespace.</param>
		/// <param name="nameBytes">The name (within that namespace).</param>
		/// <param name="version">The version number of the UUID to create; this value must be either
		/// 3 (for MD5 hashing) or 5 (for SHA-1 hashing).</param>
		/// <returns>A UUID derived from the namespace and name.</returns>
        /// <remarks>
        /// https://github.com/Faithlife/FaithlifeUtility/blob/master/src/Faithlife.Utility/GuidUtility.cs
        /// </remarks>
		public static Guid CreateGuid(Guid namespaceId, byte[] nameBytes, int version)
        {
            if (version != 3 && version != 5)
                throw new ArgumentOutOfRangeException(nameof(version), "version must be either 3 or 5.");

            // convert the namespace UUID to network order (step 3)
            byte[] namespaceBytes = namespaceId.ToByteArray();
            SwapByteOrder(namespaceBytes);

            // compute the hash of the namespace ID concatenated with the name (step 4)
            byte[] data = namespaceBytes.Concat(nameBytes).ToArray();
            byte[] hash;
            using (var algorithm = version == 3 ? (HashAlgorithm)MD5.Create() : SHA1.Create())
                hash = algorithm.ComputeHash(data);

            // most bytes from the hash are copied straight to the bytes of the new GUID (steps 5-7, 9, 11-12)
            byte[] newGuid = new byte[16];
            Array.Copy(hash, 0, newGuid, 0, 16);

            // set the four most significant bits (bits 12 through 15) of the time_hi_and_version field to the appropriate 4-bit version number from Section 4.1.3 (step 8)
            newGuid[6] = (byte)((newGuid[6] & 0x0F) | (version << 4));

            // set the two most significant bits (bits 6 and 7) of the clock_seq_hi_and_reserved to zero and one, respectively (step 10)
            newGuid[8] = (byte)((newGuid[8] & 0x3F) | 0x80);

            // convert the resulting UUID to local byte order (step 13)
            SwapByteOrder(newGuid);
            return new Guid(newGuid);
        }

        private static void SwapByteOrder(byte[] guid)
        {
            SwapBytes(guid, 0, 3);
            SwapBytes(guid, 1, 2);
            SwapBytes(guid, 4, 5);
            SwapBytes(guid, 6, 7);
        }

        private static void SwapBytes(byte[] guid, int left, int right)
        {
            byte temp = guid[left];
            guid[left] = guid[right];
            guid[right] = temp;
        }
    }

    class LabelMetadata : MetaMetadata
    {
        public LabelMetadata() : base(typeof(Label))
        {
        }

        protected override AttributeMetadata[] GetAttributes()
        {
            var attributes = new List<AttributeMetadata>(base.GetAttributes());
            attributes.Add(new UniqueIdentifierAttributeMetadata("LabelId") { LogicalName = "labelid" });
            return attributes.ToArray();
        }
    }

    class LocalizedLabelMetadata : MetaMetadata
    {
        public LocalizedLabelMetadata() : base(typeof(LocalizedLabel))
        {
        }

        protected override AttributeMetadata[] GetAttributes()
        {
            var attributes = new List<AttributeMetadata>(base.GetAttributes());
            attributes.Add(new LookupAttributeMetadata { SchemaName = "LabelId", LogicalName = "labelid", Targets = new[] { "label" } });
            return attributes.ToArray();
        }

        public override Entity GetEntity(object obj, Guid? id, Entity parent)
        {
            var entity = base.GetEntity(obj, id, parent);
            entity["labelid"] = parent.ToEntityReference();
            return entity;
        }
    }

    class OptionMetadataMetadata : MetaMetadata
    {
        public OptionMetadataMetadata() : base(typeof(OptionMetadata), "option")
        {
        }

        protected override AttributeMetadata[] GetAttributes()
        {
            var attributes = new List<AttributeMetadata>(base.GetAttributes());
            attributes.Add(new LookupAttributeMetadata { SchemaName = "OptionSetId", LogicalName = "optionsetid", Targets = new[] { "globaloptionset" } });
            return attributes.ToArray();
        }

        public override Entity GetEntity(object obj, Guid? id, Entity parent)
        {
            var entity = base.GetEntity(obj, id, parent);
            entity["optionsetid"] = parent.ToEntityReference();
            return entity;
        }
    }
}
