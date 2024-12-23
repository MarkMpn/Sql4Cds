using System;
using System.Data.SqlTypes;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    public struct SqlEntityReference : INullable, IComparable
    {
        private readonly SqlGuid _guid;
        private readonly string _primaryIdAttribute;

        public SqlEntityReference(string dataSource, string logicalName, SqlGuid id)
        {
            DataSource = dataSource;
            LogicalName = logicalName;
            _guid = id;
            _primaryIdAttribute = null;
            PartitionId = null;
        }

        public SqlEntityReference(string dataSource, EntityMetadata metadata, SqlGuid id, string partitionId)
        {
            DataSource = dataSource;
            LogicalName = metadata.LogicalName;
            _guid = id;
            _primaryIdAttribute = metadata.PrimaryIdAttribute;
            PartitionId = partitionId;
        }

        public SqlEntityReference(string dataSource, EntityReference entityReference)
        {
            DataSource = dataSource;
            _primaryIdAttribute = null;
            PartitionId = null;

            if (entityReference == null)
            {
                _guid = SqlGuid.Null;
                LogicalName = null;
            }
            else
            {
                _guid = entityReference.Id;
                LogicalName = entityReference.LogicalName;

                if (entityReference.KeyAttributes.TryGetValue("partitionid", out var value))
                {
                    var primaryId = entityReference.KeyAttributes.Single(a => {
                        try
                        {
                            return a.Value is Guid || Guid.TryParse(a.Value as string, out var _);
                        }
                        catch (Exception _)
                        {
                            return false;
                        }
                    });
                    if (primaryId.Value is Guid)
                        _guid = (Guid)primaryId.Value;
                    else
                        _guid = new Guid(primaryId.Value as string);
                    _primaryIdAttribute = primaryId.Key;
                    PartitionId = value as string;
                }
            }
        }

        public static readonly SqlEntityReference Null = new SqlEntityReference(null, null);

        public string DataSource { get; }

        public string LogicalName { get; }

        public Guid Id => _guid.Value;

        public string PartitionId { get; set; }

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

            if (er.PartitionId != null)
            {
                var keyAttributes = new KeyAttributeCollection
                {
                    { er._primaryIdAttribute, er.Id },
                    { "partitionid", er.PartitionId }
                };

                return new EntityReference(er.LogicalName, keyAttributes);
            }

            return new EntityReference(er.LogicalName, er._guid.Value);
        }

        public static explicit operator Guid(SqlEntityReference er)
        {
            return er._guid.Value;
        }

        public static implicit operator SqlEntityReference(EntityReference er)
        {
            return new SqlEntityReference(null, er);
        }
    }
}
