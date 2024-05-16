using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    class Sql4CdsParameterCollection : DbParameterCollection
    {
        private readonly ArrayList _parameters;
        private readonly object _syncRoot;

        public Sql4CdsParameterCollection()
        {
            _parameters = new ArrayList();
            _syncRoot = new object();
        }

        public override int Count => _parameters.Count;

        public override object SyncRoot => _syncRoot;

        public override int Add(object value)
        {
            return _parameters.Add(value);
        }

        public override void AddRange(Array values)
        {
            _parameters.AddRange(values);
        }

        public override void Clear()
        {
            _parameters.Clear();
        }

        public override bool Contains(object value)
        {
            return _parameters.Contains(value);
        }

        internal Dictionary<string, DataTypeReference> GetParameterTypes()
        {
            return _parameters
                .Cast<Sql4CdsParameter>()
                .ToDictionary(param => param.FullParameterName, param => param.GetDataType(), StringComparer.OrdinalIgnoreCase);
        }

        internal Dictionary<string, INullable> GetParameterValues()
        {
            return _parameters
                .Cast<Sql4CdsParameter>()
                .ToDictionary(param => param.FullParameterName, param => param.GetValue(), StringComparer.OrdinalIgnoreCase);
        }

        public override bool Contains(string value)
        {
            return _parameters.Cast<DbParameter>()
                .Any(p => p.ParameterName.Equals(value, StringComparison.OrdinalIgnoreCase));
        }

        public override void CopyTo(Array array, int index)
        {
            _parameters.CopyTo(array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            return _parameters.IndexOf(value);
        }

        public override int IndexOf(string parameterName)
        {
            return _parameters.Cast<DbParameter>()
                .Select((p, idx) => new { p.ParameterName, idx })
                .FirstOrDefault(p => p.ParameterName.EndsWith(parameterName, StringComparison.OrdinalIgnoreCase))
                ?.idx ?? -1;
        }

        public override void Insert(int index, object value)
        {
            _parameters.Insert(index, value);
        }

        public override void Remove(object value)
        {
            _parameters.Remove(value);
        }

        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var param = _parameters.Cast<DbParameter>()
                .Where(p => p.ParameterName.Equals(parameterName, StringComparison.OrdinalIgnoreCase))
                .SingleOrDefault();

            if (param != null)
                Remove(param);
        }

        protected override DbParameter GetParameter(int index)
        {
            return (DbParameter)_parameters[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            var param = _parameters.Cast<DbParameter>()
                .Where(p => p.ParameterName.Equals(parameterName, StringComparison.OrdinalIgnoreCase))
                .SingleOrDefault();

            return param;
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            _parameters[index] = value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            RemoveAt(parameterName);
            Add(value);
        }
    }
}
