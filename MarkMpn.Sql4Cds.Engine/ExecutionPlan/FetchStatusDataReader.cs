﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchStatusDataReader : DbDataReader
    {
        private readonly DbDataReader _reader;
        private readonly NodeExecutionContext _context;
        private readonly IDisposable _timer;

        public FetchStatusDataReader(DbDataReader reader, NodeExecutionContext context, IDisposable timer)
        {
            _reader = reader;
            _context = context;
            _timer = timer;
        }

        public override object this[int ordinal] => _reader[ordinal];

        public override object this[string name] => _reader[name];

        public override int Depth => _reader.Depth;

        public override int FieldCount => _reader.FieldCount;

        public override bool HasRows => _reader.HasRows;

        public override bool IsClosed => _reader.IsClosed;

        public override int RecordsAffected => _reader.RecordsAffected;

        public override bool GetBoolean(int ordinal)
        {
            return _reader.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return _reader.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return _reader.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _reader.GetDataTypeName(ordinal);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return _reader.GetDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return _reader.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return _reader.GetDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return _reader.GetEnumerator();
        }

        public override Type GetFieldType(int ordinal)
        {
            return _reader.GetFieldType(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return _reader.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return _reader.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return _reader.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return _reader.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return _reader.GetInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return _reader.GetName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return _reader.GetOrdinal(name);
        }

        public override string GetString(int ordinal)
        {
            return _reader.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return _reader.GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            return _reader.GetValues(values);
        }

        public override bool IsDBNull(int ordinal)
        {
            return _reader.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return _reader.NextResult();
        }

        public override bool Read()
        {
            if (_reader.Read())
            {
                _context.ParameterValues["@@FETCH_STATUS"] = (SqlInt32)0;
                return true;
            }

            return false;
        }

        public override void Close()
        {
            _reader.Close();
            _timer?.Dispose();
        }

        protected override void Dispose(bool disposing)
        {
            ((IDisposable)_reader).Dispose();
        }

        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            return _reader.GetProviderSpecificFieldType(ordinal);
        }

        public override object GetProviderSpecificValue(int ordinal)
        {
            return _reader.GetProviderSpecificValue(ordinal);
        }

        public override int GetProviderSpecificValues(object[] values)
        {
            return _reader.GetProviderSpecificValues(values);
        }

        public override DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }

        public override int VisibleFieldCount => _reader.VisibleFieldCount;

        public override Stream GetStream(int ordinal)
        {
            return _reader.GetStream(ordinal);
        }
    }
}
