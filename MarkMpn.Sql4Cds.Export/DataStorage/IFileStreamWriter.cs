//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Export.Contracts;


namespace MarkMpn.Sql4Cds.Export.DataStorage
{
    /// <summary>
    /// Interface for a object that writes to a filesystem wrapper
    /// </summary>
    public interface IFileStreamWriter : IDisposable
    {
        int WriteRow(StorageDataReader dataReader);
        void WriteRow(IList<DbCellValue> row, IReadOnlyList<DbColumnWrapper> columns);
        void Seek(long offset);
        void FlushBuffer();
    }
}
