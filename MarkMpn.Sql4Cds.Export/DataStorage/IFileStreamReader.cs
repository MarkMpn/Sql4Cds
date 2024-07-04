//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Export.Contracts;

namespace MarkMpn.Sql4Cds.Export.DataStorage
{
    /// <summary>
    /// Interface for a object that reads from the filesystem
    /// </summary>
    public interface IFileStreamReader : IDisposable
    {
        IList<DbCellValue> ReadRow(long offset, long rowId, IEnumerable<DbColumnWrapper> columns);
    }
}
