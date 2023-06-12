using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides well-known data provider IDs, sourced from the entitydataprovider table
    /// </summary>
    static class DataProviders
    {
        /// <summary>
        /// The data provider for elastic tables
        /// </summary>
        public static Guid ElasticDataProvider { get; } = new Guid("1d9bde74-9ebd-4da9-8ff5-aa74945b9f74");
    }
}
