using Microsoft.Xrm.Sdk;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Implements the OFFSET clause
    /// </summary>
    class Offset : IQueryExtension
    {
        private readonly int _offset;
        private readonly int _fetch;

        /// <summary>
        /// Creates a new <see cref="Offset"/>
        /// </summary>
        /// <param name="offset">The number of records to skip</param>
        /// <param name="fetch">The number of records to return</param>
        public Offset(int offset, int fetch)
        {
            _offset = offset;
            _fetch = fetch;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Skip(_offset).Take(_fetch);
        }
    }
}
