using Microsoft.Xrm.Sdk;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Take only the TOP x records
    /// </summary>
    class Top : IQueryExtension
    {
        private readonly int _top;

        /// <summary>
        /// Creates a new <see cref="Top"/>
        /// </summary>
        /// <param name="top">The number of records to retrieve</param>
        public Top(int top)
        {
            _top = top;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Take(_top);
        }
    }
}
