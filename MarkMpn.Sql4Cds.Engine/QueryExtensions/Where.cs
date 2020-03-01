using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// First in the sequence - apply filter from the WHERE clause to the results produced by the raw FetchXML
    /// </summary>
    class Where : IQueryExtension
    {
        private readonly Func<Entity, bool> _predicate;

        public Where(Func<Entity, bool> predicate)
        {
            _predicate = predicate;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Where(_predicate);
        }
    }
}
