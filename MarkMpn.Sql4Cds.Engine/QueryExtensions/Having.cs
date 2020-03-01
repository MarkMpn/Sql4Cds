using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Fourth, apply any custom filters from the HAVING clause
    /// </summary>
    class Having : IQueryExtension
    {
        private readonly Func<Entity, bool> _predicate;

        public Having(Func<Entity, bool> predicate)
        {
            _predicate = predicate;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Where(_predicate);
        }
    }
}
