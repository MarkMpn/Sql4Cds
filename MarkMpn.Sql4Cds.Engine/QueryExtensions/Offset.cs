using Microsoft.Xrm.Sdk;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    class Offset : IQueryExtension
    {
        private readonly int _offset;
        private readonly int _fetch;

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
