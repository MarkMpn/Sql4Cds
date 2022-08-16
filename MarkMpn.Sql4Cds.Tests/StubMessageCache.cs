using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Tests
{
    class StubMessageCache : IMessageCache
    {
        private readonly Dictionary<string, Message> _cache;

        public StubMessageCache()
        {
            _cache = new Dictionary<string, Message>(StringComparer.OrdinalIgnoreCase);
        }

        public Message this[string name] => _cache[name];

        public IEnumerable<Message> GetAllMessages()
        {
            return _cache.Values;
        }

        public bool TryGetValue(string name, out Message message)
        {
            return _cache.TryGetValue(name, out message);
        }
    }
}
