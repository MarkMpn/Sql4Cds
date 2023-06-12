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
            _cache["SampleMessage"] = new Message
            {
                Name = "SampleMessage",
                InputParameters = new List<MessageParameter>
                {
                    new MessageParameter
                    {
                        Name = "StringParam",
                        Position = 0,
                        Type = typeof(string)
                    }
                }.AsReadOnly(),
                OutputParameters = new List<MessageParameter>
                {
                    new MessageParameter
                    {
                        Name = "OutputParam1",
                        Position = 0,
                        Type = typeof(string)
                    },
                    new MessageParameter
                    {
                        Name = "OutputParam2",
                        Position = 1,
                        Type = typeof(int)
                    }
                }.AsReadOnly()
            };
            _cache["OtherMessage"] = new Message
            {
                Name = "OtherMessage",
                InputParameters = new List<MessageParameter>
                {
                    new MessageParameter
                    {
                        Name = "StringParam",
                        Position = 0,
                        Type = typeof(string)
                    }
                }.AsReadOnly(),
                OutputParameters = new List<MessageParameter>().AsReadOnly()
            };
        }

        public IEnumerable<Message> GetAllMessages()
        {
            return _cache.Values;
        }

        public bool TryGetValue(string name, out Message message)
        {
            return _cache.TryGetValue(name, out message);
        }

        public bool IsMessageAvailable(string entityLogicalName, string messageName)
        {
            return false;
        }
    }
}
