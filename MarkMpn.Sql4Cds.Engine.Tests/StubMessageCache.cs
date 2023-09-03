using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
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
            _cache["SetState"] = new Message
            {
                Name = "SetState",
                InputParameters = new List<MessageParameter>
                {
                    new MessageParameter
                    {
                        Name = "EntityMoniker",
                        Position = 0,
                        Type = typeof(EntityReference)
                    },
                    new MessageParameter
                    {
                        Name = "State",
                        Position = 1,
                        Type = typeof(OptionSetValue)
                    },
                    new MessageParameter
                    {
                        Name = "Status",
                        Position = 2,
                        Type = typeof(OptionSetValue)
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
