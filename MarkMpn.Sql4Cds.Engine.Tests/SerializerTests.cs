using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class SerializerTests
    {
        class RootNode : IRootExecutionPlanNode
        {
            public SqlString Value { get; set; }
            public string Sql { get; set; }
            public int Index { get; set; }
            public int Length { get; set; }
            public int LineNumber { get; set; }

            public IExecutionPlanNode Parent => null;

            public int ExecutionCount => 0;

            public TimeSpan Duration => TimeSpan.Zero;

            public IEnumerable<IExecutionPlanNode> GetSources()
            {
                throw new NotImplementedException();
            }
        }

        [TestMethod]
        public void SerializesNullString()
        {
            var node = new RootNode();
            var serialized = ExecutionPlanSerializer.Serialize(node);
            var deserialized = (RootNode)ExecutionPlanSerializer.Deserialize(serialized);

            Assert.AreEqual(SqlString.Null, deserialized.Value);
        }

        [TestMethod]
        public void SerializesString()
        {
            var node = new RootNode();
            node.Value = "hello world";
            var serialized = ExecutionPlanSerializer.Serialize(node);
            var deserialized = (RootNode)ExecutionPlanSerializer.Deserialize(serialized);

            Assert.AreEqual(node.Value, deserialized.Value);
        }
    }
}
