using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExecutionPlanNodeTests
    {
        private readonly IOrganizationService _org;
        private readonly IAttributeMetadataCache _metadata;

        public ExecutionPlanNodeTests()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            _org = context.GetOrganizationService();
            _metadata = new AttributeMetadataCache(_org);
        }

        [TestMethod]
        public void ConstantScanTest()
        {
            var node = new ConstantScanNode
            {
                Values =
                {
                    new Entity
                    {
                        ["firstname"] = "Mark"
                    }
                },
                Schema =
                {
                    ["firstname"] = typeof(string)
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null).ToArray();

            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", results[0]["firstname"]);
        }

        [TestMethod]
        public void FilterNodeTest()
        {
            var node = new FilterNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["firstname"] = "Mark" },
                        new Entity { ["firstname"] = "Joe" }
                    },
                    Schema =
                    {
                        ["firstname"] = typeof(string)
                    }
                },
                Filter = new BooleanComparisonExpression
                {
                    FirstExpression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = "firstname" }
                            }
                        }
                    },
                    ComparisonType = BooleanComparisonType.Equals,
                    SecondExpression = new StringLiteral { Value = "Mark" }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null).ToArray();

            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", results[0]["firstname"]);
        }

        [TestMethod]
        public void MergeJoinInnerTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["f.key"] = 1, ["f.firstname"] = "Mark" },
                        new Entity { ["f.key"] = 2, ["f.firstname"] = "Joe" }
                    },
                    Schema =
                    {
                        ["f.key"] = typeof(int),
                        ["f.firstname"] = typeof(string)
                    }
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Carrington" },
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Twain" },
                        new Entity { ["l.key"] = 3, ["l.lastname"] = "Webber" }
                    },
                    Schema =
                    {
                        ["l.key"] = typeof(int),
                        ["l.lastname"] = typeof(string)
                    }
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.Inner
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null).ToArray();

            Assert.AreEqual(2, results.Length);
            Assert.AreEqual("Mark", results[0]["f.firstname"]);
            Assert.AreEqual("Carrington", results[0]["l.lastname"]);
            Assert.AreEqual("Mark", results[1]["f.firstname"]);
            Assert.AreEqual("Twain", results[1]["l.lastname"]);
        }

        [TestMethod]
        public void MergeJoinLeftOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["f.key"] = 1, ["f.firstname"] = "Mark" },
                        new Entity { ["f.key"] = 2, ["f.firstname"] = "Joe" }
                    },
                    Schema =
                    {
                        ["f.key"] = typeof(int),
                        ["f.firstname"] = typeof(string)
                    }
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Carrington" },
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Twain" },
                        new Entity { ["l.key"] = 3, ["l.lastname"] = "Hamill" }
                    },
                    Schema =
                    {
                        ["l.key"] = typeof(int),
                        ["l.lastname"] = typeof(string)
                    }
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.LeftOuter
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", results[0]["f.firstname"]);
            Assert.AreEqual("Carrington", results[0]["l.lastname"]);
            Assert.AreEqual("Mark", results[1]["f.firstname"]);
            Assert.AreEqual("Twain", results[1]["l.lastname"]);
            Assert.AreEqual("Joe", results[2]["f.firstname"]);
            Assert.AreEqual(null, results[2]["l.lastname"]);
        }

        [TestMethod]
        public void MergeJoinRightOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["f.key"] = 1, ["f.firstname"] = "Mark" },
                        new Entity { ["f.key"] = 2, ["f.firstname"] = "Joe" }
                    },
                    Schema =
                    {
                        ["f.key"] = typeof(int),
                        ["f.firstname"] = typeof(string)
                    }
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Carrington" },
                        new Entity { ["l.key"] = 1, ["l.lastname"] = "Twain" },
                        new Entity { ["l.key"] = 3, ["l.lastname"] = "Hamill" }
                    },
                    Schema =
                    {
                        ["l.key"] = typeof(int),
                        ["l.lastname"] = typeof(string)
                    }
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.RightOuter
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", results[0]["f.firstname"]);
            Assert.AreEqual("Carrington", results[0]["l.lastname"]);
            Assert.AreEqual("Mark", results[1]["f.firstname"]);
            Assert.AreEqual("Twain", results[1]["l.lastname"]);
            Assert.AreEqual(null, results[2]["f.firstname"]);
            Assert.AreEqual("Hamill", results[2]["l.lastname"]);
        }
    }
}
