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

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).ToArray();

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

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).ToArray();

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

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).ToArray();

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

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).ToArray();

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

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", results[0]["f.firstname"]);
            Assert.AreEqual("Carrington", results[0]["l.lastname"]);
            Assert.AreEqual("Mark", results[1]["f.firstname"]);
            Assert.AreEqual("Twain", results[1]["l.lastname"]);
            Assert.AreEqual(null, results[2]["f.firstname"]);
            Assert.AreEqual("Hamill", results[2]["l.lastname"]);
        }

        [TestMethod]
        public void AssertionTest()
        {
            var node = new AssertNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["name"] = "Mark" },
                        new Entity { ["name"] = "Carrington" }
                    }
                },
                Assertion = e => e.GetAttributeValue<string>("name") == "Mark",
                ErrorMessage = "Only Mark is allowed"
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null).GetEnumerator();

            Assert.IsTrue(results.MoveNext());
            Assert.AreEqual("Mark", results.Current.GetAttributeValue<string>("name"));

            var ex = Assert.ThrowsException<QueryExecutionException>(() => results.MoveNext());
            Assert.AreEqual(node.ErrorMessage, ex.Message);
        }

        [TestMethod]
        public void ComputeScalarTest()
        {
            var node = new ComputeScalarNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = 1, ["value2"] = 2 },
                        new Entity { ["value1"] = 3, ["value2"] = 4 }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(int),
                        ["value2"] = typeof(int)
                    }
                },
                Columns =
                {
                    ["mul"] = new BinaryExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value1" } }
                            }
                        },
                        BinaryExpressionType = BinaryExpressionType.Multiply,
                        SecondExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value2" } }
                            }
                        }
                    }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("mul"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { 2, 12 }, results);
        }

        [TestMethod]
        public void DistinctTest()
        {
            var node = new DistinctNode
            {
                Columns = { "value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = 1, ["value2"] = 1 },
                        new Entity { ["value1"] = 3, ["value2"] = 2 },
                        new Entity { ["value1"] = 1, ["value2"] = 3 }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(int),
                        ["value2"] = typeof(int)
                    }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("value1"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 3 }, results);
        }

        [TestMethod]
        public void DistinctCaseInsensitiveTest()
        {
            var node = new DistinctNode
            {
                Columns = { "value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = "hello", ["value2"] = 1 },
                        new Entity { ["value1"] = "world", ["value2"] = 2 },
                        new Entity { ["value1"] = "Hello", ["value2"] = 3 }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(string),
                        ["value2"] = typeof(int)
                    }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<string>("value1"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { "hello", "world" }, results);
        }

        [TestMethod]
        public void SortNodeTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = "hello", ["value2"] = 1, ["expectedorder"] = 2 },
                        new Entity { ["value1"] = "world", ["value2"] = 2, ["expectedorder"] = 3 },
                        new Entity { ["value1"] = "Hello", ["value2"] = 3, ["expectedorder"] = 1 }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(string),
                        ["value2"] = typeof(int),
                        ["expectedorder"] = typeof(int)
                    }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("expectedorder"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, results);
        }

        [TestMethod]
        public void SortNodePresortedTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                PresortedCount = 1,
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = "hello", ["value2"] = 1, ["expectedorder"] = 1 },
                        new Entity { ["value1"] = "world", ["value2"] = 2, ["expectedorder"] = 2 },
                        new Entity { ["value1"] = "Hello", ["value2"] = 3, ["expectedorder"] = 4 },
                        new Entity { ["value1"] = "Hello", ["value2"] = 4, ["expectedorder"] = 3 }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(string),
                        ["value2"] = typeof(int),
                        ["expectedorder"] = typeof(int)
                    }
                }
            };

            var results = node.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("expectedorder"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, results);
        }

        [TestMethod]
        public void TableSpoolTest()
        {
            var source = new ConstantScanNode
            {
                Values =
                {
                    new Entity{["value1"] = 1},
                    new Entity{["value1"] = 2}
                },
                Schema =
                {
                    ["value1"] = typeof(int)
                }
            };

            var spool = new TableSpoolNode { Source = source };

            var results1 = spool.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("value1"))
                .ToArray();

            var results2 = spool.Execute(_org, _metadata, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<int>("value1"))
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2 }, results1);
            CollectionAssert.AreEqual(new[] { 1, 2 }, results2);
            Assert.AreEqual(1, source.ExecutionCount);
        }
    }
}
