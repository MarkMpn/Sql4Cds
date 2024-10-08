﻿using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class SqlVariantTests : FakeXrmEasyTestsBase
    {
        private readonly ExpressionExecutionContext _eec;

        public SqlVariantTests()
        {
            _eec = new ExpressionExecutionContext(
                session: new SessionContext(_localDataSources, new StubOptions()),
                options: new StubOptions(),
                parameterTypes: new Dictionary<string, DataTypeReference>(),
                parameterValues: new Dictionary<string, INullable>(),
                log: e => { },
                entity: null);
        }

        [TestMethod]
        public void NullSortsBeforeAllOtherValues()
        {
            Assert.IsTrue(SqlVariant.Null.CompareTo(SqlVariant.Null) == 0);
            Assert.IsTrue(SqlVariant.Null.CompareTo(new SqlVariant(DataTypeHelpers.Int, new SqlInt32(1), _eec)) < 0);
            Assert.IsTrue(new SqlVariant(DataTypeHelpers.Int, new SqlInt32(1), _eec).CompareTo(SqlVariant.Null) > 0);
        }

        [TestMethod]
        public void NullDoesNotEqualNull()
        {
            Assert.AreEqual(SqlVariant.Null == SqlVariant.Null, (SqlBoolean)false);
        }

        [TestMethod]
        public void ValuesFromDifferentFamiliesAreNotEqual()
        {
            Assert.AreEqual(new SqlVariant(DataTypeHelpers.VarChar(1, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString("1"), _eec) == new SqlVariant(DataTypeHelpers.Int, new SqlInt32(1), _eec), (SqlBoolean)false);
        }

        [TestMethod]
        public void ValuesFromDifferentTypesInSameFamilyAreEqual()
        {
            Assert.AreEqual(new SqlVariant(DataTypeHelpers.BigInt, new SqlInt64(1), _eec) == new SqlVariant(DataTypeHelpers.Int, new SqlInt32(1), _eec), (SqlBoolean)true);
        }

        [TestMethod]
        public void SortsAccordingToDataTypeFamilies()
        {
            var variant = SqlVariant.Null;
            var dt = new SqlVariant(DataTypeHelpers.DateTime, new SqlDateTime(2000, 1, 1), _eec);
            var approx = new SqlVariant(DataTypeHelpers.Float, new SqlSingle(1), _eec);
            var exact = new SqlVariant(DataTypeHelpers.Int, new SqlInt32(1), _eec);
            var ch = new SqlVariant(DataTypeHelpers.VarChar(10, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString("1"), _eec);
            var nch = new SqlVariant(DataTypeHelpers.NVarChar(10, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString("1"), _eec);
            var bin = new SqlVariant(DataTypeHelpers.VarBinary(10), new SqlBinary(new byte[] { 1 }), _eec);
            var guid = new SqlVariant(DataTypeHelpers.UniqueIdentifier, new SqlGuid(Guid.NewGuid()), _eec);

            var list = new List<SqlVariant> { variant, dt, approx, exact, ch, nch, bin, guid };
            var rnd = new Random();

            // Randomize the list, then sort it again
            var randomized = list.OrderBy(obj => rnd.Next()).ToList();
            randomized.Sort();

            Assert.IsTrue(list.SequenceEqual(new[] { variant, dt, approx, exact, ch, nch, bin, guid }));
        }
    }
}
