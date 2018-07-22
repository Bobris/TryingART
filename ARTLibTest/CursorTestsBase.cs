using ARTLib;
using System;
using System.Collections.Generic;
using Xunit;

namespace ARTLibTest
{
    public abstract class CursorTestsBase : IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;
        IRootNode _root;
        ICursor _cursor;

        public abstract bool Is12 { get; }
        public abstract ReadOnlySpan<byte> GetSampleValue(int index = 0);

        public CursorTestsBase()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            _root = ARTImpl.CreateEmptyRoot(_allocator, Is12);
            _cursor = _root.CreateCursor();
        }

        public void Dispose()
        {
            _root.Dispose();
            var leaks = _allocator.QueryAllocations();
            Assert.Equal(0ul, leaks.Count);
        }

        public static IEnumerable<object[]> InterestingValues()
        {
            for (int i = 0; i < 12; i++)
            {
                for (int j = i + 1; j < 12; j++)
                {
                    yield return new object[] { i, j };
                }
            }
        }

        [Fact]
        public void InvalidCursorBehaviour()
        {
            Assert.Equal(-1, _cursor.CalcIndex());
        }

        public static IEnumerable<object[]> SampleKeys =>
        new List<object[]>
        {
            new object[] { new byte[] { 1, 2, 3 } },
            new object[] { new byte[] { 1 } },
            new object[] { new byte[] { } },
            new object[] { new byte[] { 5, 4, 3, 2, 1, 0, 255, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 } },
        };

        [Theory]
        [MemberData(nameof(SampleKeys))]
        public void CanInsertFirstData(byte[] key)
        {
            Assert.Equal(0, _root.GetCount());
            var val = GetSampleValue();
            Assert.True(_cursor.Upsert(key, val));
            Assert.Equal(1, _root.GetCount());
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val.ToArray(), _cursor.GetValue().ToArray());
        }

        [Theory]
        [MemberData(nameof(InterestingValues))]
        public void CanChangeValues(int valueIndex1, int valueIndex2)
        {
            var val = GetSampleValue(valueIndex1).ToArray();
            var val2 = GetSampleValue(valueIndex2).ToArray();
            _cursor.Upsert(new byte[] { 1 }, val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val2);
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            using (var snapshot = _root.Snapshot())
            {
                _cursor.WriteValue(val2);
                Assert.Equal(val2.Length, _cursor.GetValueLength());
                Assert.Equal(val2, _cursor.GetValue().ToArray());
            }
        }

        public static IEnumerable<object[]> SampleKeys2 =>
        new List<object[]>
        {
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 , 2 } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 , 2, 2 } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 , 2, 4 } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 , 1 } },
            new object[] { new byte[] { 1, 2, 3 }, new byte[] { 1 , 3 } },
        };

        [Theory]
        [MemberData(nameof(SampleKeys2))]
        public void CanInsertSecondKey(byte[] key, byte[] key2)
        {
            var val = GetSampleValue().ToArray();
            var val2 = GetSampleValue(3).ToArray();
            Assert.True(_cursor.Upsert(key, val));
            Assert.True(_cursor.Upsert(key2, val2));
            Assert.Equal(key2.Length, _cursor.GetKeyLength());
            Assert.Equal(key2, _cursor.FillByKey(new byte[key2.Length]).ToArray());
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            Assert.Equal(2, _root.GetCount());
        }

        [Theory]
        [MemberData(nameof(SampleKeys))]
        public void SecondUpsertWithSameKeyJustOverwriteValue(byte[] key)
        {
            var val = GetSampleValue().ToArray();
            var val2 = GetSampleValue(3).ToArray();
            Assert.True(_cursor.Upsert(key, val));
            Assert.False(_cursor.Upsert(key, val2));
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            Assert.Equal(1, _root.GetCount());
        }

        [Fact]
        public void MultipleInsertsInSingleTransaction()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[1];
            for (var i = 0; i < 256; i++)
            {
                key[0] = (byte)i;
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte)j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
            }
            key = new byte[2];
            key[0] = 20;
            for (var j = 0; j < 256; j++)
            {
                key[1] = (byte)j;
                Assert.False(_cursor.FindExact(key));
            }
            for (var i = 0; i < 256; i++)
            {
                key[1] = (byte)i;
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(20 + 1 + i, _cursor.CalcIndex());
                Assert.Equal(256 + i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte)j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
            }
        }

        [Fact]
        public void MultipleInsertsInMultipleTransactions()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[1];
            for (var i = 0; i < 256; i++)
            {
                key[0] = (byte)i;
                var snapshot = _root.Snapshot();
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte)j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
                var snapshotCursor = snapshot.CreateCursor();
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte)j;
                    Assert.Equal(j < i, snapshotCursor.FindExact(key));
                }
                snapshot.Dispose();
            }
            key = new byte[2];
            key[0] = 20;
            for (var j = 0; j < 256; j++)
            {
                key[1] = (byte)j;
                Assert.False(_cursor.FindExact(key));
            }
            for (var i = 0; i < 256; i++)
            {
                key[1] = (byte)i;
                var snapshot = _root.Snapshot();
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(256 + i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte)j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
                var snapshotCursor = snapshot.CreateCursor();
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte)j;
                    Assert.Equal(j < i, snapshotCursor.FindExact(key));
                }
                snapshot.Dispose();
            }
        }
    }
}
