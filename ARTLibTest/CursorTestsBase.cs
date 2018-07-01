﻿using ARTLib;
using System;
using System.Collections.Generic;
using Xunit;

namespace ARTLibTest
{
    public abstract class CursorTestsBase : IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;
        ARTImpl _impl;
        RootNode _root;
        ICursor _cursor;

        public abstract bool Is12 { get; }
        public abstract ReadOnlySpan<byte> GetSampleValue(int index = 0);

        public CursorTestsBase()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            _impl = new ARTImpl(_allocator, Is12);
            _root = new RootNode(_impl);
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
            var val = GetSampleValue();
            _cursor.Upsert(key, val);
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val.ToArray(), _cursor.FillByValue(new byte[val.Length]).ToArray());
        }

        [Theory]
        [MemberData(nameof(InterestingValues))]
        public void CanChangeValues(int valueIndex1, int valueIndex2)
        {
            var val = GetSampleValue(valueIndex1).ToArray();
            var val2 = GetSampleValue(valueIndex2).ToArray();
            _cursor.Upsert(new byte[] { 1 }, val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.FillByValue(new byte[val.Length]).ToArray());
            _cursor.WriteValue(val2);
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.FillByValue(new byte[val2.Length]).ToArray());
            _cursor.WriteValue(val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.FillByValue(new byte[val.Length]).ToArray());
            using (var snapshot = _root.Snapshot())
            {
                _cursor.WriteValue(val2);
                Assert.Equal(val2.Length, _cursor.GetValueLength());
                Assert.Equal(val2, _cursor.FillByValue(new byte[val2.Length]).ToArray());
            }
        }
    }
}