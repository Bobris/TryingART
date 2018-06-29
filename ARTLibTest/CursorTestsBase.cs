using ARTLib;
using System;
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

        [Fact]
        public void CanInsertFirstData()
        {
            var val = GetSampleValue();
            _cursor.Upsert(new byte[] { 123 }, val);
            Assert.Equal(1, _cursor.GetKeyLength());
            Assert.Equal(new byte[] { 123 }, _cursor.FillByKey(new byte[1]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val.ToArray(), _cursor.FillByValue(new byte[val.Length]).ToArray());
        }
    }
}
