using ARTLib;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ARTLibTest
{
    public class CursorTests : IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;
        ARTImpl _impl;
        RootNode _root;
        ICursor _cursor;

        public CursorTests()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            _impl = new ARTImpl(_allocator, false);
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
            _cursor.Upsert(new byte[] { 0 }, new byte[] { 1 });
        }
    }
}
