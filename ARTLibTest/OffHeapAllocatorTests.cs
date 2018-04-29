using ARTLib;
using System;
using Xunit;

namespace ARTLibTest
{
    public class OffHeapAllocatorTests
    {
        [Fact]
        public void AllocateReturnsPointerCanWriteInto()
        {
            var allocator = new HGlobalAllocator();
            var ptr = allocator.Allocate((IntPtr)4);
            unsafe
            {
                *(int*)ptr = 0x12345678;
                Assert.Equal(0x12345678, *(int*)ptr);
            }
            allocator.Deallocate(ptr);
        }

        [Fact]
        public void LeakDetectorWorks()
        {
            var allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            var ptr1 = allocator.Allocate((IntPtr)4);
            var ptr2 = allocator.Allocate((IntPtr)8);
            var ptr3 = allocator.Allocate((IntPtr)16);
            allocator.Deallocate(ptr2);
            var leaks = allocator.QueryAllocations();
            Assert.Equal(2u, leaks.Count);
            Assert.Equal(20ul, leaks.Size);
            allocator.Deallocate(ptr1);
            allocator.Deallocate(ptr3);
            Assert.Throws<InvalidOperationException>(() => allocator.Deallocate(ptr1));
        }
    }
}
