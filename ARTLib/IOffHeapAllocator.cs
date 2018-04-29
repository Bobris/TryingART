using System;

namespace ARTLib
{
    public interface IOffHeapAllocator
    {
        IntPtr Allocate(IntPtr size);
        void Deallocate(IntPtr ptr);
    }
}
