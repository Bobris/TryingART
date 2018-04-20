using System;

namespace ARTLib
{
    interface IOffHeapAllocator
    {
        IntPtr Allocate(IntPtr size);
        void Deallocate(IntPtr ptr);
    }
}
