using System;
using System.Collections.Concurrent;

namespace ARTLib
{
    class LeakDetectorWrapperAllocator : IOffHeapAllocator, IDisposable
    {
        IOffHeapAllocator _wrapped;
        ConcurrentDictionary<IntPtr, IntPtr> _ptr2SizeMap = new ConcurrentDictionary<IntPtr, IntPtr>();

        public LeakDetectorWrapperAllocator(IOffHeapAllocator wrap)
        {
            _wrapped = wrap;
        }

        public (uint Count, ulong Size) QueryAllocations()
        {
            (uint Count, ulong Size) res = (0,0);
            foreach(var i in _ptr2SizeMap)
            {
                res = (res.Count + 1, res.Size + (ulong)i.Value.ToInt64());
            }
            return res;
        }

        public IntPtr Allocate(IntPtr size)
        {
            var res = _wrapped.Allocate(size);
            _ptr2SizeMap.TryAdd(res, size);
            return res;
        }

        public void Deallocate(IntPtr ptr)
        {
            if (!_ptr2SizeMap.TryRemove(ptr, out var size))
                throw new InvalidOperationException("Trying to free memory which is not allocated " + ptr.ToInt64());
            _wrapped.Deallocate(ptr);
        }

        public void Dispose()
        {
            foreach (var i in _ptr2SizeMap)
            {
                _wrapped.Deallocate(i.Key);
            }
        }
    }
}
