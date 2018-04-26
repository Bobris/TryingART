using System;
using System.Runtime.CompilerServices;

namespace ARTLib
{
    static class NodeUtils
    {
        internal static int BaseSize(NodeType nodeType)
        {
            switch (nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf: return 16;
                case NodeType.Node1: return 16 + 8;
                case NodeType.Node1 | NodeType.Has12BPtrs: return 16 + 12;
                case NodeType.Node4: return 16 + 4 + 4 * 8;
                case NodeType.Node4 | NodeType.Has12BPtrs: return 16 + 4 + 4 * 12;
                case NodeType.Node16: return 16 + 16 + 16 * 8;
                case NodeType.Node16 | NodeType.Has12BPtrs: return 16 + 16 + 16 * 12;
                case NodeType.Node48: return 16 + 256 + 48 * 8;
                case NodeType.Node48 | NodeType.Has12BPtrs: return 16 + 256 + 48 * 12;
                case NodeType.Node256: return 16 + 256 * 8;
                case NodeType.Node256 | NodeType.Has12BPtrs: return 16 + 256 * 12;
                default: throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr Read12Ptr(IntPtr childPtr)
        {
            unsafe
            {
                return *(IntPtr*)(childPtr + 4);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPtr12Ptr(IntPtr childPtr)
        {
            unsafe
            {
                return *(uint*)childPtr == uint.MaxValue;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPtrPtr(IntPtr child)
        {
            return ((int)child & 1) == 0;
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr ReadPtr(IntPtr ptr)
        {
            unsafe
            {
                return *(IntPtr*)ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref NodeHeader Ptr2NodeHeader(IntPtr pointerInt)
        {
            unsafe
            {
                return ref *(NodeHeader*)pointerInt;
            };
        }
    }
}
