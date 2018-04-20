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
                case NodeType.NodeLeaf: return 12;
                case NodeType.Node1: return 16;
                case NodeType.Node4: return 16 + 4 + 4 * 8;
                case NodeType.Node4 | NodeType.Has12BPtrs:  return 16 + 4 + 4 * 12;
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
        internal static ref NodeHeader Ptr2NodeHeader(ulong pointerInt)
        {
            unsafe
            {
                return ref *(NodeHeader*)pointerInt;
            };
        }
    }
}
