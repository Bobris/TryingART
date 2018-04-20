using System.Runtime.InteropServices;

namespace ARTLib
{
    [StructLayout(LayoutKind.Explicit, Pack = 8, Size = 16)]
    struct NodeHeader
    {
        [FieldOffset(0)]
        internal NodeType _nodeType;
        [FieldOffset(1)]
        internal byte _childCountM1;
        [FieldOffset(2)]
        internal ushort _keyPrefixLength;
        [FieldOffset(4)]
        internal int _referenceCount;
        [FieldOffset(8)]
        internal ulong _recursiveChildCount;
        [FieldOffset(8)]
        internal int _valueLength;
    }
}
