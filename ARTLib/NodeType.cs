using System;

namespace ARTLib
{
    // Node Header
    // NodeType                   0    1
    // Child Count-1              1    1
    // Key Prefix Length          2    2
    // Reference Count            4    4
    // Recursive Child Count      8    8

    [Flags]
    enum NodeType : byte
    {
        NodeLeaf = 0,
        Node1 = 1,
        Node4 = 2,
        Node16 = 3,
        Node48 = 4,
        Node256 = 5,
        NodeSizeMask = 7,
        Has12BPtrs = 8,
        NodeSizePtrMask = 15,
        IsLeaf = 16
    }
}
