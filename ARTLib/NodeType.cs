using System;

namespace ARTLib
{
    // Node Header
    // NodeType                   0    1
    // Child Count-1              1    1
    // Key Prefix Length          2    2
    // Reference Count            4    4
    // Recursive Child Count      8    8  !NodeLeaf
    // Value Length               8    4  NodeLeaf

    [Flags]
    enum NodeType: byte
    {
        NodeLeaf=8,
        Node1=0, //< cannot Has12BPtrs
        Node4=1,
        Node16=2,
        Node48=3,
        Node256=4,
        NodeSizeMask=7,
        Has12BPtrs=8,
        NodeSizePtrMask = 15
    }
}
