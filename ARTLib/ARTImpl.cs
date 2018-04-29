using System;

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("ARTLibTest")]

namespace ARTLib
{
    public class ARTImpl
    {
        readonly IOffHeapAllocator _allocator;

        ARTImpl(IOffHeapAllocator allocator)
        {
            _allocator = allocator;
        }

        IntPtr AllocateNode(NodeType nodeType, ushort keyPrefixLength, uint valueLength)
        {
            var node = _allocator.Allocate((IntPtr)(NodeUtils.BaseSize(nodeType) + keyPrefixLength + valueLength));
            var nodeHeader = NodeUtils.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._keyPrefixLength = keyPrefixLength;
            nodeHeader._referenceCount = 1;
            return node;
        }

        void Dereference(IntPtr node)
        {
            if (node.ToInt64() == 0)
                return;
            var nodeHeader = NodeUtils.Ptr2NodeHeader(node);
            if (!nodeHeader.Dereference()) return;
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                    // does not contain any pointers
                    break;
                case NodeType.Node1:
                    {
                        var child = NodeUtils.ReadPtr(node + 16);
                        if (NodeUtils.IsPtrPtr(child))
                        {
                            Dereference(child);
                        }
                    }
                    break;
                case NodeType.Node1 | NodeType.Has12BPtrs:
                    {
                        var childPtr = node + 16;
                        if (NodeUtils.IsPtr12Ptr(childPtr))
                        {
                            Dereference(NodeUtils.Read12Ptr(childPtr));
                        }
                    }
                    break;
                case NodeType.Node4:
                    {
                        var p = node + 16 + 4;
                        for (var i = 0; i < 4; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node4 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 4;
                        for (var i = 0; i < 4; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node16:
                    {
                        var p = node + 16 + 16;
                        for (var i = 0; i < 16; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node16 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 16;
                        for (var i = 0; i < 16; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node48:
                    {
                        var p = node + 16 + 256;
                        for (var i = 0; i < 48; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node48 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 256;
                        for (var i = 0; i < 48; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node256:
                    {
                        var p = node + 16;
                        for (var i = 0; i < 256; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node256 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16;
                        for (var i = 0; i < 256; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                default: throw new InvalidOperationException();
            }
            _allocator.Deallocate(node);
        }
    }
}
