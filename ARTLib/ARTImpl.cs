using System;
using System.Collections.Generic;

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("ARTLibTest")]

namespace ARTLib
{
    public class ARTImpl
    {
        readonly IOffHeapAllocator _allocator;
        readonly bool _max12ByteValues;

        internal ARTImpl(IOffHeapAllocator allocator, bool max12ByteValues)
        {
            _allocator = allocator;
            _max12ByteValues = max12ByteValues;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator, bool max12ByteValues)
        {
            return new RootNode(new ARTImpl(allocator, max12ByteValues));
        }

        internal IntPtr AllocateNode(NodeType nodeType, uint keyPrefixLength, uint valueLength)
        {
            IntPtr node;
            if (_max12ByteValues)
            {
                nodeType = nodeType | NodeType.Has12BPtrs;
                var size = NodeUtils.BaseSize(nodeType) + NodeUtils.AlignUIntUpInt32(keyPrefixLength) + 12;
                if (keyPrefixLength >= 0xffff) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            else
            {
                nodeType = nodeType | NodeType.Has12BPtrs;
                var size = NodeUtils.BaseSize(nodeType) + keyPrefixLength + valueLength;
                if (keyPrefixLength >= 0xffff) size += 4;
                if ((nodeType & NodeType.IsLeaf) != 0) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            var nodeHeader = NodeUtils.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._childCount = 0;
            nodeHeader._keyPrefixLength = (ushort)(keyPrefixLength >= 0xffffu ? 0xffffu : keyPrefixLength);
            nodeHeader._referenceCount = 1;
            nodeHeader._recursiveChildCount = 1;
            return node;
        }

        internal void Dereference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            var nodeHeader = NodeUtils.Ptr2NodeHeader(node);
            if (!nodeHeader.Dereference()) return;
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                    // does not contain any pointers
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

        internal bool Upsert(RootNode rootNode, List<CursorItem> stack, Span<byte> key, Span<byte> content)
        {
            if (rootNode._root == IntPtr.Zero)
            {
                var cursorItem = new CursorItem(AllocateNode(NodeType.NodeLeaf, (uint)key.Length, (uint)content.Length), (uint)key.Length);
                stack.Add(cursorItem);
                rootNode._root = cursorItem._node;
                var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(cursorItem._node);
                unsafe { key.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                (size, ptr) = NodeUtils.GetValueSizeAndPtr(cursorItem._node);
                unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                return true;
            }
            throw new NotImplementedException();
        }
    }
}
