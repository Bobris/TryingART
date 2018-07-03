using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("ARTLibTest")]

namespace ARTLib
{
    public class ARTImpl
    {
        internal readonly bool IsValue12;
        readonly IOffHeapAllocator _allocator;

        internal ARTImpl(IOffHeapAllocator allocator, bool isValue12)
        {
            _allocator = allocator;
            IsValue12 = isValue12;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator, bool isValue12)
        {
            return new RootNode(new ARTImpl(allocator, isValue12));
        }

        internal IntPtr AllocateNode(NodeType nodeType, uint keyPrefixLength, uint valueLength)
        {
            IntPtr node;
            int baseSize;
            if (IsValue12)
            {
                nodeType = nodeType | NodeType.Has12BPtrs;
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + NodeUtils.AlignUIntUpInt32(keyPrefixLength) + 12;
                if (keyPrefixLength >= 0xffff) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            else
            {
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + keyPrefixLength + valueLength;
                if (keyPrefixLength >= 0xffff) size += 4;
                if ((nodeType & NodeType.IsLeaf) != 0) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._childCount = 0;
            nodeHeader._keyPrefixLength = (ushort)(keyPrefixLength >= 0xffffu ? 0xffffu : keyPrefixLength);
            nodeHeader._referenceCount = 1;
            nodeHeader._recursiveChildCount = 1;
            if (keyPrefixLength >= 0xffffu)
            {
                unsafe { *(uint*)(node + baseSize).ToPointer() = keyPrefixLength; }
                baseSize += 4;
            }
            if (!IsValue12 && ((nodeType & NodeType.IsLeaf) != 0))
            {
                unsafe { *(uint*)(node + baseSize).ToPointer() = valueLength; }
            }
            return node;
        }

        internal IntPtr CloneNode(IntPtr nodePtr)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                unsafe { prefixSize = *(uint*)ptr; };
                ptr += sizeof(uint);
            }
            if ((header._nodeType & NodeType.IsLeaf) == NodeType.IsLeaf)
            {
                if ((header._nodeType & (NodeType.IsLeaf | NodeType.Has12BPtrs)) == NodeType.IsLeaf)
                {
                    unsafe { ptr += *(int*)ptr; };
                    ptr += sizeof(uint);
                    ptr += (int)prefixSize;
                }
                else
                {
                    ptr += (int)prefixSize;
                    ptr = NodeUtils.AlignPtrUpInt32(ptr);
                    ptr += 12;
                }
            }
            var size = (IntPtr)(ptr.ToInt64() - nodePtr.ToInt64());
            var newNode = _allocator.Allocate(size);
            unsafe
            {
                if (size.ToInt64() < uint.MaxValue)
                    Unsafe.CopyBlock(newNode.ToPointer(), nodePtr.ToPointer(), (uint)size);
                else
                {
                    byte* dst = (byte*)newNode.ToPointer();
                    byte* src = (byte*)nodePtr.ToPointer();
                    while (size.ToInt64() > 0x8000_0000u)
                    {
                        Unsafe.CopyBlock(dst, src, 0x8000_0000u);
                        dst += 0x8000_0000u;
                        src += 0x8000_0000u;
                        size = (IntPtr)(size.ToInt64() - 0x8000_0000u);
                    }
                    Unsafe.CopyBlock(dst, src, (uint)size.ToInt64());
                }
            }
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        IntPtr CloneNodeWithValueResize(IntPtr nodePtr, int length)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType;
            if (length < 0)
            {
                newNodeType = newNodeType & (~NodeType.IsLeaf);
            }
            else
            {
                newNodeType = newNodeType | NodeType.IsLeaf;
            }
            var newNode = AllocateNode(newNodeType, keyPrefixSize, (uint)(length < 0 ? 0 : length));
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
            }
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._nodeType = newNodeType;
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        void ReferenceAllChildren(IntPtr node)
        {
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
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
                                NodeUtils.Reference(child);
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
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
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
                                NodeUtils.Reference(child);
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
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
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
                                NodeUtils.Reference(child);
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
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
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
                                NodeUtils.Reference(child);
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
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                default: throw new InvalidOperationException();
            }
        }

        internal void Dereference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            if (!nodeHeader.Dereference()) return;
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
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

        void CheckContent12(ReadOnlySpan<byte> content)
        {
            if (content.Length != 12) throw new ArgumentOutOfRangeException(nameof(content));
            unsafe
            {
                fixed (void* p = &content[0])
                {
                    if (Unsafe.ReadUnaligned<uint>(p) == uint.MaxValue)
                    {
                        throw new ArgumentException("Content cannot start with 0xFFFFFFFF when in 12 bytes mode");
                    }
                }
            }
        }

        internal void WriteValue(RootNode rootNode, List<CursorItem> stack, ReadOnlySpan<byte> content)
        {
            if (IsValue12)
            {
                CheckContent12(content);
                MakeUnique(rootNode, stack);
                var stackItem = stack[stack.Count - 1];
                if (stackItem._posInNode == -1)
                {
                    var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                }
                else
                {
                    var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), 12)); }
                }
            }
            else
            {
                var stackItem = stack[stack.Count - 1];
                if (stackItem._posInNode == -1)
                {
                    var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    if (size == content.Length)
                    {
                        MakeUnique(rootNode, stack);
                    }
                    else
                    {
                        MakeUniqueLastResize(rootNode, stack, content.Length);
                    }
                    stackItem = stack[stack.Count - 1];
                    (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                }
                else
                {
                    MakeUnique(rootNode, stack);
                    if (content.Length < 8)
                    {
                        WriteContentInNode(stack[stack.Count - 1], content);
                    }
                    else
                    {
                        stackItem = new CursorItem(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, 0, (uint)content.Length), stack[stack.Count - 1]._keyOffset, -1, 0);
                        stack.Add(stackItem);
                        WritePtrInNode(stack[stack.Count - 2], stackItem._node);
                        var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                        unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    }
                }
            }
        }

        void MakeUniqueLastResize(RootNode rootNode, List<CursorItem> stack, int length)
        {
            for (int i = 0; i < stack.Count; i++)
            {
                var stackItem = stack[i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._referenceCount == 1 && i != stack.Count - 1)
                    continue;
                IntPtr newNode;
                if (i == stack.Count - 1)
                {
                    newNode = CloneNodeWithValueResize(stackItem._node, length);
                }
                else
                {
                    newNode = CloneNode(stackItem._node);
                }
                OverwriteNodePtrInStack(rootNode, stack, i, newNode);
            }
        }

        void MakeUnique(RootNode rootNode, List<CursorItem> stack)
        {
            for (int i = 0; i < stack.Count; i++)
            {
                var stackItem = stack[i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._referenceCount == 1)
                    continue;
                var newNode = CloneNode(stackItem._node);
                OverwriteNodePtrInStack(rootNode, stack, i, newNode);
            }
        }

        void OverwriteNodePtrInStack(RootNode rootNode, List<CursorItem> stack, int i, IntPtr newNode)
        {
            var stackItem = stack[i];
            stackItem._node = newNode;
            stack[i] = stackItem;
            if (i == 0)
            {
                Dereference(rootNode._root);
                rootNode._root = newNode;
            }
            else
            {
                WritePtrInNode(stack[i - 1], newNode);
            }
        }

        void WritePtrInNode(in CursorItem stackItem, IntPtr newNode)
        {
            var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            unsafe
            {
                if (IsValue12)
                {
                    if (NodeUtils.IsPtr12Ptr(ptr))
                    {
                        Dereference(NodeUtils.Read12Ptr(ptr));
                    }
                    Unsafe.Write(ptr.ToPointer(), uint.MaxValue);
                    Unsafe.Write((ptr + 4).ToPointer(), newNode);
                }
                else
                {
                    var child = NodeUtils.ReadPtr(ptr);
                    if (NodeUtils.IsPtrPtr(child))
                    {
                        Dereference(child);
                    }
                    Unsafe.Write(ptr.ToPointer(), newNode);
                }
            }
        }

        void WriteContentInNode(CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            unsafe
            {
                if (IsValue12)
                {
                    if (NodeUtils.IsPtr12Ptr(ptr))
                    {
                        Dereference(NodeUtils.Read12Ptr(ptr));
                    }
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), 12)); }
                }
                else
                {
                    var child = NodeUtils.ReadPtr(ptr);
                    if (NodeUtils.IsPtrPtr(child))
                    {
                        Dereference(child);
                    }
                    unsafe
                    {
                        NodeUtils.AssertLittleEndian();
                        *(byte*)ptr.ToPointer() = (byte)((content.Length << 1) + 1);
                        content.CopyTo(new Span<byte>(NodeUtils.SkipLenFromPtr(ptr).ToPointer(), 7));
                    }
                }
            }
        }

        internal bool Upsert(RootNode rootNode, List<CursorItem> stack, ReadOnlySpan<byte> key, ReadOnlySpan<byte> content)
        {
            if (IsValue12)
                CheckContent12(content);
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                if (top == IntPtr.Zero)
                {
                    MakeUnique(rootNode, stack);
                    var stackItem = new CursorItem(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, (uint)(key.Length - keyOffset), (uint)content.Length), (uint)key.Length, -1, 0);
                    stack.Add(stackItem);
                    var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe { key.Slice(keyOffset).CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, stackItem._node);
                    AdjustRecursiveChildCount(stack, stack.Count - 1, +1);
                    return true;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, KeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                if (key.Length - keyOffset < keyPrefixSize)
                {

                }
                throw new NotImplementedException();
            }
        }

        void AdjustRecursiveChildCount(List<CursorItem> stack, int upTo, int delta)
        {
            for (int i = 0; i < upTo; i++)
            {
                var stackItem = stack[i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                header._recursiveChildCount = (ulong)unchecked((long)header._recursiveChildCount + delta);
            }
        }
    }
}
