using System;
using System.Collections.Generic;
using System.Numerics;
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

        IntPtr CloneNodeWithKeyPrefixCut(IntPtr nodePtr, int skipPrefix)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(nodePtr);
            var newNode = AllocateNode(header._nodeType, keyPrefixSize - (uint)skipPrefix, valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            var backupNewKeyPrefix = newHeader._keyPrefixLength;
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).Slice(skipPrefix).CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    new Span<byte>(valuePtr.ToPointer(), (int)valueSize).CopyTo(new Span<byte>(newValuePtr.ToPointer(), (int)newValueSize));
                }
            }
            newHeader._referenceCount = 1;
            newHeader._keyPrefixLength = backupNewKeyPrefix;
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

        void WritePtrAndByteInNode(in CursorItem stackItem, IntPtr newNode)
        {
            WritePtrInNode(stackItem, newNode);
            var nodeType = NodeUtils.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType.NodeSizeMask;
            if (nodeType != NodeType.Node256)
                Marshal.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
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

        void WriteContentAndByteInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            WriteContentInNode(stackItem, content);
            var nodeType = NodeUtils.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType.NodeSizeMask;
            if (nodeType != NodeType.Node256)
                Marshal.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
        }

        void WriteContentInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
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

        unsafe void InitializeZeroSizeValue(IntPtr ptr)
        {
            if (IsValue12)
            {
                var v = new Span<uint>(ptr.ToPointer(), 3);
                v[0] = uint.MaxValue;
                v[1] = 0;
                v[2] = 0;
            }
            else
            {
                NodeUtils.AssertLittleEndian();
                *(byte*)ptr.ToPointer() = 1;
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
                var keyRest = key.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    // nodes on stack must be already unique
                    if (IsValue12 && keyRest == 0 && stack.Count > 0)
                    {
                        WriteContentInNode(stack[stack.Count - 1], content);
                        AdjustRecursiveChildCount(stack, stack.Count, +1);
                        return true;
                    }
                    var stackItem = new CursorItem(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, (uint)keyRest, (uint)content.Length), (uint)key.Length, -1, 0);
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
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var newKeyPrefixSize = FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, Math.Min(keyRest, (int)keyPrefixSize));
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    MakeUnique(rootNode, stack);
                    var nodeType = NodeType.Node4 | (newKeyPrefixSize == keyRest ? NodeType.IsLeaf : 0);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, (uint)content.Length);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = header._recursiveChildCount;
                        var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                        unsafe { key.Slice(keyOffset, newKeyPrefixSize).CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize)); }
                        if (IsValue12 && (header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf && newKeyPrefixSize + 1 == keyPrefixSize)
                        {
                            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(top);
                            unsafe
                            {
                                WriteContentAndByteInNode(new CursorItem(newNode, 0, 0, Marshal.ReadByte(keyPrefixPtr, key.Length)), new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                            }
                        }
                        else
                        {
                            var newNode2 = CloneNodeWithKeyPrefixCut(top, newKeyPrefixSize + 1);
                            WritePtrAndByteInNode(new CursorItem(newNode, 0, 0, Marshal.ReadByte(keyPrefixPtr, key.Length)), newNode2);
                        }
                        if (nodeType.HasFlag(NodeType.IsLeaf))
                        {
                            stack.Add(new CursorItem(newNode, (uint)key.Length, -1, 0));
                            (size, ptr) = NodeUtils.GetValueSizeAndPtr(newNode);
                            unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                            keyOffset = key.Length;
                            AdjustRecursiveChildCount(stack, stack.Count, +1);
                            OverwriteNodePtrInStack(rootNode, stack, stack.Count - 2, newNode);
                            newNode = IntPtr.Zero;
                            return true;
                        }
                        else
                        {
                            keyOffset += newKeyPrefixSize;
                            var b = key[keyOffset];
                            var pos = InsertChild(newNode, b);
                            stack.Add(new CursorItem(newNode, (uint)keyOffset + 1, pos, b));
                            top = IntPtr.Zero;
                            keyOffset++;
                            OverwriteNodePtrInStack(rootNode, stack, stack.Count - 2, newNode);
                            newNode = IntPtr.Zero;
                            continue;
                        }
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                throw new NotImplementedException();
            }
        }

        short InsertChild(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
                return b;
            int pos;
            unsafe
            {
                var childernBytes = new Span<byte>((nodePtr + 16).ToPointer(), header._childCount);
                pos = childernBytes.BinarySearch(b, Comparer<byte>.Default);
                if (pos < childernBytes.Length)
                {
                    if (childernBytes[pos] == b)
                    {
                        return (short)pos;
                    }
                    childernBytes.Slice(pos).CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                    childernBytes[pos] = b;
                    var chPtr = NodeUtils.PtrInNode(nodePtr, pos);
                    var itemSize = IsValue12 ? 12 : 8;
                    var chSize = itemSize * (header._childCount - pos);
                    new Span<byte>(chPtr.ToPointer(),chSize).CopyTo(new Span<byte>((chPtr+itemSize).ToPointer(),chSize));
                }
                header._childCount++;
                InitializeZeroSizeValue(NodeUtils.PtrInNode(nodePtr, pos));
                return (short)pos;
            }
        }

        unsafe int FindFirstDifference(ReadOnlySpan<byte> buf1, IntPtr buf2IntPtr, int len)
        {
            fixed (byte* buf1Ptr = &MemoryMarshal.GetReference(buf1))
            {
                byte* buf2Ptr = (byte*)buf2IntPtr.ToPointer();
                int i = 0;
                int n;
                if (Vector.IsHardwareAccelerated && len >= Vector<byte>.Count)
                {
                    n = len - Vector<byte>.Count;
                    while (n >= i)
                    {
                        if (Unsafe.ReadUnaligned<Vector<byte>>(buf1Ptr + i) != Unsafe.ReadUnaligned<Vector<byte>>(buf2Ptr + i))
                            break;
                        i += Vector<byte>.Count;
                    }
                }
                n = len - sizeof(long);
                while (n >= i)
                {
                    if (Unsafe.ReadUnaligned<long>(buf1Ptr + i) != Unsafe.ReadUnaligned<long>(buf2Ptr + i))
                        break;
                    i += sizeof(long);
                }
                while (len > i)
                {
                    if (Unsafe.Read<byte>(buf1Ptr + i) != Unsafe.Read<byte>(buf2Ptr + i))
                        break;
                    i++;
                }
                return i;
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
