using System;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: InternalsVisibleTo("ARTLibTest")]

namespace ARTLib
{
    public class ARTImpl
    {
        internal readonly bool IsValue12;
        readonly IOffHeapAllocator _allocator;
        internal readonly int PtrSize;

        internal ARTImpl(IOffHeapAllocator allocator, bool isValue12)
        {
            _allocator = allocator;
            IsValue12 = isValue12;
            PtrSize = isValue12 ? 12 : 8;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator, bool isValue12)
        {
            return new RootNode(new ARTImpl(allocator, isValue12));
        }

        unsafe internal IntPtr AllocateNode(NodeType nodeType, uint keyPrefixLength, uint valueLength)
        {
            IntPtr node;
            int baseSize;
            if (IsValue12)
            {
                nodeType = nodeType | NodeType.Has12BPtrs;
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + NodeUtils.AlignUIntUpInt32(keyPrefixLength) + (nodeType.HasFlag(NodeType.IsLeaf) ? 12 : 0);
                if (keyPrefixLength >= 0xffff) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            else
            {
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + keyPrefixLength + (nodeType.HasFlag(NodeType.IsLeaf) ? valueLength : 0);
                if (keyPrefixLength >= 0xffff) size += 4;
                if (nodeType.HasFlag(NodeType.IsLeaf)) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._childCount = 0;
            nodeHeader._keyPrefixLength = (ushort)(keyPrefixLength >= 0xffffu ? 0xffffu : keyPrefixLength);
            nodeHeader._referenceCount = 1;
            nodeHeader._recursiveChildCount = 1;
            new Span<byte>(node.ToPointer(), baseSize).Slice(16).Clear();
            if (keyPrefixLength >= 0xffffu)
            {
                *(uint*)(node + baseSize).ToPointer() = keyPrefixLength;
                baseSize += 4;
            }
            if (!IsValue12 && nodeType.HasFlag(NodeType.IsLeaf))
            {
                *(uint*)(node + baseSize).ToPointer() = valueLength;
            }
            if ((nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                Unsafe.InitBlock((node + 16).ToPointer(), 255, 256);
            }
            if ((nodeType & NodeType.NodeSizePtrMask) == (NodeType.Node256 | NodeType.Has12BPtrs))
            {
                var p = (uint*)(node + 16).ToPointer();
                for (var i = 0; i < 256; i++)
                {
                    *p = uint.MaxValue;
                    p += 3;
                }
            }
            return node;
        }

        internal long CalcIndex(List<CursorItem> stack)
        {
            var stackCount = stack.Count;
            if (stackCount == 0)
                return -1;
            var res = 0L;
            for (var i = 0; i < stackCount; i++)
            {
                var stackItem = stack[i];
                if (stackItem._posInNode == -1)
                    return res;
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                    res++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        for (int j = 0; j < stackItem._posInNode; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr))
                            {
                                res += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                            }
                            else
                            {
                                res++;
                            }
                        }
                        break;
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), stackItem._byte);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                if (IsPtr(NodeUtils.PtrInNode(stackItem._node, span[j]), out var ptr))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node256:
                        for (int j = 0; j < stackItem._posInNode; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr))
                            {
                                if (ptr != IntPtr.Zero)
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                }
                            }
                            else
                            {
                                res++;
                            }
                        }
                        break;
                }
            }
            return res;
        }

        internal bool MoveNext(List<CursorItem> stack)
        {
            while (stack.Count > 0)
            {
                var stackItem = stack[stack.Count - 1];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1) stackItem._keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.NodeLeaf:
                        goto up;
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            if (stackItem._posInNode == header._childCount - 1)
                            {
                                goto up;
                            }
                            stackItem._posInNode++;
                            stackItem._byte = Marshal.ReadByte(stackItem._node, 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), 256);
                            for (int j = (stackItem._posInNode == -1) ? 0 : (stackItem._byte + 1); j < 256; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                stackItem._posInNode = span[j];
                                stackItem._byte = (byte)j;
                                goto down;
                            }
                            goto up;
                        }
                    case NodeType.Node256:
                        for (int j = (stackItem._posInNode == -1) ? 0 : (stackItem._byte + 1); j < 256; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr2))
                            {
                                if (ptr2 == IntPtr.Zero)
                                {
                                    continue;
                                }
                            }
                            stackItem._posInNode = (short)j;
                            stackItem._byte = (byte)j;
                            goto down;
                        }
                        goto up;
                }
                down:
                stack[stack.Count - 1] = stackItem;
                if (IsPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
                {
                    PushLeftMost(ptr, (int)stackItem._keyOffset, stack);
                }
                return true;
                up:
                stack.RemoveAt(stack.Count - 1);
            }
            return false;
        }

        internal bool MovePrevious(List<CursorItem> stack)
        {
            while (stack.Count > 0)
            {
                var stackItem = stack[stack.Count - 1];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1)
                {
                    goto trullyUp;
                }
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            if (stackItem._posInNode == 0)
                            {
                                goto up;
                            }
                            stackItem._posInNode--;
                            stackItem._byte = Marshal.ReadByte(stackItem._node, 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), 256);
                            for (int j = stackItem._byte - 1; j >= 0; j--)
                            {
                                if (span[j] == 255)
                                    continue;
                                stackItem._posInNode = span[j];
                                stackItem._byte = (byte)j;
                                goto down;
                            }
                            goto up;
                        }
                    case NodeType.Node256:
                        for (int j = stackItem._byte - 1; j >= 0; j--)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr2))
                            {
                                if (ptr2 == IntPtr.Zero)
                                {
                                    continue;
                                }
                            }
                            stackItem._posInNode = (short)j;
                            stackItem._byte = (byte)j;
                            goto down;
                        }
                        goto up;
                }
                down:
                stack[stack.Count - 1] = stackItem;
                if (IsPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
                {
                    PushRightMost(ptr, (int)stackItem._keyOffset, stack);
                }
                return true;
                up:
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    stackItem._posInNode = -1;
                    stackItem._keyOffset--;
                    stack[stack.Count - 1] = stackItem;
                    return true;
                }
                trullyUp:
                stack.RemoveAt(stack.Count - 1);
            }
            return false;
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

        unsafe void CopyMemory(IntPtr src, IntPtr dst, int size)
        {
            Unsafe.CopyBlockUnaligned(dst.ToPointer(), src.ToPointer(), (uint)size);
        }

        unsafe IntPtr ExpandNode(IntPtr nodePtr)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType + 1;
            var newNode = AllocateNode(newNodeType, keyPrefixSize, valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            if (newNodeType.HasFlag(NodeType.IsLeaf))
            {
                var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                CopyMemory(valuePtr, newValuePtr, (int)valueSize);
            }
            CopyMemory(keyPrefixPtr, newKeyPrefixPtr, (int)keyPrefixSize);
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._childCount = header._childCount;
            newHeader._recursiveChildCount = header._recursiveChildCount;
            switch (newNodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node16:
                    {
                        CopyMemory(nodePtr + 16, newNode + 16, 4);
                        CopyMemory(NodeUtils.PtrInNode(nodePtr, 0), NodeUtils.PtrInNode(newNode, 0), 4 * PtrSize);
                        break;
                    }
                case NodeType.Node48:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        var dstBytesPtr = (byte*)(newNode + 16).ToPointer();
                        for (var i = 0; i < 16; i++)
                        {
                            dstBytesPtr[srcBytesPtr[i]] = (byte)i;
                        }
                        CopyMemory(NodeUtils.PtrInNode(nodePtr, 0), NodeUtils.PtrInNode(newNode, 0), 16 * PtrSize);
                        break;
                    }
                case NodeType.Node256:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        for (var i = 0; i < 256; i++)
                        {
                            var pos = srcBytesPtr[i];
                            if (pos == 255) continue;
                            CopyMemory(NodeUtils.PtrInNode(nodePtr, pos), NodeUtils.PtrInNode(newNode, i), PtrSize);
                        }
                        break;
                    }
                default:
                    throw new InvalidOperationException();
            }
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
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
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
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
            if (MemoryMarshal.Read<uint>(content) == uint.MaxValue)
            {
                throw new ArgumentException("Content cannot start with 0xFFFFFFFF when in 12 bytes mode");
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

        unsafe void InitializeZeroPtrValue(IntPtr ptr)
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
                var v = new Span<uint>(ptr.ToPointer(), 2);
                v[0] = 0;
                v[1] = 0;
            }
        }

        internal bool FindExact(RootNode rootNode, List<CursorItem> stack, ReadOnlySpan<byte> key)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = key.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var newKeyPrefixSize = FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, Math.Min(keyRest, (int)keyPrefixSize));
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    stack.Clear();
                    return false;
                }
                if (keyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        stack.Clear();
                        return false;
                    }
                    stack.Add(new CursorItem(top, (uint)key.Length, -1, 0));
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = key[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, b));
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (key.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        internal bool FindFirst(RootNode rootNode, List<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = keyPrefix.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var newKeyPrefixSize = FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, Math.Min(keyRest, (int)keyPrefixSize));
                if (newKeyPrefixSize < keyPrefixSize && newKeyPrefixSize < keyRest)
                {
                    stack.Clear();
                    return false;
                }
                if (newKeyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        PushLeftMost(top, keyOffset, stack);
                        return true;
                    }
                    stack.Add(new CursorItem(top, (uint)keyOffset + keyPrefixSize, -1, 0));
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = keyPrefix[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, b));
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (keyPrefix.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        internal bool FindLast(RootNode rootNode, List<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = keyPrefix.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var newKeyPrefixSize = FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, Math.Min(keyRest, (int)keyPrefixSize));
                if (newKeyPrefixSize < keyPrefixSize && newKeyPrefixSize < keyRest)
                {
                    stack.Clear();
                    return false;
                }
                if (newKeyPrefixSize == keyRest)
                {
                    PushRightMost(top, keyOffset, stack);
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = keyPrefix[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, b));
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (keyPrefix.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        void PushLeftMost(IntPtr top, int keyOffset, List<CursorItem> stack)
        {
            while (true)
            {
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils.GetPrefixSizeAndPtr(top).Size;
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    stack.Add(new CursorItem(top, (uint)keyOffset, -1, 0));
                    return;
                }
                keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            stack.Add(new CursorItem(top, (uint)keyOffset, 0, Marshal.ReadByte(top, 16)));
                            if (IsPtr(NodeUtils.PtrInNode(top, 0), out var ptr))
                            {
                                top = ptr;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 0; true; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                stack.Add(new CursorItem(top, (uint)keyOffset, span[j], (byte)j));
                                if (IsPtr(NodeUtils.PtrInNode(top, span[j]), out var ptr))
                                {
                                    top = ptr;
                                    break;
                                }
                                return;
                            }
                            break;
                        }
                    case NodeType.Node256:
                        for (int j = 0; true; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(top, j), out var ptr))
                            {
                                if (ptr != IntPtr.Zero)
                                {
                                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)j, (byte)j));
                                    top = ptr;
                                    break;
                                }
                                continue;
                            }
                            else
                            {
                                stack.Add(new CursorItem(top, (uint)keyOffset, (short)j, (byte)j));
                                return;
                            }
                        }
                        break;
                }
            }
        }

        void PushRightMost(IntPtr top, int keyOffset, List<CursorItem> stack)
        {
            while (true)
            {
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils.GetPrefixSizeAndPtr(top).Size;
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Add(new CursorItem(top, (uint)keyOffset, -1, 0));
                    return;
                }
                keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            var pos = header._childCount - 1;
                            stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, Marshal.ReadByte(top, 16 + pos)));
                            if (IsPtr(NodeUtils.PtrInNode(top, pos), out var ptr))
                            {
                                top = ptr;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 255; true; j--)
                            {
                                if (span[j] == 255)
                                    continue;
                                stack.Add(new CursorItem(top, (uint)keyOffset, span[j], (byte)j));
                                if (IsPtr(NodeUtils.PtrInNode(top, span[j]), out var ptr))
                                {
                                    top = ptr;
                                    break;
                                }
                                return;
                            }
                            break;
                        }
                    case NodeType.Node256:
                        for (int j = 255; true; j--)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(top, j), out var ptr))
                            {
                                if (ptr != IntPtr.Zero)
                                {
                                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)j, (byte)j));
                                    top = ptr;
                                    break;
                                }
                                continue;
                            }
                            else
                            {
                                stack.Add(new CursorItem(top, (uint)keyOffset, (short)j, (byte)j));
                                return;
                            }
                        }
                        break;
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
                var keyRest = key.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    // nodes on stack must be already unique
                    if (keyRest == 0 && (IsValue12 || content.Length < 8) && stack.Count > 0)
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
                                WriteContentAndByteInNode(new CursorItem(newNode, 0, 0, Marshal.ReadByte(keyPrefixPtr, newKeyPrefixSize)), new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                            }
                        }
                        else
                        {
                            var newNode2 = CloneNodeWithKeyPrefixCut(top, newKeyPrefixSize + 1);
                            WritePtrAndByteInNode(new CursorItem(newNode, 0, 0, Marshal.ReadByte(keyPrefixPtr, newKeyPrefixSize)), newNode2);
                        }
                        if (nodeType.HasFlag(NodeType.IsLeaf))
                        {
                            stack.Add(new CursorItem(newNode, (uint)key.Length, -1, 0));
                            (size, ptr) = NodeUtils.GetValueSizeAndPtr(newNode);
                            unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                            keyOffset = key.Length;
                            AdjustRecursiveChildCount(stack, stack.Count, +1);
                            OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, newNode);
                            newNode = IntPtr.Zero;
                            return true;
                        }
                        else
                        {
                            keyOffset += newKeyPrefixSize + 1;
                            var b2 = key[keyOffset - 1];
                            var pos2 = InsertChild(newNode, b2);
                            stack.Add(new CursorItem(newNode, (uint)keyOffset, pos2, b2));
                            top = IntPtr.Zero;
                            OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, newNode);
                            newNode = IntPtr.Zero;
                            continue;
                        }
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                if (keyPrefixSize == keyRest)
                {
                    stack.Add(new CursorItem(top, (uint)key.Length, -1, 0));
                    if (header._nodeType.HasFlag(NodeType.IsLeaf) && (IsValue12 || NodeUtils.GetValueSizeAndPtr(top).Size == content.Length))
                    {
                        MakeUnique(rootNode, stack);
                    }
                    else
                    {
                        MakeUniqueLastResize(rootNode, stack, content.Length);
                    }
                    var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stack[stack.Count - 1]._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        AdjustRecursiveChildCount(stack, stack.Count, +1);
                        return true;
                    }
                    return false;
                }
                var b = key[keyOffset + newKeyPrefixSize];
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    MakeUnique(rootNode, stack);
                    var nodeType = NodeType.Node4 | NodeType.IsLeaf;
                    var (topValueSize, topValuePtr) = NodeUtils.GetValueSizeAndPtr(top);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                        unsafe { key.Slice(keyOffset, newKeyPrefixSize).CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize)); }
                        var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                        }
                        keyOffset += newKeyPrefixSize + 1;
                        stack.Add(new CursorItem(newNode, (uint)keyOffset, 0, b));
                        top = IntPtr.Zero;
                        OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, newNode);
                        newNode = IntPtr.Zero;
                        continue;
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, b));
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    MakeUnique(rootNode, stack);
                    if (key.Length == keyOffset)
                    {
                        if (IsValueInlinable(content))
                        {
                            WriteContentInNode(stack[stack.Count - 1], content);
                        }
                        else
                        {
                            var stackItem = new CursorItem(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, 0, (uint)content.Length), (uint)key.Length, -1, 0);
                            stack.Add(stackItem);
                            var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                            unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                            OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, stackItem._node);
                        }
                        return false;
                    }
                    var nodeType = NodeType.Node4 | NodeType.IsLeaf;
                    var (topValueSize, topValuePtr) = GetValueSizeAndPtrFromPtrInNode(NodeUtils.PtrInNode(top, pos));
                    var newNode = AllocateNode(nodeType, 0, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                        }
                        b = key[keyOffset++];
                        stack.Add(new CursorItem(newNode, (uint)keyOffset, 0, b));
                        top = IntPtr.Zero;
                        OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, newNode);
                        newNode = IntPtr.Zero;
                        continue;
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                pos = ~pos;
                MakeUnique(rootNode, stack);
                bool topChanged = false;
                if (header.IsFull)
                {
                    top = ExpandNode(top);
                    topChanged = true;
                }
                else if (header._referenceCount > 1)
                {
                    top = CloneNode(top);
                    topChanged = true;
                }
                InsertChildRaw(top, ref pos, b);
                keyOffset += newKeyPrefixSize + 1;
                stack.Add(new CursorItem(top, (uint)keyOffset, (short)pos, b));
                if (topChanged)
                {
                    OverwriteNodePtrInStack(rootNode, stack, stack.Count - 1, top);
                }
                top = IntPtr.Zero;
            }
        }

        (uint, IntPtr) GetValueSizeAndPtrFromPtrInNode(IntPtr ptr)
        {
            if (IsValue12)
                return (12, ptr);
            return (NodeUtils.ReadLenFromPtr(ptr), NodeUtils.SkipLenFromPtr(ptr));
        }

        bool IsValueInlinable(ReadOnlySpan<byte> content)
        {
            if (IsValue12) return true;
            if (content.Length < 8) return true;
            return false;
        }

        bool IsPtr(IntPtr ptr, out IntPtr pointsTo)
        {
            if (IsValue12)
            {
                if (NodeUtils.IsPtr12Ptr(ptr))
                {
                    pointsTo = NodeUtils.Read12Ptr(ptr);
                    return true;
                }
            }
            else
            {
                var child = NodeUtils.ReadPtr(ptr);
                if (NodeUtils.IsPtrPtr(child))
                {
                    pointsTo = child;
                    return true;
                }
            }
            pointsTo = IntPtr.Zero;
            return false;
        }

        unsafe int Find(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
            {
                var ptr = NodeUtils.PtrInNode(nodePtr, b);
                if (IsValue12)
                {
                    if (NodeUtils.IsPtr12Ptr(ptr) && NodeUtils.Read12Ptr(ptr) == IntPtr.Zero)
                        return ~b;
                }
                else
                {
                    if (NodeUtils.ReadPtr(ptr) == IntPtr.Zero)
                        return ~b;
                }
                return b;
            }
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                var pos = Marshal.ReadByte(nodePtr, 16 + b);
                if (pos == 255)
                    return ~header._childCount;
                return pos;
            }
            else
            {
                var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                return BinarySearch(childernBytes, b);
            }
        }

        int BinarySearch(ReadOnlySpan<byte> data, byte value)
        {
            var l = 0;
            ref var d = ref MemoryMarshal.GetReference(data);
            var r = data.Length;
            while (l < r)
            {
                var m = (int)(((uint)l + (uint)r) >> 1);
                var diff = Unsafe.Add(ref d, m) - value;
                if (diff == 0) return m;
                if (diff > 0)
                {
                    r = m;
                }
                else
                {
                    l = m + 1;
                }
            }
            return ~l;
        }

        unsafe short InsertChild(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
                return b;
            int pos;
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                pos = Marshal.ReadByte(nodePtr, 16 + b);
                if (pos != 255)
                    return (short)pos;
                pos = header._childCount;
                Marshal.WriteByte(nodePtr, 16 + b, (byte)pos);
            }
            else
            {
                var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                pos = BinarySearch(childernBytes, b);
                if (pos >= 0) return (short)pos;
                pos = ~pos;
                if (pos < childernBytes.Length)
                {
                    childernBytes.Slice(pos).CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                    var chPtr = NodeUtils.PtrInNode(nodePtr, pos);
                    var chSize = PtrSize * (header._childCount - pos);
                    new Span<byte>(chPtr.ToPointer(), chSize).CopyTo(new Span<byte>((chPtr + PtrSize).ToPointer(), chSize));
                }
                Marshal.WriteByte(nodePtr, 16 + pos, b);
            }
            header._childCount++;
            InitializeZeroPtrValue(NodeUtils.PtrInNode(nodePtr, pos));
            return (short)pos;
        }

        unsafe void InsertChildRaw(IntPtr nodePtr, ref int pos, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
                return;
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                pos = header._childCount;
                Marshal.WriteByte(nodePtr, 16 + b, (byte)pos);
            }
            else
            {
                var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                if (pos < childernBytes.Length)
                {
                    childernBytes.Slice(pos).CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                    var chPtr = NodeUtils.PtrInNode(nodePtr, pos);
                    var chSize = PtrSize * (header._childCount - pos);
                    new Span<byte>(chPtr.ToPointer(), chSize).CopyTo(new Span<byte>((chPtr + PtrSize).ToPointer(), chSize));
                }
                Marshal.WriteByte(nodePtr, 16 + pos, b);
            }
            header._childCount++;
            InitializeZeroPtrValue(NodeUtils.PtrInNode(nodePtr, pos));
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
