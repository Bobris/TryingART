﻿using System;

namespace ARTLib
{
    internal class Cursor : ICursor
    {
        RootNode _rootNode;
        StructList<CursorItem> _stack;

        public Cursor(RootNode rootNode)
        {
            _rootNode = rootNode;
            _stack = new StructList<CursorItem>();
        }

        Cursor(Cursor from)
        {
            _rootNode = from._rootNode;
            _stack = new StructList<CursorItem>(from._stack);
        }

        public long CalcDistance(ICursor to)
        {
            if (_rootNode != ((Cursor)to)._rootNode)
                throw new ArgumentException("Cursor must be from same transaction", nameof(to));
            return to.CalcIndex() - CalcIndex();
        }

        public long CalcIndex()
        {
            return _rootNode._impl.CalcIndex(_stack);
        }

        public ICursor Clone()
        {
            return new Cursor(this);
        }

        public void Erase()
        {
            AssertWrittable();
            EraseTo(this);
        }

        void AssertWrittable()
        {
            if (!_rootNode._writtable)
                throw new InvalidOperationException("Cursor not writtable");
        }

        public long EraseTo(ICursor to)
        {
            AssertWrittable();
            if (_rootNode != ((Cursor)to)._rootNode)
                throw new ArgumentException("Both cursors must be from same transaction", nameof(to));
            if (!to.IsValid())
                throw new ArgumentException("Cursor must be valid", nameof(to));
            if (!IsValid())
                throw new ArgumentException("Cursor must be valid", "this");
            return _rootNode._impl.EraseRange(_rootNode, ref _stack, ref ((Cursor)to)._stack);
        }

        public bool FindExact(ReadOnlySpan<byte> key)
        {
            return _rootNode._impl.FindExact(_rootNode, ref _stack, key);
        }

        public bool FindFirst(ReadOnlySpan<byte> keyPrefix)
        {
            return _rootNode._impl.FindFirst(_rootNode, ref _stack, keyPrefix);
        }

        public bool FindLast(ReadOnlySpan<byte> keyPrefix)
        {
            return _rootNode._impl.FindLast(_rootNode, ref _stack, keyPrefix);
        }

        public int GetKeyLength()
        {
            if (_stack.Count == 0) return -1;
            return (int)_stack[_stack.Count - 1]._keyOffset;
        }

        public bool IsValid()
        {
            return _stack.Count != 0;
        }

        void AssertValid()
        {
            if (_stack.Count == 0)
            {
                throw new InvalidOperationException("Cursor must be valid for this operation");
            }
        }

        public Span<byte> FillByKey(Span<byte> buffer)
        {
            AssertValid();
            var stack = _stack;
            var keyLength = (int)stack[stack.Count - 1]._keyOffset;
            if (buffer.Length < keyLength || keyLength < 0)
                throw new ArgumentOutOfRangeException(nameof(buffer), "Key has " + keyLength + " bytes, but provided buffer has only " + buffer.Length);
            var offset = 0;
            var i = 0;
            while (offset < keyLength)
            {
                ref var stackItem = ref stack[(uint)i++];
                if (offset < stackItem._keyOffset - (stackItem._posInNode == -1 ? 0 : 1))
                {
                    var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe { new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(buffer.Slice(offset)); }
                    offset += (int)keyPrefixSize;
                }
                if (stackItem._posInNode != -1)
                {
                    buffer[offset] = stackItem._byte;
                    offset++;
                }
            }
            return buffer.Slice(0, keyLength);
        }

        public int GetValueLength()
        {
            AssertValid();
            if (_rootNode._impl.IsValue12)
                return 12;
            var stackItem = _stack[_stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var (size, _) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                return (int)size;
            }
            return (int)NodeUtils.ReadLenFromPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode));
        }

        public ReadOnlySpan<byte> GetValue()
        {
            AssertValid();
            var stackItem = _stack[_stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                unsafe { return new Span<byte>(ptr.ToPointer(), (int)size); }
            }
            var ptr2 = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            if (_rootNode._impl.IsValue12)
            {
                unsafe { return new Span<byte>(ptr2.ToPointer(), 12); }
            }
            else
            {
                var size2 = NodeUtils.ReadLenFromPtr(ptr2);
                unsafe { return new Span<byte>(NodeUtils.SkipLenFromPtr(ptr2).ToPointer(), (int)size2); }
            }
        }

        public bool MoveNext()
        {
            if (_stack.Count == 0)
            {
                return FindFirst(new ReadOnlySpan<byte>());
            }
            return _rootNode._impl.MoveNext(ref _stack);
        }

        public bool MovePrevious()
        {
            if (_stack.Count == 0)
            {
                return FindLast(new ReadOnlySpan<byte>());
            }
            return _rootNode._impl.MovePrevious(ref _stack);
        }

        public bool SeekIndex(long index)
        {
            _stack.Clear();
            if (index < 0)
            {
                return false;
            }
            return _rootNode._impl.SeekIndex(index, _rootNode._root, ref _stack);
        }

        public bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content)
        {
            AssertWrittable();
            return _rootNode._impl.Upsert(_rootNode, ref _stack, key, content);
        }

        public void WriteValue(ReadOnlySpan<byte> content)
        {
            AssertWrittable();
            AssertValid();
            _rootNode._impl.WriteValue(_rootNode, ref _stack, content);
        }
    }
}
