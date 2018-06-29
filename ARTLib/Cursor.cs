using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ARTLib
{
    internal class Cursor : ICursor
    {
        RootNode _rootNode;
        readonly List<CursorItem> _stack;

        public Cursor(RootNode rootNode)
        {
            _rootNode = rootNode;
            _stack = new List<CursorItem>();
        }

        Cursor(Cursor from)
        {
            _rootNode = from._rootNode;
            _stack = new List<CursorItem>(from._stack);
        }

        public long CalcDistance(ICursor to)
        {
            if (_rootNode != ((Cursor)to)._rootNode)
                throw new ArgumentException("Cursor must be from same transaction", nameof(to));
            return to.CalcIndex() - CalcIndex();
        }

        public long CalcIndex()
        {
            throw new NotImplementedException();
        }

        public ICursor Clone()
        {
            return new Cursor(this);
        }

        public void Erase()
        {
            Debug.Assert(_rootNode._writtable);
            throw new NotImplementedException();
        }

        public long EraseAllWithKeyPrefixLength(int len)
        {
            throw new NotImplementedException();
        }

        public long EraseTo(ICursor to)
        {
            if (_rootNode != ((Cursor)to)._rootNode)
                throw new ArgumentException("Cursor must be from same transaction", nameof(to));
            throw new NotImplementedException();
        }

        public bool FindExact(Span<byte> key)
        {
            throw new NotImplementedException();
        }

        public bool FindFirst(Span<byte> keyPrefix)
        {
            throw new NotImplementedException();
        }

        public bool FindLast(Span<byte> keyPrefix)
        {
            throw new NotImplementedException();
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
                var stackItem = stack[i++];
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
            return NodeUtils.ReadLenFromPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode));
        }

        public Span<byte> FillByValue(Span<byte> buffer)
        {
            AssertValid();
            var stackItem = _stack[_stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                if (buffer.Length < size)
                    throw new ArgumentOutOfRangeException(nameof(buffer), "Value has " + size + " bytes, but provided buffer has only " + buffer.Length);
                unsafe { new Span<byte>(ptr.ToPointer(), (int)size).CopyTo(buffer); }
                return buffer.Slice(0, (int)size);
            }
            var ptr2 = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            if (_rootNode._impl.IsValue12)
            {
                if (buffer.Length < 12)
                    throw new ArgumentOutOfRangeException(nameof(buffer), "Value has 12 bytes, but provided buffer has only " + buffer.Length);
                unsafe { new Span<byte>(ptr2.ToPointer(), 12).CopyTo(buffer); }
                return buffer.Slice(0, 12);
            }
            else
            {
                var size2 = NodeUtils.ReadLenFromPtr(ptr2);
                if (buffer.Length < size2)
                    throw new ArgumentOutOfRangeException(nameof(buffer), "Value has " + size2 + " bytes, but provided buffer has only " + buffer.Length);
                unsafe { new Span<byte>(NodeUtils.SkipLenFromPtr(ptr2).ToPointer(), size2).CopyTo(buffer); }
                return buffer.Slice(0, size2);
            }
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public bool MovePrevious()
        {
            throw new NotImplementedException();
        }

        public bool SeekIndex(long index)
        {
            throw new NotImplementedException();
        }

        public bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content)
        {
            _stack.Clear();
            return _rootNode._impl.Upsert(_rootNode, _stack, key, content);
        }

        public void WriteValue(ReadOnlySpan<byte> content)
        {
            throw new NotImplementedException();
        }
    }
}
