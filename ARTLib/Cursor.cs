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
                throw new ArgumentOutOfRangeException(nameof(buffer), "Key has " + keyLength + " bytes but provided buffer has only " + buffer.Length);
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
                if (stackItem._posInNode == -1)
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
            if (stackItem._posInNode==-1)
            {
                var (size, _) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                return (int)size;
            }
            return NodeUtils.ReadLenFromPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode));
        }

        public Span<byte> FillByValue(Span<byte> buffer)
        {
            AssertValid();
            throw new NotImplementedException();
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

        public bool Upsert(Span<byte> key, Span<byte> content)
        {
            _stack.Clear();
            return _rootNode._impl.Upsert(_rootNode, _stack, key, content);
        }

        public void WriteValue(Span<byte> content)
        {
            throw new NotImplementedException();
        }
    }
}
