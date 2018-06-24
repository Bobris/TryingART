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
            Debug.Assert(_rootNode == ((Cursor)to)._rootNode);
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
            Debug.Assert(_rootNode == ((Cursor)to)._rootNode);
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

        public byte[] GetKey()
        {
            throw new NotImplementedException();
        }

        public int GetKeyLength()
        {
            throw new NotImplementedException();
        }

        public Span<byte> GetValue()
        {
            throw new NotImplementedException();
        }

        public bool HasValue()
        {
            throw new NotImplementedException();
        }

        public bool IsValid()
        {
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
