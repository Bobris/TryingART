﻿using System;

namespace ARTLib
{
    internal class RootNode : IRootNode
    {
        internal RootNode(ARTImpl impl)
        {
            _impl = impl;
            _root = IntPtr.Zero;
            _writtable = true;
        }

        internal IntPtr _root;
        internal ARTImpl _impl;
        internal bool _writtable;

        public void Dispose()
        {
            _impl.Dereference(_root);
        }

        public IRootNode Snapshot()
        {
            var snapshot = new RootNode(_impl);
            snapshot._writtable = false;
            snapshot._root = _root;
            NodeUtils.Reference(_root);
            return snapshot;
        }

        public ICursor CreateCursor()
        {
            return new Cursor(this);
        }

        public long GetCount()
        {
            if (_root == IntPtr.Zero) return 0;
            ref var header = ref NodeUtils.Ptr2NodeHeader(_root);
            return (long)header._recursiveChildCount;
        }

        public void RevertTo(IRootNode snapshot)
        {
            if (!_writtable)
                throw new InvalidOperationException("Only writtable root node could be reverted");
            var oldRoot = _root;
            _root = ((RootNode)snapshot)._root;
            if (oldRoot != _root)
            {
                NodeUtils.Reference(_root);
                _impl.Dereference(oldRoot);
            }
        }
    }
}
