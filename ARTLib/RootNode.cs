using System;

namespace ARTLib
{
    internal class RootNode: IRootNode
    {
        internal RootNode(ARTImpl impl)
        {
            _impl = impl;
            _root = IntPtr.Zero;
            _writtable = true;
        }

        internal IntPtr _root;
        internal ARTImpl _impl;
        internal long _pairs;
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
            snapshot._pairs = _pairs;
            NodeUtils.Reference(_root);
            return snapshot;
        }

        public ICursor CreateCursor()
        {
            return new Cursor(this);
        }

        public long GetCount()
        {
            return _pairs;
        }
    }
}
