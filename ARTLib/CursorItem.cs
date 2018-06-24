using System;

namespace ARTLib
{
    struct CursorItem
    {
        public CursorItem(IntPtr node, uint keyOffset)
        {
            _node = node;
            _keyOffset = keyOffset;
        }

        internal IntPtr _node;
        internal uint _keyOffset;
    }
}
