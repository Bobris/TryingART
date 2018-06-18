using System;

namespace ARTLib
{
    public interface IRootNode: IDisposable
    {
        IRootNode Snapshot();
        void RevertTo(IRootNode snapshot);
        ICursor CreateCursor();
        long GetCount();
    }
}
