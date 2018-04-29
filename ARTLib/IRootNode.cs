using System;

namespace ARTLib
{
    public interface IRootNode: IDisposable
    {
        IRootNode Snapshot();
        ICursor CreateCursor();
        long GetCount();
    }
}
