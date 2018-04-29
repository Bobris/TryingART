using System;

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("ARTLibTest")]

namespace ARTLib
{
    public interface ICursor
    {
        ICursor Clone();
        bool FindExact(Span<byte> key);
        bool FindFirst(Span<byte> keyPrefix);
        bool FindLast(Span<byte> keyPrefix);
        bool SeekIndex(long index);
        bool MoveNext();
        bool MovePrevious();
        long CalcIndex();
        long CalcDistance(ICursor to);
        int GetKeyLength();
        byte[] GetKey();
        bool HasValue();
        Span<byte> GetValue();

        void WriteValue(Span<byte> content);
        bool Upsert(Span<byte> key, Span<byte> content);
        void Erase();
        long EraseTo(ICursor to);
        long EraseAllWithKeyPrefixLength(int len);
    }
}
