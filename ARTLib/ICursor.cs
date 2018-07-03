﻿using System;

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
        bool IsValid();
        int GetKeyLength();
        Span<byte> FillByKey(Span<byte> buffer);
        int GetValueLength();
        Span<byte> GetValue();

        void WriteValue(ReadOnlySpan<byte> content);
        bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content);
        void Erase();
        long EraseTo(ICursor to);
        long EraseAllWithKeyPrefixLength(int len);
    }
}
