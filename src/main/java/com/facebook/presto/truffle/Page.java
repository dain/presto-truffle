package com.facebook.presto.truffle;

public class Page
{
    private final int rowCount;
    private final byte[][] columns;

    public Page(int rowCount, byte[]... columns)
    {
        this.rowCount = rowCount;
        this.columns = columns;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public byte[] getColumn(int column)
    {
        return columns[column];
    }
}
