package com.facebook.presto.truffle;

import io.airlift.slice.Slice;

public class Page
{
    private final int rowCount;
    private final Slice[] columns;

    public Page(int rowCount, Slice... columns)
    {
        this.rowCount = rowCount;
        this.columns = columns;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public Slice getColumn(int column)
    {
        return columns[column];
    }

    public Slice[] getColumns()
    {
        return columns;
    }
}
