package com.facebook.presto.truffle;

public class Page
{
    private final int rowCount;
    private final Object[] columns;

    public Page(int rowCount, Object... columns)
    {
        this.rowCount = rowCount;
        this.columns = columns;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public Object getColumn(int column)
    {
        return columns[column];
    }

    public Object[] getColumns()
    {
        return columns;
    }
}
