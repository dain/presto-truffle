package com.facebook.presto.truffle;

import io.airlift.slice.SizeOf;

import static com.facebook.presto.truffle.TpchDataGenerator.DATE_STRING_LENGTH;
import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;
import static com.facebook.presto.truffle.UnsafeUtil.unsafe;
import static com.google.common.base.Charsets.UTF_8;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class TpchQuery6
{
    private static final byte[] MIN_SHIP_DATE = "1994-01-01".getBytes(UTF_8);
    private static final byte[] MAX_SHIP_DATE = "1995-01-01".getBytes(UTF_8);

    public static double executeTpchQuery6(Iterable<Page> pages)
    {
        double sum = 0;
        long processedRows = 0;

        for (Page page : pages) {
            byte[] price = page.getColumn(PRICE);
            byte[] discount = page.getColumn(DISCOUNT);
            byte[] shipDate = page.getColumn(SHIP_DATE);
            byte[] quantity = page.getColumn(QUANTITY);

            for (int row = 0; row < page.getRowCount(); row++) {
                if (filter(row, discount, shipDate, quantity)) {
                    sum += (getDouble(price, row) * getDouble(discount, row));
                    processedRows++;
                }
            }
        }

        // System.out.println(sum + " " + processedRows);
        // 3.0645958657700088E7 28201
        return sum;
    }

    private static boolean filter(int row, byte[] discount, byte[] shipDate, byte[] quantity)
    {
        return compare(getDate(shipDate, row), (MIN_SHIP_DATE)) >= 0 &&
                compare(getDate(shipDate, row), (MAX_SHIP_DATE)) < 0 &&
                getDouble(discount, row) >= 0.05 &&
                getDouble(discount, row) <= 0.07 &&
                getLong(quantity, row) < 24;
    }

    private static double getDouble(byte[] slice, int row)
    {
        return unsafe.getDouble(slice, (long) ARRAY_BYTE_BASE_OFFSET + (row * SizeOf.SIZE_OF_DOUBLE));
    }

    private static long getLong(byte[] slice, int row)
    {
        return unsafe.getLong(slice, (long) ARRAY_BYTE_BASE_OFFSET + (row * SizeOf.SIZE_OF_DOUBLE));
    }

    private static byte[] getDate(byte[] slice, int row)
    {
        byte[] date = new byte[DATE_STRING_LENGTH];
        unsafe.copyMemory(
                slice,
                (long) ARRAY_BYTE_BASE_OFFSET + (row * DATE_STRING_LENGTH),
                date,
                (long) ARRAY_BYTE_BASE_OFFSET,
                DATE_STRING_LENGTH);
        return date;
    }

    public static int compare(byte[] left, byte[] right)
    {
        for (int i = 0; i < SHIP_DATE; i++) {
            int a = (left[i] & 0xff);
            int b = (right[i] & 0xff);
            int result = Integer.compare(a, b);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

}
