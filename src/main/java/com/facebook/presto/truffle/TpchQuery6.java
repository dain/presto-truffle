package com.facebook.presto.truffle;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.truffle.TpchDataGenerator.DATE_STRING_LENGTH;
import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;
import static com.google.common.base.Charsets.UTF_8;

public class TpchQuery6
{
    public static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
    public static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);

    /*
     * select sum(price * discount) from pages where shipDate >= cst1 and shipDate < cst2 and discount >= 0.05 and discount <= 0.07 and quantity < 24
     * select reduce(exp) from it where exp
     * exp of columns
     */
    public static double executeTpchQuery6(Iterable<Page> pages)
    {
        double sum = 0;
        long processedRows = 0;

        for (Page page : pages) {
            Slice price = page.getColumn(PRICE);
            Slice discount = page.getColumn(DISCOUNT);
            Slice shipDate = page.getColumn(SHIP_DATE);
            Slice quantity = page.getColumn(QUANTITY);

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

    private static boolean filter(int row, Slice discount, Slice shipDate, Slice quantity)
    {
        return getDate(shipDate, row).compareTo(MIN_SHIP_DATE) >= 0 &&
                getDate(shipDate, row).compareTo(MAX_SHIP_DATE) < 0 &&
                getDouble(discount, row) >= 0.05 &&
                0.07 >= getDouble(discount, row) &&
                getLong(quantity, row) < 24;
    }
    
    // 2.0632824238740677E8

    private static double getDouble(Slice slice, int row)
    {
        return slice.getDouble(row * SizeOf.SIZE_OF_DOUBLE);
    }

    private static long getLong(Slice slice, int row)
    {
        return slice.getLong(row * SizeOf.SIZE_OF_DOUBLE);
    }

    private static Slice getDate(Slice slice, int row)
    {
        return slice.slice(row * DATE_STRING_LENGTH, DATE_STRING_LENGTH);
    }
}
