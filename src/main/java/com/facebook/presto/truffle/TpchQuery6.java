package com.facebook.presto.truffle;

import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;

public class TpchQuery6
{
    private static final String MIN_SHIP_DATE = "1994-01-01";
    private static final String MAX_SHIP_DATE = "1995-01-01";

    public static double executeTpchQuery6(Iterable<Page> pages)
    {
        double sum = 0;
        long processedRows = 0;

        for (Page page : pages) {
            double[] extendedPrice = (double[]) page.getColumn(PRICE);
            double[] discount = (double[]) page.getColumn(DISCOUNT);
            String[] shipDate = (String[]) page.getColumn(SHIP_DATE);
            long[] quantity = (long[]) page.getColumn(QUANTITY);

            for (int row = 0; row < page.getRowCount(); row++) {
                if (filter(row, discount, shipDate, quantity)) {
                    sum += (getDouble(extendedPrice, row) * getDouble(discount, row));
                    processedRows++;
                }
            }
        }

        // System.out.println(sum + " " + processedRows);
        // 3.0645958657700088E7 28201
        return sum;
    }

    private static boolean filter(int row, double[] discount, String[] shipDate, long[] quantity)
    {
        return getDate(shipDate, row).compareTo(MIN_SHIP_DATE) >= 0 &&
                getDate(shipDate, row).compareTo(MAX_SHIP_DATE) < 0 &&
                getDouble(discount, row) >= 0.05 &&
                getDouble(discount, row) <= 0.07 &&
                getLong(quantity, row) < 24;
    }

    private static double getDouble(double[] slice, int row)
    {
        return slice[row];
    }

    private static long getLong(long[] slice, int row)
    {
        return slice[row];
    }

    private static String getDate(String[] slice, int row)
    {
        return slice[row];
    }
}
