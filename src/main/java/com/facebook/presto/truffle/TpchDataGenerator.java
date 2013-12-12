package com.facebook.presto.truffle;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

public class TpchDataGenerator
{
    private static final int PAGES = 5_000;
    private static final int ROWS_PAGE = 1_000;

    public static List<Page> generateTestData()
    {
        TpchDataGenerator tpchDataGenerator = new TpchDataGenerator(new Random(0));
        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        for (int i = 0; i < PAGES; i++) {
            builder.add(tpchDataGenerator.generateLineItemsPage(ROWS_PAGE));
        }
        return builder.build();
    }


    public static final int PRICE = 0;
    public static final int DISCOUNT = 1;
    public static final int SHIP_DATE = 2;
    public static final int QUANTITY = 3;

    public static final int DATE_STRING_LENGTH = 10;

    public static final int scale = 10;

    public static final int L_QTY_MIN = 1;
    public static final int L_QTY_MAX = 50;
    public static final int L_DCNT_MIN = 0;
    public static final int L_DCNT_MAX = 10;
    public static final int L_SDTE_MIN = 1;
    public static final int L_SDTE_MAX = 121;
    public static final int L_RDTE_MAX = 30;

    public static final int L_PKEY_MIN = 1;
    public static final int L_PKEY_MAX = (int) (200000 * scale);

    public static final int START_DATE = 92001;
    public static final int TOTDATE = 2557;
    public static final int O_ODATE_MIN = START_DATE;
    public static final int O_ODATE_MAX = (START_DATE + TOTDATE - (L_SDTE_MAX + L_RDTE_MAX) - 1);

    public Random random;

    public TpchDataGenerator(Random random)
    {
        this.random = random;
    }

    public Page generateLineItemsPage(int rowCount)
    {
        Slice extendedPriceSlice = Slices.allocate(rowCount * SizeOf.SIZE_OF_DOUBLE);
        SliceOutput extendedPriceOutput = extendedPriceSlice.getOutput();

        Slice discountSlice = Slices.allocate(rowCount * SizeOf.SIZE_OF_DOUBLE);
        SliceOutput discountOutput = discountSlice.getOutput();

        Slice quantitySlice = Slices.allocate(rowCount * SizeOf.SIZE_OF_LONG);
        SliceOutput quantityOutput = quantitySlice.getOutput();

        Slice shipDateSlice = Slices.allocate(rowCount * (DATE_STRING_LENGTH));
        SliceOutput shipDateOutput = shipDateSlice.getOutput();

        for (int row = 0; row < rowCount; row++) {
            int quantity = randomInt(L_QTY_MIN, L_QTY_MAX);

            double discount = randomInt(L_DCNT_MIN, L_DCNT_MAX) / 100.0;

            long partKey = randomLong(L_PKEY_MIN, L_PKEY_MAX);
            long partPrice = generatePartPrice(partKey);
            double extendedPrice = partPrice * quantity / 100.0;

            int orderDate = randomInt(O_ODATE_MIN, O_ODATE_MAX);
            int shipDate = randomInt(L_SDTE_MIN, L_SDTE_MAX);
            shipDate += orderDate;

            extendedPriceOutput.appendDouble(extendedPrice);
            discountOutput.appendDouble(discount);
            quantityOutput.appendLong(quantity);

            shipDateOutput.appendBytes(toDateString(shipDate).getBytes(StandardCharsets.UTF_8));
        }

        return new Page(rowCount, extendedPriceSlice, discountSlice, shipDateSlice, quantitySlice);
    }

    public int randomInt(int low, int high)
    {
        return random.nextInt(1 + high - low) + low;
    }

    public long randomLong(long low, long high)
    {
        return nextLong(1 + high - low) + low;
    }

    private long nextLong(long n)
    {
        Preconditions.checkArgument(n > 0, "n is negative");

        long bits;
        long val;
        do {
            bits = (random.nextLong() << 1) >>> 1;
            val = bits % n;
        } while (bits - val + (n - 1) < 0L);
        return val;
    }

    private static long generatePartPrice(long partKey)
    {
        long price = 90000;
        // limit contribution to $200
        price += (partKey / 10) % 20001;
        price += (partKey % 1000) * 100;

        return (price);
    }

    private static final int[] MONTH_YEAR_DAY_START = new int[]{
            0,
            31,
            59,
            90,
            120,
            151,
            181,
            212,
            243,
            273,
            304,
            334,
            365,
    };

    private static final List<String> DATE_STRING_INDEX = makeDateStringIndex();

    public static String toDateString(int date)
    {
        return DATE_STRING_INDEX.get(date - START_DATE);
    }

    private static List<String> makeDateStringIndex()
    {
        ImmutableList.Builder<String> dates = ImmutableList.builder();
        for (int i = 0; i < TOTDATE; i++) {
            dates.add(makeDate(i + 1));
        }

        return dates.build();
    }

    private static String makeDate(int index)
    {
        int y = julian(index + START_DATE - 1) / 1000;
        int d = julian(index + START_DATE - 1) % 1000;

        int m = 0;
        while (d > MONTH_YEAR_DAY_START[m] + leapYearAdjustment(y, m)) {
            m++;
        }
        int dy = d - MONTH_YEAR_DAY_START[m - 1] - ((isLeapYear(y) && m > 2) ? 1 : 0);

        return String.format("19%02d-%02d-%02d", y, m, dy);
    }

    private static int leapYearAdjustment(int year, int month)
    {
        return ((isLeapYear(year) && (month) >= 2) ? 1 : 0);
    }

    public static int julian(int date)
    {
        int offset = date - START_DATE;
        int result = START_DATE;

        while (true) {
            int year = result / 1000;
            int yearEnd = year * 1000 + 365 + (isLeapYear(year) ? 1 : 0);
            if (result + offset <= yearEnd) {
                break;
            }

            offset -= yearEnd - result + 1;
            result += 1000;
        }
        return (result + offset);
    }

    public static boolean isLeapYear(int year)
    {
        return year % 4 == 0 && year % 100 != 0;
    }
}
