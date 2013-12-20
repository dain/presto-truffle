package com.facebook.presto.truffle;

import java.util.List;

import static com.facebook.presto.truffle.TpchDataGenerator.generateTestData;
import static com.facebook.presto.truffle.TpchQuery6.executeTpchQuery6;

public class PureJavaTest
{
    public static void main(String[] args)
    {
        List<Page> pages = generateTestData();

        double sum = 0;
        for (int i = 0; i < 20; i++) {
            long start = System.nanoTime();
            sum += executeTpchQuery6(pages);
            long duration = System.nanoTime() - start;
            System.out.printf("%6.2fms\n", duration / 1e6);
        }
        System.out.println(sum);
    }

}
