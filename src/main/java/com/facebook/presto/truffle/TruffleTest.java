package com.facebook.presto.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;

import java.util.List;

import static com.facebook.presto.truffle.TpchDataGenerator.generateTestData;
import static com.facebook.presto.truffle.TpchQuery6.executeTpchQuery6;

public class TruffleTest
{
    public static void main(String[] args)
    {
        List<Page> pages = generateTestData();
        CallTarget call = Truffle.getRuntime().createCallTarget(new TpchQuery6Node(pages));

        double sum = 0;
        for (int i = 0; i < 1000; i++) {
            long start = System.nanoTime();
            sum += (double) call.call();
            long duration = System.nanoTime() - start;
            System.out.printf("%6.2fms\n", duration / 1e6);
        }
        System.out.println(sum);
    }

    public static final class TpchQuery6Node
            extends RootNode
    {
        private final List<Page> pages;

        public TpchQuery6Node(List<Page> pages)
        {
            this.pages = pages;
        }

        @Override
        public Object execute(VirtualFrame frame)
        {
            return executeTpchQuery6(pages);
        }
    }
}
