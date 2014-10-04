package com.facebook.presto.truffle;

import static com.facebook.presto.truffle.TpchDataGenerator.DATE_STRING_LENGTH;
import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;
import static com.facebook.presto.truffle.TpchDataGenerator.generateTestData;
import static com.facebook.presto.truffle.TruffleTestFactory.ConjunctionNodeFactory;
import static com.facebook.presto.truffle.TruffleTestFactory.GreaterEqualsNodeFactory;
import static com.facebook.presto.truffle.TruffleTestFactory.LessThanNodeFactory;
import static com.facebook.presto.truffle.TruffleTestFactory.MulNodeFactory;

import com.facebook.presto.truffle.nodes.*;
import com.oracle.truffle.api.*;

import java.util.List;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;

public class TruffleTest {
    public final static int ITERATIONS = 100;

    public static void main(String[] args) {
        ReduceQueryNode rootNode = createExpressionAST();
        RootCallTarget call = Truffle.getRuntime().createCallTarget(rootNode);

        runComputation(generateTestData(), call);
    }

    private static ReduceQueryNode createExpressionAST() {
        FrameDescriptor frameDescriptor = new FrameDescriptor();

        FrameSlot rowSlot = frameDescriptor.addFrameSlot("row", FrameSlotKind.Int);
        FrameSlot priceSlot = frameDescriptor.addFrameSlot("PRICE", FrameSlotKind.Object);
        FrameSlot discountSlot = frameDescriptor.addFrameSlot("DISCOUNT", FrameSlotKind.Object);
        FrameSlot shipDateSlot = frameDescriptor.addFrameSlot("SHIP_DATE", FrameSlotKind.Object);
        FrameSlot quantitySlot = frameDescriptor.addFrameSlot("QUANTITY", FrameSlotKind.Object);
        FrameMapping[] mapping = new FrameMapping[] {
             new FrameMapping(PRICE, priceSlot),
             new FrameMapping(DISCOUNT, discountSlot),
             new FrameMapping(SHIP_DATE, shipDateSlot),
             new FrameMapping(QUANTITY, quantitySlot), };

        /*
         * Build AST for expression:
         * select sum(price * discount) from pages where
         *  shipDate >= cst1 and
         *  shipDate < cst2 and
         *  discount >= 0.05 and
         *  discount <= 0.07 and
         *  quantity < 24
         */
        ExpressionNode expressionNode = MulNodeFactory.create(
             new CellGetDoubleNode(priceSlot, rowSlot),
             new CellGetDoubleNode(discountSlot, rowSlot));

        DoubleSumNode sumNode = new DoubleSumNode(frameDescriptor.addFrameSlot("sum", FrameSlotKind.Double), expressionNode);

        ExpressionNode filterNode = ConjunctionNodeFactory.create(
            GreaterEqualsNodeFactory.create(
                    new CellGetSliceNode(shipDateSlot, rowSlot, DATE_STRING_LENGTH),
                    new SliceConstantNode(TpchQuery6.MIN_SHIP_DATE)),
            ConjunctionNodeFactory.create(
               LessThanNodeFactory.create(
                   new CellGetSliceNode(shipDateSlot, rowSlot, DATE_STRING_LENGTH),
                   new SliceConstantNode(TpchQuery6.MAX_SHIP_DATE)),
                   ConjunctionNodeFactory.create(
                       GreaterEqualsNodeFactory.create(
                               new CellGetDoubleNode(discountSlot, rowSlot),
                               new DoubleConstantNode(0.05)),
                       ConjunctionNodeFactory.create(GreaterEqualsNodeFactory.create(
                                       new DoubleConstantNode(0.07),
                                       new CellGetDoubleNode(discountSlot, rowSlot)),
                           LessThanNodeFactory.create(
                               new CellGetLongNode(quantitySlot, rowSlot),
                               new LongConstantNode(24L))))));

        return new ReduceQueryNode(sumNode, filterNode, mapping, rowSlot, frameDescriptor);
    }

    private static void runComputation(List<Page> pages, RootCallTarget call) {
        double sum = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            double pagesSum = 0;
            long start = System.nanoTime();

            for (Page page : pages) {
                pagesSum += (double) call.call(page);
            }
            sum += pagesSum;
            long duration = System.nanoTime() - start;
            System.out.printf("%6.2fms\n", duration / 1e6);
        }
        System.out.println(sum);
    }

    public static class FrameMapping {
        private final int column;
        private final FrameSlot frameSlot;

        public FrameMapping(int column, FrameSlot frameSlot) {
            this.column = column;
            this.frameSlot = frameSlot;
        }

        public int getColumn() {
            return column;
        }

        public FrameSlot getFrameSlot() {
            return frameSlot;
        }
    }

}
