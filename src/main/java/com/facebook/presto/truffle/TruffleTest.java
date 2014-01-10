package com.facebook.presto.truffle;

import static com.facebook.presto.truffle.TpchDataGenerator.DATE_STRING_LENGTH;
import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;
import static com.facebook.presto.truffle.TpchDataGenerator.generateTestData;
import static java.lang.String.format;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.reflect.Field;
import java.util.List;

import javax.annotation.Nullable;

import sun.misc.Unsafe;

import com.google.common.base.Preconditions;
import com.oracle.truffle.api.Arguments;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.SlowPath;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.ShortCircuit;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.dsl.TypeSystem;
import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import com.oracle.truffle.api.utilities.BranchProfile;

public class TruffleTest {
    public final static int ITERATIONS = 30;

    private final static Unsafe unsafe;
    private final static long baseOffset;
    private final static long referenceOffset;
    private final static long addressOffset;
    private final static long sizeOffset;

    static {
        Unsafe unsafe_ = null;
        long baseOffset_ = -1;
        long referenceOffset_ = -1;
        long addressOffset_ = -1;
        long sizeOffset_ = -1;
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe_ = (Unsafe) field.get(null);
            if (unsafe_ == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            baseOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("base"));
            referenceOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("reference"));
            addressOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("address"));
            sizeOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("size"));
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }
        
        unsafe = unsafe_;
        baseOffset = baseOffset_;
        referenceOffset = referenceOffset_;
        addressOffset = addressOffset_;
        sizeOffset = sizeOffset_;
    }

    public static void main(String[] args) {
        List<Page> pages = generateTestData();
        TruffleRuntime runtime = Truffle.getRuntime();
        FrameDescriptor desc = new FrameDescriptor();

        FrameSlot rowSlot = desc.addFrameSlot("row", FrameSlotKind.Int);
        FrameSlot priceSlot = desc.addFrameSlot("PRICE", FrameSlotKind.Object);
        FrameSlot discountSlot = desc.addFrameSlot("DISCOUNT", FrameSlotKind.Object);
        FrameSlot shipDateSlot = desc.addFrameSlot("SHIP_DATE", FrameSlotKind.Object);
        FrameSlot quantitySlot = desc.addFrameSlot("QUANTITY", FrameSlotKind.Object);
        FrameMapping[] mapping = new FrameMapping[] {
             new FrameMapping(PRICE, priceSlot),
             new FrameMapping(DISCOUNT, discountSlot),
             new FrameMapping(SHIP_DATE, shipDateSlot),
             new FrameMapping(QUANTITY, quantitySlot), };

        ExpressionNode expressionNode = TruffleTestFactory.MulNodeFactory.create(
             new CellGetDoubleNode(priceSlot, rowSlot),
             new CellGetDoubleNode(discountSlot, rowSlot));

        DoubleSumNode sumNode = new DoubleSumNode(desc.addFrameSlot("sum", FrameSlotKind.Double), expressionNode);

        ExpressionNode filterNode = TruffleTestFactory.ConjunctionNodeFactory.create(
            TruffleTestFactory.GreaterEqualsNodeFactory.create(
               new CellGetSliceNode(shipDateSlot, rowSlot, DATE_STRING_LENGTH),
               new SliceConstantNode(TpchQuery6.MIN_SHIP_DATE)),
            TruffleTestFactory.ConjunctionNodeFactory.create(
               TruffleTestFactory.LessThanNodeFactory.create(
                   new CellGetSliceNode(shipDateSlot, rowSlot, DATE_STRING_LENGTH),
                   new SliceConstantNode(TpchQuery6.MAX_SHIP_DATE)),
                   TruffleTestFactory.ConjunctionNodeFactory.create(
                       TruffleTestFactory.GreaterEqualsNodeFactory.create(
                           new CellGetDoubleNode(discountSlot, rowSlot),
                           new DoubleConstantNode(0.05)),
                       TruffleTestFactory.ConjunctionNodeFactory.create(TruffleTestFactory.GreaterEqualsNodeFactory.create(
                           new DoubleConstantNode(0.07),
                           new CellGetDoubleNode(discountSlot, rowSlot)),
                           TruffleTestFactory.LessThanNodeFactory.create(
                               new CellGetLongNode(quantitySlot, rowSlot),
                               new LongConstantNode( 24L))))));

        CallTarget call = runtime.createCallTarget(new ReduceQueryNode(sumNode,        filterNode, mapping, rowSlot), desc);

        double sum = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            double pagesSum = 0;
            long start = System.nanoTime();

            for (Page page : pages) {
                pagesSum += (double) call.call(new PageArguments(page));
            }
            sum += pagesSum;
            long duration = System.nanoTime() - start;
            System.out.printf("%6.2fms\n", duration / 1e6);
        }
        System.out.println(sum);
    }

    public abstract static class DoubleReduceNode extends PrestoNode {
        private final FrameSlot slot;
        @Child
        private final ExpressionNode expressionNode;

        public DoubleReduceNode(FrameSlot slot, ExpressionNode expressionNode) {
            this.slot = slot;
            this.expressionNode = this.adoptChild(expressionNode);
        }

        public FrameSlot getSlot() {
            return slot;
        }

        public void execute(VirtualFrame frame) {
            try {
                frame.setDouble(slot, apply(frame.getDouble(slot), expressionNode.executeDouble(frame)));
            } catch (FrameSlotTypeException e) {
                e.printStackTrace();
            } catch (UnexpectedResultException e) {
                e.printStackTrace();
            }
        }

        public abstract double apply(double oldValue, double newValue);
    }

    public static class DoubleSumNode extends DoubleReduceNode {
        public DoubleSumNode(FrameSlot slot, ExpressionNode expression) {
            super(slot, expression);
        }

        @Override
        public double apply(double oldValue, double newValue) {
            return oldValue + newValue;
        }
    }

    @TypeSystem({ boolean.class, long.class, double.class, Slice.class })
    public static class PrestoTypes {

    }

    @TypeSystemReference(PrestoTypes.class)
    public static class PrestoNode extends Node {

    }

    public static abstract class ExpressionNode extends PrestoNode {
        public boolean executeBoolean(VirtualFrame frame) throws UnexpectedResultException {
            return PrestoTypesGen.PRESTOTYPES.expectBoolean(executeGeneric(frame));
        }

        public long executeLong(VirtualFrame frame) throws UnexpectedResultException {
            return PrestoTypesGen.PRESTOTYPES.expectLong(executeGeneric(frame));
        }

        public double executeDouble(VirtualFrame frame) throws UnexpectedResultException {
            return PrestoTypesGen.PRESTOTYPES.expectDouble(executeGeneric(frame));
        }

        public Slice executeSlice(VirtualFrame frame) throws UnexpectedResultException {
            return PrestoTypesGen.PRESTOTYPES.expectSlice(executeGeneric(frame));
        }

        public abstract Object executeGeneric(VirtualFrame frame);
    }

    @NodeChildren({ @NodeChild("leftNode"), @NodeChild("rightNode") })
    public static abstract class BinaryNode extends ExpressionNode {
    }

    public abstract static class MulNode extends BinaryNode {
        @Specialization
        public double doDouble(double left, double right) {
            return left * right;
        }
    }

    public abstract static class LessThanNode extends BinaryNode {
        @Specialization
        public boolean doLong(long left, long right) {
            return left < right;
        }

        @Specialization
        public boolean doDouble(double left, double right) {
            return left < right;
        }

        @Specialization
        public boolean doSlice(Slice left, Slice right) {
            return left.compareTo(right) < 0;
        }
    }

    public abstract static class GreaterEqualsNode extends BinaryNode {
        @Specialization
        public boolean doLong(long left, long right) {
            return left >= right;
        }

        @Specialization
        public boolean doDouble(double left, double right) {
            return left >= right;
        }

        @Specialization
        public boolean doSlice(Slice left, Slice right) {
            return left.compareTo(right) >= 0;
        }
    }

    public abstract static class ConjunctionNode extends BinaryNode {
        @ShortCircuit("rightNode")
        public boolean needsRightNode(boolean left) {
            return left;
        }

        @ShortCircuit("rightNode")
        public boolean needsRightNode(Object left) {
            return left instanceof Boolean && (Boolean) left;
        }

        @Specialization
        public boolean doBoolean(boolean left, boolean hasRight, boolean right) {
            return hasRight && right;
        }
    }

    public abstract static class CellGetNode extends ExpressionNode {
        final private FrameSlot sliceSlot;
        final private FrameSlot rowSlot;

        public CellGetNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
            this.sliceSlot = sliceSlot;
            this.rowSlot = rowSlot;
        }

        protected int getRow(VirtualFrame frame) {
            try {
                return frame.getInt(rowSlot);
            } catch (FrameSlotTypeException e) {
                e.printStackTrace();
                return 0;
            }
        }

        protected Slice getSlice(VirtualFrame frame) {
            try {
                return (Slice) frame.getObject(sliceSlot);
            } catch (FrameSlotTypeException e) {
                e.printStackTrace();
                return null;
            }
        }

        protected FrameSlot getSliceSlot() {
            return sliceSlot;
        }
    }

    private static Object getSliceBase(Slice slice) {
        return unsafe.getObject(slice, baseOffset);
    }

    private static Object getSliceReference(Slice slice) {
        return unsafe.getObject(slice, referenceOffset);
    }

    private static long getSliceAddress(Slice slice) {
        return unsafe.getLong(slice, addressOffset);
    }

    private static void setSliceBase(Slice slice, Object base) {
        unsafe.putObject(slice, baseOffset, base);
    }

    private static void setSliceReference(Slice slice, Object reference) {
        unsafe.putObject(slice, referenceOffset, reference);
    }

    private static void setSliceAddress(Slice slice, long address) {
        unsafe.putLong(slice, addressOffset, address);
    }

    private static void setSliceSize(Slice slice, int size) {
        unsafe.putInt(slice, sizeOffset, size);
    }

    private static void checkIndexLength(int index, int length, Slice slice) {
        checkPositionIndexes(index, index + length, slice.length());
    }
    
    private static void checkPositionIndexes(int start, int end, int size) {
        if (start < 0 || end < start || end > size) {
            CompilerDirectives.transferToInterpreter();
            Preconditions.checkPositionIndexes(start, end, size);
        }
    }
    
    private static void checkArgument(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            CompilerDirectives.transferToInterpreter();
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static class CellGetLongNode extends CellGetNode {
        public CellGetLongNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
            super(sliceSlot, rowSlot);
        }

        @Override
        public long executeLong(VirtualFrame frame) {
            Slice slice = getSlice(frame);
            int index = getRow(frame) * SizeOf.SIZE_OF_LONG;
            checkIndexLength(index, SizeOf.SIZE_OF_LONG, slice);
            // TODO: unsafe access should be guarded by index check. Currently, we use false such that it can not float before the index check.
            return CompilerDirectives.unsafeGetLong(getSliceBase(slice), getSliceAddress(slice) + index, false, getSliceSlot());
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return executeLong(frame);
        }
    }

    public static class CellGetDoubleNode extends CellGetNode {
        public CellGetDoubleNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
            super(sliceSlot, rowSlot);
        }

        @Override
        public double executeDouble(VirtualFrame frame) {
            Slice slice = getSlice(frame);
            int index = getRow(frame) * SizeOf.SIZE_OF_DOUBLE;
            checkIndexLength(index, SizeOf.SIZE_OF_DOUBLE, slice);
            // TODO: unsafe access should be guarded by index check. Currently, we use false such that it can not float before the index check.
            return CompilerDirectives.unsafeGetDouble(getSliceBase(slice), getSliceAddress(slice) + index, false, getSliceSlot());
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return executeDouble(frame);
        }
    }

    public static class CellGetSliceNode extends CellGetNode {
        private final int length;
        private final BranchProfile branchSame = new BranchProfile();
        private final BranchProfile branchEmpty = new BranchProfile();

        public CellGetSliceNode(FrameSlot sliceSlot, FrameSlot rowSlot, int length) {
            super(sliceSlot, rowSlot);
            this.length = length;
        }

        @Override
        public Slice executeSlice(VirtualFrame frame) {
            return helper(getSlice(frame), getRow(frame));
        }

        /**
         * trufflized version {@link Slice#slice(int, int)}.
         */
        private Slice helper(Slice slice, int row) {
            int index = row * length;
            if ((index == 0) && (length == slice.length())) {
                branchSame.enter();
                return slice;
            }
            checkIndexLength(index, length, slice);
            if (length == 0) {
                branchEmpty.enter();
                return Slices.EMPTY_SLICE;
            }

            try {
                Slice newSlice = (Slice) unsafe.allocateInstance(Slice.class);
                long address = getSliceAddress(slice) + index;
                Object base = getSliceBase(slice);
                Object reference = getSliceReference(slice);
                int size = length;

                if (address <= 0) {
                    CompilerDirectives.transferToInterpreter();
                    throw new IllegalArgumentException(format("Invalid address: %s", address));
                }
                if (size <= 0) {
                    CompilerDirectives.transferToInterpreter();
                    throw new IllegalArgumentException(format("Invalid size: %s", size));
                }
                checkArgument((address + size) >= size, "Address + size is greater than 64 bits");

                setSliceReference(newSlice, reference);
                setSliceBase(newSlice, base);
                setSliceAddress(newSlice, address);
                setSliceSize(newSlice, size);

                return newSlice;
            } catch (InstantiationException e) {
                CompilerDirectives.transferToInterpreter();
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return executeSlice(frame);
        }
    }

    public static class LongConstantNode extends ExpressionNode {
        private final long constant;

        public LongConstantNode(long constant) {
            this.constant = constant;
        }

        @Override
        public long executeLong(VirtualFrame frame) {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return constant;
        }
    }

    public static class DoubleConstantNode extends ExpressionNode {
        private final double constant;

        public DoubleConstantNode(double constant) {
            this.constant = constant;
        }

        @Override
        public double executeDouble(VirtualFrame frame) {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return constant;
        }
    }

    public static class SliceConstantNode extends ExpressionNode {
        private final Slice constant;

        public SliceConstantNode(Slice constant) {
            this.constant = constant;
        }

        @Override
        public Slice executeSlice(VirtualFrame frame) {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame) {
            return constant;
        }
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

    public static final class PageArguments extends Arguments {
        public final Page argument;

        public PageArguments(Page argument) {
            this.argument = argument;
        }

        public static Page get(VirtualFrame frame) {
            return frame.getArguments(PageArguments.class).argument;
        }
    }
}
