package com.facebook.presto.truffle;

import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;

import static com.facebook.presto.truffle.TpchDataGenerator.DATE_STRING_LENGTH;
import static com.facebook.presto.truffle.TpchDataGenerator.DISCOUNT;
import static com.facebook.presto.truffle.TpchDataGenerator.PRICE;
import static com.facebook.presto.truffle.TpchDataGenerator.QUANTITY;
import static com.facebook.presto.truffle.TpchDataGenerator.SHIP_DATE;
import static com.facebook.presto.truffle.TpchDataGenerator.generateTestData;

import io.airlift.slice.Slice;

import java.util.List;

import com.oracle.truffle.api.Arguments;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;

public class TruffleTest
{
	public static void main(String[] args)
    {
        List<Page> pages = generateTestData();
        TruffleRuntime runtime = Truffle.getRuntime();
        FrameDescriptor desc = new FrameDescriptor();

        FrameSlot rowSlot = desc.addFrameSlot("row", FrameSlotKind.Int);
        FrameMapping[] mapping = new FrameMapping[] {
        		new FrameMapping(PRICE, desc.addFrameSlot("PRICE", FrameSlotKind.Object)),
        		new FrameMapping(DISCOUNT, desc.addFrameSlot("DISCOUNT", FrameSlotKind.Object)),
        		new FrameMapping(SHIP_DATE, desc.addFrameSlot("SHIP_DATE", FrameSlotKind.Object)),
        		new FrameMapping(QUANTITY, desc.addFrameSlot("QUANTITY", FrameSlotKind.Object)),
        		};

        ExpressionNode expressionNode = new MulNode(
        		new CellGetDoubleNode(mapping[0].getFrameSlot(), rowSlot),
        		new CellGetDoubleNode(mapping[1].getFrameSlot(), rowSlot));
        SumNode sumNode = new SumNode(desc.addFrameSlot("sum", FrameSlotKind.Object), expressionNode);
        
        ExpressionNode filterNode = null;
        
        

		CallTarget call = runtime.createCallTarget(new ReduceQueryNode(sumNode, filterNode, mapping, rowSlot));

        double sum = 0;
        for (int i = 0; i < 1000; i++) {
            long start = System.nanoTime();

            // TODO: pass arguments
            sum += (double) call.call();
            long duration = System.nanoTime() - start;
            System.out.printf("%6.2fms\n", duration / 1e6);
        }
        System.out.println(sum);
    }

    public static final class ReduceQueryNode
            extends RootNode
    { 
        @Child private final ReduceNode reduceNode;
        @Child private final ExpressionNode filterNode;
		private final FrameMapping[] mapping;
		private final FrameSlot rowSlot;

        public ReduceQueryNode(ReduceNode reduceNode, ExpressionNode filterNode, FrameMapping[] arguments, FrameSlot rowSlot) {
			this.rowSlot = rowSlot;
			this.reduceNode = adoptChild(reduceNode);
			this.filterNode = adoptChild(filterNode);
			this.mapping = arguments;
		}

		@Override
        public Object execute(VirtualFrame frame)
        {
			Page page = PageArguments.get(frame);
			setFrame(frame, page);
			for (int row = 0; row < page.getRowCount(); row++) {
				frame.setInt(rowSlot, row);
				if((Boolean) filterNode.execute(frame)) {
					reduceNode.execute(frame);
				}
			}
			
			try {
				return frame.getObject(reduceNode.getSlot());
			} catch (FrameSlotTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
        }

		@ExplodeLoop
		private void setFrame(VirtualFrame frame, Page page) {
            for (FrameMapping frameMapping : mapping) {
				frame.setObject(frameMapping.getFrameSlot(), page.getColumn(frameMapping.getColumn()));
			}
		}

    }
    
    public abstract static class ReduceNode extends Node {
    	private final FrameSlot slot;
    	@Child private final ExpressionNode expressionNode;

    	public ReduceNode(FrameSlot slot, ExpressionNode expressionNode) {
    		this.slot = slot;
    		this.expressionNode = this.adoptChild(expressionNode);
    	}
    	
    	public FrameSlot getSlot() {
			return slot;
		}

    	public void execute(VirtualFrame frame) {
    		try {
				frame.setObject(slot, apply(frame.getObject(slot), expressionNode.execute(frame)));
			} catch (FrameSlotTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	public abstract Object apply(Object oldValue, Object newValue);
    }
    
    public static class SumNode extends ReduceNode {
		public SumNode(FrameSlot slot, ExpressionNode expression) {
			super(slot, expression);
		}

		@Override
		public Object apply(Object oldValue, Object newValue) {
			Double oldDouble = (Double) oldValue;
			Double newDouble = (Double) newValue;
			return oldDouble + newDouble;
		}
    }
    
    public static abstract class ExpressionNode extends Node {
    	public abstract Object execute(VirtualFrame frame);
    }
    
    public static class MulNode extends ExpressionNode {
    	@Child final private ExpressionNode left;
		@Child final private ExpressionNode right;

		public MulNode(ExpressionNode left, ExpressionNode right) {
			this.left = adoptChild(left);
			this.right = adoptChild(right);
    	}

		@Override
		public Object execute(VirtualFrame frame) {
			return (Double) left.execute(frame) * (Double) right.execute(frame);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 0;
			}
		}
		
		protected Slice getSlice(VirtualFrame frame) {
			try {
				return (Slice) frame.getObject(sliceSlot);
			} catch (FrameSlotTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
    }
    
    public static class CellGetLongNode extends CellGetNode {
		public CellGetLongNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
			super(sliceSlot, rowSlot);
		}

		@Override
		public Object execute(VirtualFrame frame) {
			return getSlice(frame).getLong(getRow(frame));
		}
    }
    public static class CellGetDoubleNode extends CellGetNode {
		public CellGetDoubleNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
			super(sliceSlot, rowSlot);
		}

		@Override
		public Object execute(VirtualFrame frame) {
			return getSlice(frame).getDouble(getRow(frame));
		}
    }
    public static class CellGetSliceNode extends CellGetNode {
		final private int length;

		public CellGetSliceNode(FrameSlot sliceSlot, FrameSlot rowSlot, int length) {
			super(sliceSlot, rowSlot);
			this.length = length;
		}

		@Override
		public Object execute(VirtualFrame frame) {
			return getSlice(frame).slice(getRow(frame) * length, length);
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
