package com.facebook.presto.truffle;

import io.airlift.slice.Slice;

import com.facebook.presto.truffle.TruffleTest.DoubleReduceNode;
import com.facebook.presto.truffle.TruffleTest.ExpressionNode;
import com.facebook.presto.truffle.TruffleTest.FrameMapping;
import com.facebook.presto.truffle.TruffleTest.PageArguments;
import com.oracle.truffle.api.CompilerDirectives.SlowPath;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public final class ReduceQueryNode extends RootNode {
        @Child
        private final DoubleReduceNode reduceNode;
        @Child
        private final ExpressionNode filterNode;
        private final FrameMapping[] mapping;
        private final FrameSlot rowSlot;

        public ReduceQueryNode(DoubleReduceNode reduceNode,
                        ExpressionNode filterNode, FrameMapping[] arguments,
                        FrameSlot rowSlot) {
                this.rowSlot = rowSlot;
                this.reduceNode = adoptChild(reduceNode);
                this.filterNode = adoptChild(filterNode);
                this.mapping = arguments;
        }

        @Override
        public Object execute(VirtualFrame frame) {
                Page page = PageArguments.get(frame);
                initFrame(frame, page);

                for (int row = 0; row < page.getRowCount(); row++) {
                        frame.setInt(rowSlot, row);
                        try {
                                if (filterNode.executeBoolean(frame)) {
                                        reduceNode.execute(frame);
                                }
                        } catch (UnexpectedResultException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        }
                }

                try {
                        return frame.getDouble(reduceNode.getSlot());
                } catch (FrameSlotTypeException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        return null;
                }
        }

        @ExplodeLoop
        private void initFrame(VirtualFrame frame, Page page) {
                for (FrameMapping frameMapping : mapping) {
                        frame.setObject(frameMapping.getFrameSlot(), getColumnSlow(page, frameMapping));
                }
                frame.setDouble(reduceNode.getSlot(), 0.0);
        }

        @SlowPath
        private Slice getColumnSlow(Page page, FrameMapping frameMapping) {
                return page.getColumn(frameMapping.getColumn());
        }
}