package com.facebook.presto.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import io.airlift.slice.Slice;

import com.facebook.presto.truffle.TruffleTest.DoubleReduceNode;
import com.facebook.presto.truffle.TruffleTest.ExpressionNode;
import com.facebook.presto.truffle.TruffleTest.FrameMapping;
import com.oracle.truffle.api.CompilerDirectives.SlowPath;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public final class ReduceQueryNode extends RootNode {
    @Child private DoubleReduceNode reduceNode;
    @Child private ExpressionNode filterNode;
    private final FrameMapping[] mapping;
    private final FrameSlot rowSlot;

    public ReduceQueryNode(DoubleReduceNode reduceNode, ExpressionNode filterNode, FrameMapping[] arguments, FrameSlot rowSlot, FrameDescriptor frameDescriptor) {
        super(null,frameDescriptor);
        this.rowSlot = rowSlot;
        this.reduceNode = reduceNode;
        this.filterNode = filterNode;
        this.mapping = arguments;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        Page page = (Page)frame.getArguments()[0];
        initFrame(frame, page);

        for (int row = 0; row < page.getRowCount(); row++) {
            frame.setInt(rowSlot, row);
            try {
                if (filterNode.executeBoolean(frame)) {
                    reduceNode.execute(frame);
                }
            } catch (UnexpectedResultException e) {
                throw new IllegalStateException("not implemented yet: rewrite in reduce node");
            }
        }

        try {
            return frame.getDouble(reduceNode.getSlot());
        } catch (FrameSlotTypeException e) {
            throw new IllegalStateException("should not reach here");
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
