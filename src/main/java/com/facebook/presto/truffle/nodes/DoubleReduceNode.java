package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

/**
* @author mh
* @since 04.10.14
*/
public abstract class DoubleReduceNode extends PrestoNode {
    @Child private ExpressionNode expressionNode;
    private final FrameSlot slot;

    public DoubleReduceNode(FrameSlot slot, ExpressionNode expressionNode) {
        this.slot = slot;
        this.expressionNode = expressionNode;
    }

    public FrameSlot getSlot() {
        return slot;
    }

    public void execute(VirtualFrame frame) {
        try {
            frame.setDouble(slot, apply(frame.getDouble(slot), expressionNode.executeDouble(frame)));
        } catch (FrameSlotTypeException | UnexpectedResultException e) {
            throw new IllegalStateException("not implemented yet: rewrite of reduce node");
        }
    }

    public abstract double apply(double oldValue, double newValue);
}
