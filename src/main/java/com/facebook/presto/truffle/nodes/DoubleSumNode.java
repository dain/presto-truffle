package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.frame.FrameSlot;

/**
* @author mh
* @since 04.10.14
*/
public class DoubleSumNode extends DoubleReduceNode {
    public DoubleSumNode(FrameSlot slot, ExpressionNode expression) {
        super(slot, expression);
    }

    @Override
    public double apply(double oldValue, double newValue) {
        return oldValue + newValue;
    }
}
