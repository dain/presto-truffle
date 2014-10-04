package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;

/**
* @author mh
* @since 04.10.14
*/
public class LongConstantNode extends ExpressionNode {
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
