package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;

/**
* @author mh
* @since 04.10.14
*/
public class DoubleConstantNode extends ExpressionNode {
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
