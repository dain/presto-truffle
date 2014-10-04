package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
public class SliceConstantNode extends ExpressionNode {
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
