package com.facebook.presto.truffle.nodes;

import com.facebook.presto.truffle.PrestoTypesGen;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
public abstract class ExpressionNode extends PrestoNode {
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
