package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.Specialization;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
public abstract class LessThanNode extends BinaryNode {
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
