package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.Specialization;

/**
* @author mh
* @since 04.10.14
*/
public abstract class MulNode extends BinaryNode {
    @Specialization
    public double doDouble(double left, double right) {
        return left * right;
    }
}
