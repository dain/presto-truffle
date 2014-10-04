package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.ShortCircuit;
import com.oracle.truffle.api.dsl.Specialization;

/**
* @author mh
* @since 04.10.14
*/
public abstract class ConjunctionNode extends BinaryNode {
    @ShortCircuit("rightNode")
    public boolean needsRightNode(boolean left) {
        return left;
    }

    @ShortCircuit("rightNode")
    public boolean needsRightNode(Object left) {
        return left instanceof Boolean && (Boolean) left;
    }

    @Specialization
    public boolean doBoolean(boolean left, boolean hasRight, boolean right) {
        return hasRight && right;
    }
}
