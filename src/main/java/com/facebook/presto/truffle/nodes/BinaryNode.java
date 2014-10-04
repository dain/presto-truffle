package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;

/**
* @author mh
* @since 04.10.14
*/
@NodeChildren({ @NodeChild("leftNode"), @NodeChild("rightNode") })
public abstract class BinaryNode extends ExpressionNode {
}
