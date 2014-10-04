package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.nodes.Node;

/**
* @author mh
* @since 04.10.14
*/
@TypeSystemReference(PrestoTypes.class)
public class PrestoNode extends Node {

}
