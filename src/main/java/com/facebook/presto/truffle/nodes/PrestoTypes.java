package com.facebook.presto.truffle.nodes;

import com.oracle.truffle.api.dsl.TypeSystem;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
@TypeSystem({ boolean.class, long.class, double.class, Slice.class })
public class PrestoTypes {

}
