package com.facebook.presto.truffle.nodes;

import com.facebook.presto.truffle.CellGetNode;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
public class CellGetLongNode extends CellGetNode {
    public CellGetLongNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
        super(sliceSlot, rowSlot);
    }

    @Override
    public long executeLong(VirtualFrame frame) {
        Slice slice = getSlice(frame);
        int index = getRow(frame) * SizeOf.SIZE_OF_LONG;
        SliceAccessor.checkIndexLength(index, SizeOf.SIZE_OF_LONG, slice, branchCheckIndex);
        // TODO: unsafe access should be guarded by index check. Currently, we use false such that it can not float before the index check.
        return CompilerDirectives.unsafeGetLong(SliceAccessor.getSliceBase(slice), SliceAccessor.getSliceAddress(slice) + index, false, getSliceSlot());
    }

    @Override
    public Object executeGeneric(VirtualFrame frame) {
        return executeLong(frame);
    }
}
