package com.facebook.presto.truffle;

import com.facebook.presto.truffle.nodes.ExpressionNode;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.utilities.BranchProfile;
import io.airlift.slice.Slice;

/**
* @author mh
* @since 04.10.14
*/
public abstract class CellGetNode extends ExpressionNode {
    private final FrameSlot sliceSlot;
    private final FrameSlot rowSlot;
    protected final BranchProfile branchCheckIndex = new BranchProfile();

    public CellGetNode(FrameSlot sliceSlot, FrameSlot rowSlot) {
        this.sliceSlot = sliceSlot;
        this.rowSlot = rowSlot;
    }

    protected int getRow(VirtualFrame frame) {
        try {
            return frame.getInt(rowSlot);
        } catch (FrameSlotTypeException e) {
            throw new IllegalStateException("should not reach here");
        }
    }

    protected Slice getSlice(VirtualFrame frame) {
        try {
            return (Slice) frame.getObject(sliceSlot);
        } catch (FrameSlotTypeException e) {
            throw new IllegalStateException("should not reach here");
        }
    }

    protected FrameSlot getSliceSlot() {
        return sliceSlot;
    }
}
