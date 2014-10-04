package com.facebook.presto.truffle.nodes;

import com.facebook.presto.truffle.CellGetNode;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.utilities.BranchProfile;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.lang.String.format;

/**
* @author mh
* @since 04.10.14
*/
public class CellGetSliceNode extends CellGetNode {
    private final int length;
    private final BranchProfile branchSame = new BranchProfile();
    private final BranchProfile branchEmpty = new BranchProfile();
    private final BranchProfile branchCheckArgument = new BranchProfile();
    private final BranchProfile branchInvalidAddress = new BranchProfile();
    private final BranchProfile branchInvalidSize = new BranchProfile();

    public CellGetSliceNode(FrameSlot sliceSlot, FrameSlot rowSlot, int length) {
        super(sliceSlot, rowSlot);
        this.length = length;
    }

    @Override
    public Slice executeSlice(VirtualFrame frame) {
        int row = getRow(frame);
        return helper(getSlice(frame), row);
    }

    /**
     * trufflized version {@link io.airlift.slice.Slice#slice(int, int)}.
     */
    private Slice helper(Slice slice, int row) {
        int index = row * length;
        if ((index == 0) && (length == slice.length())) {
            branchSame.enter();
            return slice;
        }
        SliceAccessor.checkIndexLength(index, length, slice, branchCheckIndex);
        if (length == 0) {
            branchEmpty.enter();
            return Slices.EMPTY_SLICE;
        }

        try {
            Slice newSlice = SliceAccessor.newInstance();
            long address = SliceAccessor.getSliceAddress(slice) + index;
            Object base = SliceAccessor.getSliceBase(slice);
            Object reference = SliceAccessor.getSliceReference(slice);
            int size = length;

            if (address <= 0) {
                branchInvalidAddress.enter();
                throwIllegalArgumentException(address);
            }
            if (size <= 0) {
                branchInvalidSize.enter();
                throwIllegalArgumentException(size);
            }
            SliceAccessor.checkArgument((address + size) >= size, "Address + size is greater than 64 bits", branchCheckArgument);

            SliceAccessor.setSliceReference(newSlice, reference);
            SliceAccessor.setSliceBase(newSlice, base);
            SliceAccessor.setSliceAddress(newSlice, address);
            SliceAccessor.setSliceSize(newSlice, size);

            return newSlice;
        } catch (InstantiationException e) {
            CompilerDirectives.transferToInterpreter();
            throw new IllegalStateException("was not able to perform new Allocation of Slice");
        }
    }

    @CompilerDirectives.SlowPath
    private void throwIllegalArgumentException(int size) {
        throw new IllegalArgumentException(format("Invalid size: %s", size));
    }

    @CompilerDirectives.SlowPath
    private void throwIllegalArgumentException(long address) {
        throw new IllegalArgumentException(format("Invalid address: %s", address));
    }

    @Override
    public Object executeGeneric(VirtualFrame frame) {
        return executeSlice(frame);
    }
}
