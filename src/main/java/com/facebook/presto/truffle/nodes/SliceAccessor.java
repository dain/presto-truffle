package com.facebook.presto.truffle.nodes;

import com.facebook.presto.truffle.TruffleTest;
import com.google.common.base.Preconditions;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.utilities.BranchProfile;
import io.airlift.slice.Slice;
import sun.misc.Unsafe;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

/**
* @author mh
* @since 04.10.14
*/
public class SliceAccessor {

    private final static Unsafe unsafe;
    private final static long baseOffset;
    private final static long referenceOffset;
    private final static long addressOffset;
    private final static long sizeOffset;

    static {
        Unsafe unsafe_ = null;
        long baseOffset_ = -1;
        long referenceOffset_ = -1;
        long addressOffset_ = -1;
        long sizeOffset_ = -1;
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe_ = (Unsafe) field.get(null);
            if (unsafe_ == null) {
                throw new IllegalStateException("Unsafe access not available");
            }

            baseOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("base"));
            referenceOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("reference"));
            addressOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("address"));
            sizeOffset_ = unsafe_.objectFieldOffset(Slice.class.getDeclaredField("size"));
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalStateException("Unsafe failed: " + e);
        }

        unsafe = unsafe_;
        baseOffset = baseOffset_;
        referenceOffset = referenceOffset_;
        addressOffset = addressOffset_;
        sizeOffset = sizeOffset_;
    }

    // Helpers to access Slice datastructure
    public static Object getSliceBase(Slice slice) {
        return unsafe.getObject(slice, baseOffset);
    }

    public static Object getSliceReference(Slice slice) {
        return unsafe.getObject(slice, referenceOffset);
    }

    public static long getSliceAddress(Slice slice) {
        return unsafe.getLong(slice, addressOffset);
    }

    public static void setSliceBase(Slice slice, Object base) {
        unsafe.putObject(slice, baseOffset, base);
    }

    public static void setSliceReference(Slice slice, Object reference) {
        unsafe.putObject(slice, referenceOffset, reference);
    }

    public static void setSliceAddress(Slice slice, long address) {
        unsafe.putLong(slice, addressOffset, address);
    }

    public static void setSliceSize(Slice slice, int size) {
        unsafe.putInt(slice, sizeOffset, size);
    }

    public static Slice newInstance() throws InstantiationException {
        return (Slice) unsafe.allocateInstance(Slice.class);
    }

    public static void checkArgument(boolean expression, @Nullable Object errorMessage, BranchProfile profile) {
        if (!expression) {
            profile.enter();
            throwIllegalArgumentException(errorMessage);
        }
    }

    public static void checkIndexLength(int index, int length, Slice slice, BranchProfile profile) {
        checkPositionIndexes(index, index + length, slice.length(), profile);
    }

    public static void checkPositionIndexes(int start, int end, int size, BranchProfile profile) {
        if (start < 0 || end < start || end > size) {
            profile.enter();
            preconditionsCheckPositionIndexes(start, end, size);
        }
    }

    @CompilerDirectives.SlowPath
    private static void preconditionsCheckPositionIndexes(int start, int end, int size) {
        Preconditions.checkPositionIndexes(start, end, size);
    }

    @CompilerDirectives.SlowPath
    private static void throwIllegalArgumentException(Object errorMessage) {
        throw new IllegalArgumentException(String.valueOf(errorMessage));
    }
}
