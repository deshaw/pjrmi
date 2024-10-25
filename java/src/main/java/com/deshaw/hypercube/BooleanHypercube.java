package com.deshaw.hypercube;

// Recreate with `cog -rc BooleanHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_hypercube
//
//     cog.outl(primitive_hypercube.generate(numpy.bool8))
// ]]]
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.python.DType;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.pjrmi.PJRmi.Kwargs;
import com.deshaw.util.LongBitSet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;

import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which holds {@code boolean} values.
 */
public interface BooleanHypercube
    extends Hypercube<Boolean>
{
    // Public members which look like numpy.ndarray ones

    /**
     * The DType of this hypercube.
     */
    public static final DType dtype =
        new DType(
            ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN) ? ">" : "<" +
            "b1"
        );

    /**
     * The size of the elements, in bytes.
     */
    public static final int itemsize = Byte.BYTES;

    // -------------------------------------------------------------------------

    /**
     * Cast the given cube into a boolean cube, if possible, and give
     * it back. Otherwise copy the cube to a boolean, again if possible,
     * and return the new resulting cube.
     *
     * <p>Since the result may or may not be a copy, the behavior of writing to
     * the returned cube is undefined.
     *
     * @return The boolean cube, or {@code null} if the given cube was null.
     *
     * @throws UnsupportedOperationException if the cube cannot be copied into
     *                                       a boolean cube for some reason.
     */
    @SuppressWarnings("unchecked")
    public static BooleanHypercube toBooleanHypercube(final Hypercube<?> cube)
        throws UnsupportedOperationException
    {
        return (BooleanHypercube)(
            (cube == null || cube instanceof BooleanHypercube)
                ? cube
                : CubeMath.toBooleanHypercube(
                      cube,
                      new BooleanBitSetHypercube(cube.getDimensions())
                  )
        );
    }

    /**
     * Cast the given cube into a boolean cube, if possible, and give
     * it back. Otherwise attempt to wrap the given cube as a boolean
     * one using the {@code BlahFromBlahHypercube} classes.
     *
     * @return The boolean cube, or {@code null} if the given cube
     *         was null or it could not be wrapped.
     */
    @SuppressWarnings("unchecked")
    public static BooleanHypercube asBooleanHypercube(final Hypercube<?> cube)
        throws UnsupportedOperationException
    {
        if (cube == null) {
            return null;
        }

        // Handle casting from the various cube types
        if (cube instanceof BooleanHypercube) {
            return (BooleanHypercube)cube;
        }
        if (cube instanceof IntegerHypercube) {
            return new BooleanFromIntegerHypercube((IntegerHypercube)cube);
        }
        if (cube instanceof LongHypercube) {
            return new BooleanFromLongHypercube((LongHypercube)cube);
        }
        if (cube instanceof FloatHypercube) {
            return new BooleanFromFloatHypercube((FloatHypercube)cube);
        }
        if (cube instanceof DoubleHypercube) {
            return new BooleanFromDoubleHypercube((DoubleHypercube)cube);
        }

        return null;
    }

    /**
     * Returns a new reference to self.
     *
     * <p>This mimics the {@code ndarray.__array__} semantics, but in the Java
     * type-space.
     */
    public default BooleanHypercube array()
    {
        return this;
    }

    /**
     * Returns either a new reference to self if dtype is the same as the class of
     * the elements of this cube, or a new cube of the provided element dtype if
     * it is different.
     *
     * <p>This mimics the {@code ndarray.__array__} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {@code dtype} was {@code null}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final String dtype)
    {
        if (dtype == null) {
            throw new NullPointerException("Given a null dtype");
        }
        final DType.Type type = DType.Type.byName(dtype);
        if (type != null) {
            return array(type.primitiveClass);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to convert to a '" + dtype + "'"
            );
        }
    }

    /**
     * Returns either a new reference to self if dtype is the same as the class of
     * the elements of this cube, or a new cube of the provided element dtype if
     * it is different.
     *
     * <p>This mimics the {@code ndarray.__array__} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {@code dtype} was {@code null}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final DType dtype)
    {
        if (dtype == null) {
            throw new NullPointerException("Given a null dtype");
        }
        else {
            return array(dtype.type().primitiveClass);
        }
    }

    /**
     * Returns either a new reference to self if klass is the same as the class of
     * the elements of this cube, or a new cube of the provided element class if
     * klass is different.
     *
     * <p>This mimics the {@code ndarray.__array__} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {@code klass} was {@code null}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final Class<?> klass)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Check if klass is null
        if (klass == null) {
            throw new NullPointerException("Given null class");
        }
        // Return a reference to this cube if klass is the same as the class of
        // the elements of this cube. We need to do this mundane casework since
        // users might be passing primitive classes as input.
        if (klass.equals(this.getElementType()) ||
            (klass.equals(boolean.class) && Boolean.class.equals(this.getElementType())) ||
            (klass.equals(double .class) && Double .class.equals(this.getElementType())) ||
            (klass.equals(float  .class) && Float  .class.equals(this.getElementType())) ||
            (klass.equals(int    .class) && Integer.class.equals(this.getElementType())) ||
            (klass.equals(long   .class) && Long   .class.equals(this.getElementType())))
        {
            return this;
        }
        else if (klass.equals(Boolean.class) || klass.equals(boolean.class)) {
            return CubeMath.toBooleanHypercube(this);
        }
        else if (klass.equals(Double.class) || klass.equals(double.class)) {
            return CubeMath.toDoubleHypercube(this);
        }
        else if (klass.equals(Float.class) || klass.equals(float.class)) {
            return CubeMath.toFloatHypercube(this);
        }
        else if (klass.equals(Integer.class) || klass.equals(int.class)) {
            return CubeMath.toIntegerHypercube(this);
        }
        else if (klass.equals(Long.class) || klass.equals(long.class)) {
            return CubeMath.toLongHypercube(this);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast to a cube with element type " +
                klass.getSimpleName()
            );
        }
    }

    /**
     * Wrap a {@code boolean} hypercube around the given 1D array.
     */
    public static BooleanHypercube wrap(final boolean[] array)
    {
        return new Boolean1dWrappingHypercube(array);
    }

    /**
     * Wrap a {@code boolean} hypercube around the given 2D array.
     */
    public static BooleanHypercube wrap(final boolean[][] array)
    {
        return new Boolean2dWrappingHypercube(array);
    }

    /**
     * Wrap a {@code boolean} hypercube around the given 3D array.
     */
    public static BooleanHypercube wrap(final boolean[][][] array)
    {
        return new Boolean3dWrappingHypercube(array);
    }

    /**
     * Wrap a {@code boolean} hypercube around the given 4D array.
     */
    public static BooleanHypercube wrap(final boolean[][][][] array)
    {
        return new Boolean4dWrappingHypercube(array);
    }

    /**
     * Wrap a {@code boolean} hypercube around the given 5D array.
     */
    public static BooleanHypercube wrap(final boolean[][][][][] array)
    {
        return new Boolean5dWrappingHypercube(array);
    }

    /**
     * Write out a boolean as little endian.
     */
    private static void writeLittleEndian(final DataOutputStream os,
                                          final boolean v)
        throws IOException
    {
        final ByteBuffer buf = __littleEndianBuffer__.get();
        buf.put(0, v ? (byte)1 : (byte)0);
        os.write(buf.array(), 0, Byte.BYTES);
    }

    /**
     * Read out a boolean as little endian.
     */
    private static boolean readLittleEndian(final DataInputStream is)
        throws IOException
    {
        final ByteBuffer buf = __littleEndianBuffer__.get();
        is.read(buf.array(), 0, Byte.BYTES);
        return buf.get(0) != 0;
    }

    /**
     * For use by {@code writeLittleEndian} and {@code readLittleEndian} only!
     * Declared {@code public} because Java...
     */
    public static ThreadLocal<ByteBuffer> __littleEndianBuffer__ =
        ThreadLocal.withInitial(
            () -> ByteBuffer.allocate(Byte.BYTES)
                            .order(ByteOrder.LITTLE_ENDIAN)
        );

    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public default DType getDType()
    {
        return dtype;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default boolean contentEquals(final Hypercube<?> cube)
    {
        if (cube == null) {
            return false;
        }
        final long size = getSize();
        if (size != cube.getSize()) {
            return false;
        }
        final BooleanHypercube that = asBooleanHypercube(cube);
        if (that == null) {
            return Hypercube.super.contentEquals(cube);
        }
        for (long i=0; i < size; i++) {
            final boolean thisEl = this.getAt(i);
            final boolean thatEl = that.getAt(i);
            if (thisEl != thatEl &&
                !(Double.isNaN(thisEl ? (byte)1 : (byte)0) && Double.isNaN(thatEl ? (byte)1 : (byte)0)))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public default BooleanHypercube reshape(final long[] shape)
    {
        return reshape(Dimension.of(shape));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public default BooleanHypercube reshape(final Dimension<?>[] dimensions)
        throws DimensionalityException,
               NullPointerException
    {
        // Checks
        if (dimensions == null) {
            throw new NullPointerException("Given null dimensions");
        }

        // Ensure that we will have the same number of elements after the
        // reshape
        long size = 1;
        for (Dimension<?> dim : dimensions) {
            if (dim == null) {
                throw new NullPointerException(
                    "Encountered a null dimension in " +
                    Arrays.toString(dimensions)
                );
            }
            size *= dim.length();
        }
        if (size != getSize()) {
            throw new DimensionalityException(
                "New dimensions " + Arrays.toString(dimensions) + " " +
                "are not congruent with existing dimensions " +
                Arrays.toString(getDimensions())
            );
        }

        // Safe to give back the reshaped one. This is just the wrapped version
        // of this cube but with the new dimensions associated with it

        return new BooleanWrappingHypercube(dimensions, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public default BooleanHypercube transpose()
    {
        return new BooleanTransposedHypercube(this);
    }

    /**
     * Fill this hypercube with the given value.
     */
    public default void fill(final boolean v)
    {
        for (long i=0, size = getSize(); i < size; i++) {
            setAt(i, v);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void clear()
    {
        fill(false);
    }

    /**
     * Get a flattened view of this hypercube. These are flattened "C-style";
     * this is the same as {@code numpy}'s default.
     *
     * @param srcPos  The "flattened" position to start copying values from,
     *                within the hypercube.
     * @param dst     Where to put the results.
     * @param dstPos  The position in dst to start copying values into.
     * @param length  How many values to copy.
     *
     * @throws IllegalArgumentException      If {@code length} was negative
     *                                       or {@code dst} was {@code null}.
     * @throws IndexOutOfBoundsException     If {@code srcPos} was not within the
     *                                       bounds of the hypercube, or if
     *                                       {@code srcPos + length} was greater
     *                                       than the size of the hypercube, or
     *                                       if {@code dstPos + length} was
     *                                       beyond {@code dstPos}'s length.
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattened(final long srcPos,
                                    final boolean[] dst,
                                    final int dstPos,
                                    final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Checks
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (dst == null) {
            throw new IllegalArgumentException("Null destination array");
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }
        if (dstPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }
        if (srcPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }
        if (dstPos + length > dst.length) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the destination size, " + dst.length
            );
        }

        // Copy...
        for (int i=0; i < length; i++) {
            dst[i + dstPos] = getAt(i + srcPos);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default boolean[] flatten()
        throws UnsupportedOperationException
    {
        return toArray();
    }

    /**
     * Get a flattened view of this hypercube as a new {@code boolean[]}
     * instance. These are flattened "C-style"; this is the same as {@code numpy}'s
     * default.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default boolean[] toArray()
        throws UnsupportedOperationException
    {
        if (getSize() > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }
        final boolean[] destination = new boolean[(int)getSize()];
        toFlattened(destination);
        return destination;
    }

    /**
     * Get a flattened view of this hypercube. These are flattened "C-style";
     * this is the same as {@code numpy}'s default.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattened(final boolean[] destination)
        throws UnsupportedOperationException
    {
        if (getSize() > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }
        toFlattened(0L, destination, 0, (int)getSize());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void toFlattened(final long             srcPos,
                                    final long             length,
                                    final DataOutputStream os,
                                    final ByteOrder        bo)
        throws IOException,
               IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + "length=" + length
            );
        }

        // Checks
        if (os == null) {
            throw new NullPointerException("Given a null output stream");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }
        if (srcPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }

        // Write it out. We do this in chunks using a staging buffer since
        // that's faster than writing to the DataOutputStream directly.
        final byte[] buf = new byte[16 * Byte.BYTES];
        final ByteBuffer bb = ByteBuffer.wrap(buf).order(bo);
        final int left = (int)(length & 0xf);
        long i = srcPos;
        for (final long end = srcPos + length - left; i < end; /*inside*/) {
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 00
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 01
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 02
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 03
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 04
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 05
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 06
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 07
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 08
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 09
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 10
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 11
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 12
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 13
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 14
            bb.put(getAt(i++) ? (byte)1 : (byte)0); // 15
            os.write(buf, 0, buf.length);
            bb.position(0);
        }

        // Handle any tail values
        switch (left) {
        case 0xf: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0xe: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0xd: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0xc: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0xb: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0xa: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x9: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x8: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x7: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x6: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x5: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x4: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x3: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x2: bb.put(getAt(i++) ? (byte)1 : (byte)0);
        case 0x1: bb.put(getAt(i++) ? (byte)1 : (byte)0);
                  os.write(buf, 0, left * Byte.BYTES);
        }
        os.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void toFlattened(final long       srcPos,
                                    final long       length,
                                    final ByteBuffer buf)
        throws BufferOverflowException,
               IllegalArgumentException,
               IndexOutOfBoundsException,
               ReadOnlyBufferException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + "length=" + length
            );
        }

        // Checks
        if (buf == null) {
            throw new NullPointerException("Given a null buffer");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }
        if (srcPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }

        // Write it out
        for (long i = srcPos, end = srcPos + length; i < end; i++) {
            buf.put(getAt(i) ? (byte)1 : (byte)0);
        }
    }

    /**
     * Get a flattened view of this hypercube interpreted as a {@link LongBitSet}.
     * These are flattened "C-style"; this is the same as {@code numpy}'s
     * default.
     *
     * @param srcPos  The "flattened" position to start copying values from,
     *                within the hypercube.
     * @param dst     Where to put the results.
     * @param dstPos  The position in dst to start copying values into.
     * @param length  How many values to copy.
     *
     * @throws IllegalArgumentException      If {@code length} was negative
     *                                       or {@code dst} was {@code null}.
     * @throws IndexOutOfBoundsException     If {@code srcPos} was not within the
     *                                       bounds of the hypercube, or if
     *                                       {@code srcPos + length} was greater
     *                                       than the size of the hypercube, or
     *                                       if {@code dstPos + length} was
     *                                       beyond {@code dstPos}'s length.
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattened(final long srcPos,
                                    final LongBitSet dst,
                                    final long dstPos,
                                    final long length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Checks
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (dst == null) {
            throw new IllegalArgumentException("Null destination bitset");
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }
        if (dstPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }
        if (srcPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }

        // Turn this cube into something which we can push into the bitset. That
        // means we want it to be a boolean cube. We attempt to coerce it first
        // and, if that fails, create a copy.
        BooleanHypercube bools = BooleanHypercube.asBooleanHypercube(this);
        if (bools == null) {
            bools = BooleanHypercube.toBooleanHypercube(this);
        }

        // Copy...
        for (long i=0; i < length; i++) {
            dst.set(i + dstPos, bools.getAt(i + srcPos));
        }
    }

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @throws IllegalArgumentException   If {@code length} was negative.
     * @throws IndexOutOfBoundsException  If {@code srcPos} was not within the
     *                                    bounds of {@code src}.
     *                                    If {@code srcPos + length} was greater
     *                                    than the size of {@code src}.
     *                                    If {@code dstPos} was not within the
     *                                    bounds of the hypercube.
     *                                    If {@code dstPos + length} was greater
     *                                    than the size of the hypercube.
     * @throws NullPointerException       If {@code src} was {@code null}.
     */
    public default void fromFlattened(final boolean[] src,
                                      final int srcPos,
                                      final long dstPos,
                                      final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Checks
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }
        if (dstPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }
        if (dstPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }
        if (src == null) {
            throw new NullPointerException("Given a null array");
        }
        if (src.length - srcPos < length) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the array size, " + src.length
            );
        }

        // Safe to set, do it the slow way by default
        for (int i=0; i < length; i++) {
            setAt(i + dstPos, src[i + srcPos]);
        }
    }

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @throws IllegalArgumentException If the given array's length did not match
     *                                  this hypercube's size.
     * @throws NullPointerException     If the given array was {@code null}.
     */
    public default void fromFlattened(final boolean[] array)
        throws IllegalArgumentException,
               NullPointerException
    {
        if (array == null) {
            throw new NullPointerException("Given a null array");
        }
        if (array.length != getSize()) {
            throw new IllegalArgumentException(
                "Array length, " + array.length + ", " +
                "did not match size, " + getSize()
            );
        }
        fromFlattened(array, 0, 0L, array.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void fromFlattened(final long            dstPos,
                                      final long            length,
                                      final DataInputStream is,
                                      final ByteOrder       bo)
        throws IOException,
               IllegalArgumentException,
               IndexOutOfBoundsException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "dstPos=" + dstPos + " length=" + length
            );
        }

        // Checks
        if (is == null) {
            throw new NullPointerException("Given a null input stream");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (dstPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }
        if (dstPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }

        // Read them in. We do this in chunks using a staging buffer since
        // that's faster than reading from the DataInputStream directly.
        final byte[] buf = new byte[16 * Byte.BYTES];
        final ByteBuffer bb = ByteBuffer.wrap(buf).order(bo);
        final int left = (int)(length & 0xf);
        long i = dstPos;
        for (final long end = dstPos + length - left; i < end; /*inside*/) {
            is.read(buf, 0, buf.length);
            setAt(i++, bb.get() != 0); // 00
            setAt(i++, bb.get() != 0); // 01
            setAt(i++, bb.get() != 0); // 02
            setAt(i++, bb.get() != 0); // 03
            setAt(i++, bb.get() != 0); // 04
            setAt(i++, bb.get() != 0); // 05
            setAt(i++, bb.get() != 0); // 06
            setAt(i++, bb.get() != 0); // 07
            setAt(i++, bb.get() != 0); // 08
            setAt(i++, bb.get() != 0); // 09
            setAt(i++, bb.get() != 0); // 10
            setAt(i++, bb.get() != 0); // 11
            setAt(i++, bb.get() != 0); // 12
            setAt(i++, bb.get() != 0); // 13
            setAt(i++, bb.get() != 0); // 14
            setAt(i++, bb.get() != 0); // 15
            bb.position(0);
        }
        if (left != 0) {
            is.read(buf, 0, left * Byte.BYTES);
            switch (left) {
            case 0xf: setAt(i++, bb.get() != 0);
            case 0xe: setAt(i++, bb.get() != 0);
            case 0xd: setAt(i++, bb.get() != 0);
            case 0xc: setAt(i++, bb.get() != 0);
            case 0xb: setAt(i++, bb.get() != 0);
            case 0xa: setAt(i++, bb.get() != 0);
            case 0x9: setAt(i++, bb.get() != 0);
            case 0x8: setAt(i++, bb.get() != 0);
            case 0x7: setAt(i++, bb.get() != 0);
            case 0x6: setAt(i++, bb.get() != 0);
            case 0x5: setAt(i++, bb.get() != 0);
            case 0x4: setAt(i++, bb.get() != 0);
            case 0x3: setAt(i++, bb.get() != 0);
            case 0x2: setAt(i++, bb.get() != 0);
            case 0x1: setAt(i++, bb.get() != 0);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void fromFlattened(final long       dstPos,
                                      final long       length,
                                      final ByteBuffer buf)
        throws BufferUnderflowException,
               IllegalArgumentException,
               IndexOutOfBoundsException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "dstPos=" + dstPos + " length=" + length
            );
        }

        // Checks
        if (buf == null) {
            throw new NullPointerException("Given a null buffer");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (dstPos < 0) {
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }
        if (dstPos + length > getSize()) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }

        // Read them in
        for (long i = dstPos, end = dstPos + length; i < end; i++) {
            setAt(i, buf.get() != 0);
        }
    }

    /**
     * Copy the contents of given cube into this one.
     *
     * @throws IllegalArgumentException if the given cube was not compatible for
     *                                  some reason.
     */
    public default void copyFrom(final BooleanHypercube that)
    {
        if (that == null) {
            throw new IllegalArgumentException("Given a null cube to copy from");
        }
        if (!matches(that)) {
            throw new IllegalArgumentException("Given cube is not compatible");
        }
        final long size = getSize();
        for (long i=0; i < size; i++) {
            setAt(i, that.getAt(i));
        }
    }

    /**
     * Get the value at the given indices.
     *
     * @param indices The indices of the element in the hypercube.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public boolean get(final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Set the value at the given indices.
     */
    public void set(final boolean d, final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Get the value at the given {@link Coordinate}s.
     *
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default boolean get(final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        // Make sure that they match dimensionally
        if (coordinates == null) {
            throw new NullPointerException("Given null coordinates");
        }
        if (coordinates.length != getNDim()) {
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + getNDim()
            );
        }

        // Transform to the given indices
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {
            final Coordinate<?> coordinate = coordinates[i];
            if (coordinate == null) {
                throw new NullPointerException(
                    "Coordinate[" + i + "] was null"
                );
            }
            if (!dim(i).equals(coordinate.getDimension())) {
                throw new NullPointerException(
                    "Coordinate[" + i + "] was dimension was " +
                    coordinate.getDimension() + " " +
                    "but expected " + dim(i)
                );
            }
            indices[i] = coordinate.get();
        }

        // And hand off
        return get(indices);
    }

    /**
     * Set the value at the given coordinates.
     *
     * @param value       The value to set.
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default void set(final boolean value,
                            final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        // Make sure that they match dimensionally
        if (coordinates == null) {
            throw new NullPointerException("Given null coordinates");
        }
        if (coordinates.length != getNDim()) {
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + getNDim()
            );
        }

        // Transform to the given indices
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {
            final Coordinate<?> coordinate = coordinates[i];
            if (coordinate == null) {
                throw new NullPointerException(
                    "Coordinate[" + i + "] was null"
                );
            }
            if (!dim(i).equals(coordinate.getDimension())) {
                throw new NullPointerException(
                    "Coordinate[" + i + "] was dimension was " +
                    coordinate.getDimension() + " " +
                    "but expected " + dim(i)
                );
            }
            indices[i] = coordinate.get();
        }

        // And hand off
        set(value, indices);
    }

    /**
     * Get the value value at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public boolean getAt(final long index)
        throws IndexOutOfBoundsException;

    /**
     * Set the element at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public void setAt(final long index, final boolean value)
        throws IndexOutOfBoundsException;

    /**
     * {@inheritDoc}
     */
    @Override
    public default void writeElement(final Boolean el,
                                     final DataOutputStream os,
                                     final ByteOrder bo)
        throws IOException
    {
        if (el == null) {
            os.writeBoolean(false);
        }
        else if (ByteOrder.BIG_ENDIAN.equals(bo)) {
            os.writeBoolean(el);
        }
        else {
            writeLittleEndian(os, el);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void writeElement(final Boolean el,
                                     final ByteBuffer buf)
        throws BufferOverflowException,
               ReadOnlyBufferException
    {
        buf.put(((el == null) ? false : el) ? (byte)1 : (byte)0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default Boolean readElement(final DataInputStream is,
                                             final ByteOrder bo)
        throws IOException
    {
        if (ByteOrder.BIG_ENDIAN.equals(bo)) {
            return is.readBoolean();
        }
        else {
            return readLittleEndian(is);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default Boolean readElement(final ByteBuffer buf)
        throws BufferUnderflowException
    {
        return (buf.get() != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public default void assignFrom(final Object object)
        throws IllegalArgumentException
    {
        // Attempt the native version but defer to the parent one on error
        try {
            nativeAssignFrom(object);
        }
        catch (IllegalArgumentException e1) {
            try {
                // Defer to the parent method to try
                Hypercube.super.assignFrom(object);
            }
            catch (IllegalArgumentException e2) {
                // If that failed then throw the original error
                throw e1;
            }
        }
    }

    /**
     * Whether all the elements of this cube are logically {@code true}.
     */
    @Kwargs("axis,out,keepdims,where")
    public default boolean all(final Map<String,Object> kwargs)
    {
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                case "out":
                case "initial":
                case "where":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unandled kwarg: " + entry
                    );
                }
            }
        }

        for (long i = 0, size = getSize(); i < size; i++) {
            if (!(getAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether any of the elements of this cube are logically {@code true}.
     */
    @Kwargs("axis,out,keepdims,where")
    public default boolean any(final Map<String,Object> kwargs)
    {
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                case "out":
                case "initial":
                case "where":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unandled kwarg: " + entry
                    );
                }
            }
        }

        for (long i = 0, size = getSize(); i < size; i++) {
            if (getAt(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extract the items of this cube using the given boolean array as a mask.
     */
    // GenericReturnType since we want a MaskedBlahHypercube if possible
    @GenericReturnType
    public default Hypercube<Boolean> mask(final boolean[] mask)
    {
        if (mask != null && mask.length == length(0)) {
            return new BooleanMaskedHypercube(this, mask);
        }
        else {
            return Hypercube.super.mask(mask);
        }
    }

    /**
     * AND the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __and__(final boolean value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.and(this, value);
    }

    /**
     * OR the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __or__(final boolean value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.or(this, value);
    }

    /**
     * XOR the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __xor__(final boolean value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.xor(this, value);
    }

    /**
     * NOT all the elements of this cube, inverting their logic values, and
     * returning the result as a newly-created instance.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __invert__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.not(this);
    }

    /**
     * Attempt to do an assignFrom() assuming that the object is a native array.
     */
    private void nativeAssignFrom(final Object object)
        throws IllegalArgumentException
    {
        // What we are trying
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Attempting to assign from " + object);
        }

        // Can't do anything with a null
        if (object == null) {
            throw new IllegalArgumentException("Can't assign from null");
        }

        // Assume we have an array. If it is 1D and of the right size then
        // assume it's a flattened one, if it's multi-dimensional then it needs
        // to be of the same shape.
        final long size = getSize();
        if (object instanceof Number) {
            // A single value which we want to completely fill the cube with
            @SuppressWarnings("unchecked")
            final boolean value = ((Number)object).byteValue() != 0;
            for (long i=0; i < size; i++) {
                setAt(i, value);
            }
        }
        else if (object.getClass().isArray() &&
                 !object.getClass().getComponentType().isArray() &&
                 Array.getLength(object) == size)
        {
            // 1D array, attempt to directly unflatten as the native type
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Attempting to unflatten from 1D array");
            }
            try {
                fromFlattened((boolean[])object);
            }
            catch (Exception e) {
                throw new IllegalArgumentException(
                    "Can't assign from array " + object.getClass(),
                    e
                );
            }
        }
        else {
            // Multi-dimensional array, or one of the wrong absolute size.
            // We walk down and copy over the lowest subarray in each
            // dimension, all of which should all be of the expected length.
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Attempting to unflatten multi-dimensional array");
            }
            final int[] indices = new int[getNDim()-1];
            for (long i = 0; i < size; i += length(getNDim()-1)) {
                // Compute the indices into the source array. We won't need
                // the last one, which should always be zero.
                long index = i / length(getNDim()-1);
                for (int d = getNDim()-2; d >= 0; d--) {
                    indices[d] = (int)(index % length(d));
                    index      =       index / length(d);
                }
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Array indices for offset " + i + " " +
                               "were " + Arrays.toString(indices));
                }

                // Now we can walk down into the source array
                Object array = object;
                for (int d = 0; d < indices.length; d++) {
                    try {
                        // Check the length matches and then grab the next
                        // sub-array
                        final int arrayLength = Array.getLength(array);
                        if (arrayLength != length(d)) {
                            throw new IllegalArgumentException(
                                "Array at dimension had length " +
                                arrayLength + " which does not match " +
                                length(d)
                            );
                        }
                        array = Array.get(array, indices[d]);
                    }
                    catch (ArrayIndexOutOfBoundsException |
                           IllegalArgumentException       |
                           NullPointerException e)
                    {
                        throw new IllegalArgumentException(
                            "Can't assign from array " + object.getClass(),
                            e
                        );
                    }
                }
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Got " + array + " to assign from");
                }

                // And "object" should now be the right array to copy over
                try {
                    // Check the length is right and copy over. Any
                    // failures, like casting, will be caught and treated as
                    // fatal.
                    final int dim = getNDim()-1;
                    final int arrayLength = Array.getLength(array);
                    if (arrayLength != length(dim)) {
                        throw new IllegalArgumentException(
                            "Array at dimension had length " +
                            arrayLength + " which does not match " +
                            length(dim)
                        );
                    }
                    fromFlattened((boolean[])array, 0, i, arrayLength);
                }
                catch (Exception e) {
                    // Hit an error, give up
                    throw new IllegalArgumentException(
                        "Can't assign from array " + object.getClass(),
                        e
                    );
                }
            }
        }
    }
}

// [[[end]]] (checksum: 37cedac1761bc858585c177024e75eaf)
