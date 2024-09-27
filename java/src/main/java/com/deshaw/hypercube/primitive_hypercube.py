# Code for generating "numeric" cubes. This also handles booleans, which aren't
# really numeric but...

import numpy
import params

_PRIMITIVE_HYPERCUBE_MAIN = '''\
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.python.DType;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.pjrmi.PJRmi.Kwargs;
import com.deshaw.util.LongBitSet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which holds {{@code {primitive_type}}} values.
 */
public interface {object_type}Hypercube
    extends Hypercube<{object_type}>
{{
    // Public members which look like numpy.ndarray ones

    /**
     * The DType of this hypercube.
     */
    public static final DType dtype =
        new DType(
            ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN) ? ">" : "<" +
            "{dtype_str}"
        );

    /**
     * The size of the elements, in bytes.
     */
    public static final int itemsize = {size};

    // -------------------------------------------------------------------------

    /**
     * Cast the given cube into a {primitive_type} cube, if possible, and give
     * it back. Otherwise copy the cube to a {primitive_type}, again if possible,
     * and return the new resulting cube.
     *
     * <p>Since the result may or may not be a copy, the behavior of writing to
     * the returned cube is undefined.
     *
     * @return The {primitive_type} cube, or {{@code null}} if the given cube was null.
     *
     * @throws UnsupportedOperationException if the cube cannot be copied into
     *                                       a {primitive_type} cube for some reason.
     */
    @SuppressWarnings("unchecked")
    public static {object_type}Hypercube to{object_type}Hypercube(final Hypercube<?> cube)
        throws UnsupportedOperationException
    {{
        return ({object_type}Hypercube)(
            (cube == null || cube instanceof {object_type}Hypercube)
                ? cube
                : CubeMath.to{object_type}Hypercube(
                      cube,
                      new {object_type}{default_impl}Hypercube(cube.getDimensions())
                  )
        );
    }}

    /**
     * Cast the given cube into a {primitive_type} cube, if possible, and give
     * it back. Otherwise attempt to wrap the given cube as a {primitive_type}
     * one using the {{@code BlahFromBlahHypercube}} classes.
     *
     * @return The {primitive_type} cube, or {{@code null}} if the given cube
     *         was null or it could not be wrapped.
     */
    @SuppressWarnings("unchecked")
    public static {object_type}Hypercube as{object_type}Hypercube(final Hypercube<?> cube)
        throws UnsupportedOperationException
    {{
        if (cube == null) {{
            return null;
        }}

        // Handle casting from the various cube types
{as_primitive_hypercube}
        return null;
    }}

    /**
     * Returns a new reference to self.
     *
     * <p>This mimics the {{@code ndarray.__array__}} semantics, but in the Java
     * type-space.
     */
    public default {object_type}Hypercube array()
    {{
        return this;
    }}

    /**
     * Returns either a new reference to self if dtype is the same as the class of
     * the elements of this cube, or a new cube of the provided element dtype if
     * it is different.
     *
     * <p>This mimics the {{@code ndarray.__array__}} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {{@code dtype}} was {{@code null}}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final String dtype)
    {{
        if (dtype == null) {{
            throw new NullPointerException("Given a null dtype");
        }}
        final DType.Type type = DType.Type.byName(dtype);
        if (type != null) {{
            return array(type.primitiveClass);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to convert to a '" + dtype + "'"
            );
        }}
    }}

    /**
     * Returns either a new reference to self if dtype is the same as the class of
     * the elements of this cube, or a new cube of the provided element dtype if
     * it is different.
     *
     * <p>This mimics the {{@code ndarray.__array__}} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {{@code dtype}} was {{@code null}}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final DType dtype)
    {{
        if (dtype == null) {{
            throw new NullPointerException("Given a null dtype");
        }}
        else {{
            return array(dtype.type().primitiveClass);
        }}
    }}

    /**
     * Returns either a new reference to self if klass is the same as the class of
     * the elements of this cube, or a new cube of the provided element class if
     * klass is different.
     *
     * <p>This mimics the {{@code ndarray.__array__}} semantics, but in the Java
     * type-space.
     *
     * @throws NullPointerException          If {{@code klass}} was {{@code null}}.
     * @throws UnsupportedOperationException If the given class is not supported.
     */
    @GenericReturnType
    public default Hypercube<?> array(final Class<?> klass)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        // Check if klass is null
        if (klass == null) {{
            throw new NullPointerException("Given null class");
        }}
        // Return a reference to this cube if klass is the same as the class of
        // the elements of this cube. We need to do this mundane casework since
        // users might be passing primitive classes as input.
        if (klass.equals(this.getElementType()) ||
            (klass.equals(boolean.class) && Boolean.class.equals(this.getElementType())) ||
            (klass.equals(double .class) && Double .class.equals(this.getElementType())) ||
            (klass.equals(float  .class) && Float  .class.equals(this.getElementType())) ||
            (klass.equals(int    .class) && Integer.class.equals(this.getElementType())) ||
            (klass.equals(long   .class) && Long   .class.equals(this.getElementType())))
        {{
            return this;
        }}
        else if (klass.equals(Boolean.class) || klass.equals(boolean.class)) {{
            return CubeMath.toBooleanHypercube(this);
        }}
        else if (klass.equals(Double.class) || klass.equals(double.class)) {{
            return CubeMath.toDoubleHypercube(this);
        }}
        else if (klass.equals(Float.class) || klass.equals(float.class)) {{
            return CubeMath.toFloatHypercube(this);
        }}
        else if (klass.equals(Integer.class) || klass.equals(int.class)) {{
            return CubeMath.toIntegerHypercube(this);
        }}
        else if (klass.equals(Long.class) || klass.equals(long.class)) {{
            return CubeMath.toLongHypercube(this);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to cast to a cube with element type " +
                klass.getSimpleName()
            );
        }}
    }}

    /**
     * Wrap a {{@code {primitive_type}}} hypercube around the given 1D array.
     */
    public static {object_type}Hypercube wrap(final {primitive_type}[] array)
    {{
        return new {object_type}1dWrappingHypercube(array);
    }}

    /**
     * Wrap a {{@code {primitive_type}}} hypercube around the given 2D array.
     */
    public static {object_type}Hypercube wrap(final {primitive_type}[][] array)
    {{
        return new {object_type}2dWrappingHypercube(array);
    }}

    /**
     * Wrap a {{@code {primitive_type}}} hypercube around the given 3D array.
     */
    public static {object_type}Hypercube wrap(final {primitive_type}[][][] array)
    {{
        return new {object_type}3dWrappingHypercube(array);
    }}

    /**
     * Wrap a {{@code {primitive_type}}} hypercube around the given 4D array.
     */
    public static {object_type}Hypercube wrap(final {primitive_type}[][][][] array)
    {{
        return new {object_type}4dWrappingHypercube(array);
    }}

    /**
     * Wrap a {{@code {primitive_type}}} hypercube around the given 5D array.
     */
    public static {object_type}Hypercube wrap(final {primitive_type}[][][][][] array)
    {{
        return new {object_type}5dWrappingHypercube(array);
    }}

    /**
     * Write out a {primitive_type} as little endian.
     */
    private static void writeLittleEndian(final DataOutputStream os,
                                          final {primitive_type} v)
        throws IOException
    {{
        final ByteBuffer buf = __littleEndianBuffer__.get();
        buf.put{bytebuffer_type}(0, v{num_from_primitive});
        os.write(buf.array(), 0, {size});
    }}

    /**
     * Read out a {primitive_type} as little endian.
     */
    private static {primitive_type} readLittleEndian(final DataInputStream is)
        throws IOException
    {{
        final ByteBuffer buf = __littleEndianBuffer__.get();
        is.read(buf.array(), 0, {size});
        return buf.get{bytebuffer_type}(0){num_to_primitive};
    }}

    /**
     * For use by {{@code writeLittleEndian}} and {{@code readLittleEndian}} only!
     * Declared {{@code public}} because Java...
     */
    public static ThreadLocal<ByteBuffer> __littleEndianBuffer__ =
        ThreadLocal.withInitial(
            () -> ByteBuffer.allocate({size})
                            .order(ByteOrder.LITTLE_ENDIAN)
        );

    // -------------------------------------------------------------------------

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default DType getDType()
    {{
        return dtype;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default boolean contentEquals(final Hypercube<?> cube)
    {{
        if (cube == null) {{
            return false;
        }}
        final long size = getSize();
        if (size != cube.getSize()) {{
            return false;
        }}
        final {object_type}Hypercube that = as{object_type}Hypercube(cube);
        if (that == null) {{
            return Hypercube.super.contentEquals(cube);
        }}
        for (long i=0; i < size; i++) {{
            final {primitive_type} thisEl = this.getAt(i);
            final {primitive_type} thatEl = that.getAt(i);
            if (thisEl != thatEl &&
                !(Double.isNaN(thisEl{num_from_primitive}) && Double.isNaN(thatEl{num_from_primitive})))
            {{
                return false;
            }}
        }}
        return true;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public default {object_type}Hypercube reshape(final long[] shape)
    {{
        return reshape(Dimension.of(shape));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public default {object_type}Hypercube reshape(final Dimension<?>[] dimensions)
        throws DimensionalityException,
               NullPointerException
    {{
        // Checks
        if (dimensions == null) {{
            throw new NullPointerException("Given null dimensions");
        }}

        // Ensure that we will have the same number of elements after the
        // reshape
        long size = 1;
        for (Dimension<?> dim : dimensions) {{
            if (dim == null) {{
                throw new NullPointerException(
                    "Encountered a null dimension in " +
                    Arrays.toString(dimensions)
                );
            }}
            size *= dim.length();
        }}
        if (size != getSize()) {{
            throw new DimensionalityException(
                "New dimensions " + Arrays.toString(dimensions) + " " +
                "are not congruent with existing dimensions " +
                Arrays.toString(getDimensions())
            );
        }}

        // Safe to give back the reshaped one. This is just the wrapped version
        // of this cube but with the new dimensions associated with it

        return new {object_type}WrappingHypercube(dimensions, this);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public default {object_type}Hypercube transpose()
    {{
        return new {object_type}TransposedHypercube(this);
    }}

    /**
     * Fill this hypercube with the given value.
     */
    public default void fill(final {primitive_type} v)
    {{
        for (long i=0, size = getSize(); i < size; i++) {{
            setAt(i, v);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default void clear()
    {{
        fill({primitive_from_null});
    }}

    /**
     * Get a flattened view of this hypercube. These are flattened "C-style";
     * this is the same as {{@code numpy}}'s default.
     *
     * @param srcPos  The "flattened" position to start copying values from,
     *                within the hypercube.
     * @param dst     Where to put the results.
     * @param dstPos  The position in dst to start copying values into.
     * @param length  How many values to copy.
     *
     * @throws IllegalArgumentException      If {{@code length}} was negative
     *                                       or {{@code dst}} was {{@code null}}.
     * @throws IndexOutOfBoundsException     If {{@code srcPos}} was not within the
     *                                       bounds of the hypercube, or if
     *                                       {{@code srcPos + length}} was greater
     *                                       than the size of the hypercube, or
     *                                       if {{@code dstPos + length}} was
     *                                       beyond {{@code dstPos}}'s length.
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {{@link Integer#MAX_VALUE}} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattened(final long srcPos,
                                    final {primitive_type}[] dst,
                                    final int dstPos,
                                    final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }}

        // Checks
        if (length < 0) {{
            throw new IllegalArgumentException("Bad length: " + length);
        }}
        if (dst == null) {{
            throw new IllegalArgumentException("Null destination array");
        }}
        if (srcPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }}
        if (dstPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }}
        if (srcPos + length > getSize()) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }}
        if (dstPos + length > dst.length) {{
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the destination size, " + dst.length
            );
        }}

        // Copy...
        for (int i=0; i < length; i++) {{
            dst[i + dstPos] = getAt(i + srcPos);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default {primitive_type}[] flatten()
        throws UnsupportedOperationException
    {{
        return toArray();
    }}

    /**
     * Get a flattened view of this hypercube as a new {{@code {primitive_type}[]}}
     * instance. These are flattened "C-style"; this is the same as {{@code numpy}}'s
     * default.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {{@link Integer#MAX_VALUE}} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default {primitive_type}[] toArray()
        throws UnsupportedOperationException
    {{
        if (getSize() > Integer.MAX_VALUE) {{
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }}
        final {primitive_type}[] destination = new {primitive_type}[(int)getSize()];
        toFlattened(destination);
        return destination;
    }}

    /**
     * Get a flattened view of this hypercube. These are flattened "C-style";
     * this is the same as {{@code numpy}}'s default.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {{@link Integer#MAX_VALUE}} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattened(final {primitive_type}[] destination)
        throws UnsupportedOperationException
    {{
        if (getSize() > Integer.MAX_VALUE) {{
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }}
        toFlattened(0L, destination, 0, (int)getSize());
    }}

    /**
     * {{@inheritDoc}}
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
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + "length=" + length
            );
        }}

        // Checks
        if (os == null) {{
            throw new NullPointerException("Given a null output stream");
            }}
        if (length < 0) {{
            throw new IllegalArgumentException("Bad length: " + length);
        }}
        if (srcPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }}
        if (srcPos + length > getSize()) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }}

        // Write it out
        if (ByteOrder.BIG_ENDIAN.equals(bo)) {{
            for (long i = srcPos, end = srcPos + length; i < end; i++) {{
                os.write{short_object_type}(getAt(i));
            }}
        }}
        else {{
            for (long i = srcPos, end = srcPos + length; i < end; i++) {{
                writeLittleEndian(os, getAt(i));
            }}
        }}
        os.flush();
    }}

    /**
     * Get a flattened view of this hypercube interpreted as a {{@link LongBitSet}}.
     * These are flattened "C-style"; this is the same as {{@code numpy}}'s
     * default.
     *
     * @param srcPos  The "flattened" position to start copying values from,
     *                within the hypercube.
     * @param dst     Where to put the results.
     * @param dstPos  The position in dst to start copying values into.
     * @param length  How many values to copy.
     *
     * @throws IllegalArgumentException      If {{@code length}} was negative
     *                                       or {{@code dst}} was {{@code null}}.
     * @throws IndexOutOfBoundsException     If {{@code srcPos}} was not within the
     *                                       bounds of the hypercube, or if
     *                                       {{@code srcPos + length}} was greater
     *                                       than the size of the hypercube, or
     *                                       if {{@code dstPos + length}} was
     *                                       beyond {{@code dstPos}}'s length.
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {{@link Integer#MAX_VALUE}} in size,
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
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }}

        // Checks
        if (length < 0) {{
            throw new IllegalArgumentException("Bad length: " + length);
        }}
        if (dst == null) {{
            throw new IllegalArgumentException("Null destination bitset");
        }}
        if (srcPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }}
        if (dstPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }}
        if (srcPos + length > getSize()) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }}

        // Turn this cube into something which we can push into the bitset. That
        // means we want it to be a boolean cube. We attempt to coerce it first
        // and, if that fails, create a copy.
        BooleanHypercube bools = BooleanHypercube.asBooleanHypercube(this);
        if (bools == null) {{
            bools = BooleanHypercube.toBooleanHypercube(this);
        }}

        // Copy...
        for (long i=0; i < length; i++) {{
            dst.set(i + dstPos, bools.getAt(i + srcPos));
        }}
    }}

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @throws IllegalArgumentException   If {{@code length}} was negative.
     * @throws IndexOutOfBoundsException  If {{@code srcPos}} was not within the
     *                                    bounds of {{@code src}}.
     *                                    If {{@code srcPos + length}} was greater
     *                                    than the size of {{@code src}}.
     *                                    If {{@code dstPos}} was not within the
     *                                    bounds of the hypercube.
     *                                    If {{@code dstPos + length}} was greater
     *                                    than the size of the hypercube.
     * @throws NullPointerException       If {{@code src}} was {{@code null}}.
     */
    public default void fromFlattened(final {primitive_type}[] src,
                                      final int srcPos,
                                      final long dstPos,
                                      final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }}

        // Checks
        if (length < 0) {{
            throw new IllegalArgumentException("Bad length: " + length);
        }}
        if (srcPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad source position: " + srcPos
            );
        }}
        if (dstPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }}
        if (dstPos + length > getSize()) {{
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }}
        if (src == null) {{
            throw new NullPointerException("Given a null array");
        }}
        if (src.length - srcPos < length) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the array size, " + src.length
            );
        }}

        // Safe to set, do it the slow way by default
        for (int i=0; i < length; i++) {{
            setAt(i + dstPos, src[i + srcPos]);
        }}
    }}

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @throws IllegalArgumentException If the given array's length did not match
     *                                  this hypercube's size.
     * @throws NullPointerException     If the given array was {{@code null}}.
     */
    public default void fromFlattened(final {primitive_type}[] array)
        throws IllegalArgumentException,
               NullPointerException
    {{
        if (array == null) {{
            throw new NullPointerException("Given a null array");
        }}
        if (array.length != getSize()) {{
            throw new IllegalArgumentException(
                "Array length, " + array.length + ", " +
                "did not match size, " + getSize()
            );
        }}
        fromFlattened(array, 0, 0L, array.length);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default void fromFlattened(final long            dstPos,
                                      final long            length,
                                      final DataInputStream is,
                                      final ByteOrder        bo)
        throws IOException,
        IllegalArgumentException,
               IndexOutOfBoundsException
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Unflattening with " +
                "dstPos=" + dstPos + " length=" + length
            );
        }}

        // Checks
        if (is == null) {{
            throw new NullPointerException("Given a null input stream");
        }}
        if (length < 0) {{
            throw new IllegalArgumentException("Bad length: " + length);
        }}
        if (dstPos < 0) {{
            throw new IndexOutOfBoundsException(
                "Bad destination position: " + dstPos
            );
        }}
        if (dstPos + length > getSize()) {{
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + getSize()
            );
        }}

        // Read them in
        if (ByteOrder.BIG_ENDIAN.equals(bo)) {{
            for (long i = dstPos, end = dstPos + length; i < end; i++) {{
                setAt(i, is.read{short_object_type}());
            }}
        }}
        else {{
            for (long i = dstPos, end = dstPos + length; i < end; i++) {{
                setAt(i, readLittleEndian(is));
            }}
        }}
    }}

    /**
     * Copy the contents of given cube into this one.
     *
     * @throws IllegalArgumentException if the given cube was not compatible for
     *                                  some reason.
     */
    public default void copyFrom(final {object_type}Hypercube that)
    {{
        if (that == null) {{
            throw new IllegalArgumentException("Given a null cube to copy from");
        }}
        if (!matches(that)) {{
            throw new IllegalArgumentException("Given cube is not compatible");
        }}
        final long size = getSize();
        for (long i=0; i < size; i++) {{
            setAt(i, that.getAt(i));
        }}
    }}

    /**
     * Get the value at the given indices.
     *
     * @param indices The indices of the element in the hypercube.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Set the value at the given indices.
     */
    public void set(final {primitive_type} d, final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Get the value at the given {{@link Coordinate}}s.
     *
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default {primitive_type} get(final Coordinate<?>... coordinates)
        throws DimensionalityException
    {{
        // Make sure that they match dimensionally
        if (coordinates == null) {{
            throw new NullPointerException("Given null coordinates");
        }}
        if (coordinates.length != getNDim()) {{
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + getNDim()
            );
        }}

        // Transform to the given indices
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {{
            final Coordinate<?> coordinate = coordinates[i];
            if (coordinate == null) {{
                throw new NullPointerException(
                    "Coordinate[" + i + "] was null"
                );
            }}
            if (!dim(i).equals(coordinate.getDimension())) {{
                throw new NullPointerException(
                    "Coordinate[" + i + "] was dimension was " +
                    coordinate.getDimension() + " " +
                    "but expected " + dim(i)
                );
            }}
            indices[i] = coordinate.get();
        }}

        // And hand off
        return get(indices);
    }}

    /**
     * Set the value at the given coordinates.
     *
     * @param value       The value to set.
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default void set(final {primitive_type} value,
                            final Coordinate<?>... coordinates)
        throws DimensionalityException
    {{
        // Make sure that they match dimensionally
        if (coordinates == null) {{
            throw new NullPointerException("Given null coordinates");
        }}
        if (coordinates.length != getNDim()) {{
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + getNDim()
            );
        }}

        // Transform to the given indices
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {{
            final Coordinate<?> coordinate = coordinates[i];
            if (coordinate == null) {{
                throw new NullPointerException(
                    "Coordinate[" + i + "] was null"
                );
            }}
            if (!dim(i).equals(coordinate.getDimension())) {{
                throw new NullPointerException(
                    "Coordinate[" + i + "] was dimension was " +
                    coordinate.getDimension() + " " +
                    "but expected " + dim(i)
                );
            }}
            indices[i] = coordinate.get();
        }}

        // And hand off
        set(value, indices);
    }}

    /**
     * Get the value value at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException;

    /**
     * Set the element at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException;

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default void writeElement(final {object_type} el,
                                     final DataOutputStream os,
                                     final ByteOrder bo)
        throws IOException
    {{
        if (el == null) {{
            os.write{short_object_type}({primitive_from_null});
        }}
        else if (ByteOrder.BIG_ENDIAN.equals(bo)) {{
            os.write{short_object_type}(el);
        }}
        else {{
            writeLittleEndian(os, el);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default {object_type} readElement(final DataInputStream is,
                                             final ByteOrder bo)
        throws IOException
    {{
        if (ByteOrder.BIG_ENDIAN.equals(bo)) {{
            return is.read{short_object_type}();
        }}
        else {{
            return readLittleEndian(is);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @SuppressWarnings("unchecked")
    public default void assignFrom(final Object object)
        throws IllegalArgumentException
    {{
        // Attempt the native version but defer to the parent one on error
        try {{
            nativeAssignFrom(object);
        }}
        catch (IllegalArgumentException e1) {{
            try {{
                // Defer to the parent method to try
                Hypercube.super.assignFrom(object);
            }}
            catch (IllegalArgumentException e2) {{
                // If that failed then throw the original error
                throw e1;
            }}
        }}
    }}

    /**
     * Whether all the elements of this cube are logically {{@code true}}.
     */
    @Kwargs("axis,out,keepdims,where")
    public default boolean all(final Map<String,Object> kwargs)
    {{
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
                case "axis":
                case "out":
                case "initial":
                case "where":
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unandled kwarg: " + entry
                    );
                }}
            }}
        }}

        for (long i = 0, size = getSize(); i < size; i++) {{
            if (!(getAt(i){bool_from_primitive})) {{
                return false;
            }}
        }}
        return true;
    }}

    /**
     * Whether any of the elements of this cube are logically {{@code true}}.
     */
    @Kwargs("axis,out,keepdims,where")
    public default boolean any(final Map<String,Object> kwargs)
    {{
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
                case "axis":
                case "out":
                case "initial":
                case "where":
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unandled kwarg: " + entry
                    );
                }}
            }}
        }}

        for (long i = 0, size = getSize(); i < size; i++) {{
            if (getAt(i){bool_from_primitive}) {{
                return true;
            }}
        }}
        return false;
    }}

    /**
     * Extract the items of this cube using the given boolean array as a mask.
     */
    // GenericReturnType since we want a MaskedBlahHypercube if possible
    @GenericReturnType
    public default Hypercube<{object_type}> mask(final boolean[] mask)
    {{
        if (mask != null && mask.length == length(0)) {{
            return new {object_type}MaskedHypercube(this, mask);
        }}
        else {{
            return Hypercube.super.mask(mask);
        }}
    }}
'''

_PRIMITIVE_HYPERCUBE_MATH = '''
    /**
     * {{@inheritDoc}}
     */
    @Override
    public default void setFromObject(final Object obj, final long... indices)
        throws IndexOutOfBoundsException
    {{
        if (obj instanceof Number) {{
            @SuppressWarnings("unchecked")
            final Number number = (Number)obj;
            setObj(number.{primitive_type}Value(), indices);
        }}
        else {{
            Hypercube.super.setFromObject(obj, indices);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default void setFromObjectAt(final long index, final Object obj)
        throws ClassCastException,
               IndexOutOfBoundsException
    {{
        if (obj instanceof Number) {{
            @SuppressWarnings("unchecked")
            final Number number = (Number)obj;
            setObjectAt(index, number.{primitive_type}Value());
        }}
        else {{
            Hypercube.super.setFromObjectAt(index, obj);
        }}
    }}

    /**
     * Get the minimum value in this hypercube.
     *
     * <p>This is a {{@code numpy}} compatibility method and accepts (some of)
     * the {{@code kwargs}} of its {{@code ndarray}} equivalent.
     *
     * @return the minimum value or {{@code {primitive_from_null}}} if none was found.
     */
    @Kwargs(value="axis,initial,where,keepdims,out")
    @GenericReturnType
    public default Object min(final Map<String,Object> kwargs)
    {{
        return CubeMath.min(this, kwargs);
    }}

    /**
     * Get the minimum value in this hypercube.
     *
     * @return the minimum value or {{@code {primitive_from_null}}} if none was found.
     */
    public default {primitive_type} min0d()
    {{
        final long size = getSize();
        boolean any = false;
        {primitive_type} min = {object_type}.MAX_VALUE;
        for (long i=0; i < size; i++) {{
            final {primitive_type} v = getAt(i);
            if (v < min) {{
                min = v;
                any = true;
            }}
        }}
        return any ? min : {primitive_from_null};
    }}

    /**
     * Get the maximum value in this hypercube.
     *
     * <p>This is a {{@code numpy}} compatibility method and accepts (some of)
     * the {{@code kwargs}} of its {{@code ndarray}} equivalent.
     *
     * @return the maximum value or {{@code {primitive_from_null}}} if none was found.
     */
    @Kwargs(value="axis,initial,where,keepdims,out")
    @GenericReturnType
    public default Object max(final Map<String,Object> kwargs)
    {{
        return CubeMath.max(this, kwargs);
    }}

    /**
     * Get the maximum value in this hypercube.
     *
     * @return the maximum value or {{@code {primitive_from_null}}} if none was found.
     */
    public default {primitive_type} max0d()
    {{
        final long size = getSize();
        boolean any = false;
        {primitive_type} max = {object_type}.MIN_VALUE;
        for (long i=0; i < size; i++) {{
            final {primitive_type} v = getAt(i);
            if (v > max) {{
                max = v;
                any = true;
            }}
        }}
        return any ? max : {primitive_from_null};
    }}

    /**
     * Get the sum of all the values in this hypercube.
     *
     * <p>This is a {{@code numpy}} compatibility method and accepts (some of)
     * the {{@code kwargs}} of its {{@code ndarray}} equivalent.
     *
     * @return the sum of all the values or {{@code {primitive_from_null}}} if no elements
     *         were non-null.
     */
    @Kwargs(value="axis,initial,where,keepdims,out")
    @GenericReturnType
    public default Object sum(final Map<String,Object> kwargs)
    {{
        return CubeMath.sum(this, kwargs);
    }}

    /**
     * Get the sum of all the values in this hypercube. Note that this is
     * returned as a {{@code {primitive_type}}} and so may overflow, especially
     * for small types.
     *
     * @return the sum of all the values or {{@code {primitive_from_null}}} if no elements
     *         were non-null.
     */
    public default {primitive_type} sum0d()
    {{
        final long size = getSize();
        boolean any = false;
        boolean nan = false;
        {primitive_type} sum = 0;
        for (long i=0; i < size && !nan; i++) {{
            final {primitive_type} v = getAt(i);
            if (v != 0 || v == 0) {{ // <-- Cheesy NaN check, but for ints too
                sum += v;
                any = true;
            }}
            else {{
                nan = true;
            }}
        }}
        return (any && !nan) ? sum : {primitive_from_null};
    }}

    /**
     * Get the sum of all the values in this hypercube ignoring any NaN values.
     *
     * <p>This is a {{@code numpy}} compatibility method and accepts (some of)
     * the {{@code kwargs}} of its {{@code ndarray}} equivalent.
     *
     * @return the sum of all the values or {{@code 0}} if no elements were
     *         non-null.
     */
    @Kwargs(value="axis,initial,where,keepdims,out")
    @GenericReturnType
    public default Object nansum(final Map<String,Object> kwargs)
    {{
        return CubeMath.nansum(this, kwargs);
    }}

    /**
     * Get the sum of all the values in this hypercube ignoring NaN values.
     *
     * @return the sum of all the values or {{@code 0}} if no elements were
     *         non-null.
     */
    public default {primitive_type} nansum0d()
    {{
        final long size = getSize();
        {primitive_type} sum = 0;
        for (long i=0; i < size; i++) {{
            final {primitive_type} v = getAt(i);
            if (v != 0 || v == 0) {{ // <-- Cheesy NaN check, but for ints too
                sum += v;
            }}
        }}
        return sum;
    }}

    /**
     * Add the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __add__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.add(this, value);
    }}

    /**
     * Subtract the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __sub__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.subtract(this, value);
    }}

    /**
     * Multiply all the elements of this cube by the given value, and give
     * back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __mul__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.multiply(this, value);
    }}

    /**
     * Compute the modulo operation of the elements of this cube by the given
     * value, and give back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __mod__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.mod(this, value);
    }}

    /**
     * Raise all the elements of this cube to the power of the given value to
     * and give back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __pow__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.power(this, value);
    }}

    /**
     * Add the given value to all the elements of this cube, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __iadd__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.add(this, value, this);
    }}

    /**
     * Subtract the given value from all the elements of this cube, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __isub__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.subtract(this, value, this);
    }}

    /**
     * Multiply all the elements of this cube by the given value, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __imul__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.multiply(this, value, this);
    }}

    /**
     * Raise all the elements of this cube by the power of the given value,
     * mutating the elements of this cube in-place and returning it as the
     * result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __ipow__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.power(this, value, this);
    }}

    /**
     * Perform a less-than comparison operation between the elements of this
     * cube and the given value, returning the results in a newly created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __lt__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.less(this, value);
    }}

    /**
     * Perform a less-than-or-equals comparison operation between the elements
     * of this cube and the given value, returning the results in a newly
     * created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __le__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.lessEqual(this, value);
    }}

    /**
     * Perform an equality comparison operation between the elements of this
     * cube and the given value, returning the results in a newly created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __eq__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.equal(this, value);
    }}

    /**
     * Perform a inequality comparison operation between the elements of this
     * cube and the given value, returning the results in a newly created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __ne__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.notEqual(this, value);
    }}

    /**
     * Perform a greater-than-or-equals comparison operation between the
     * elements of this cube and the given value, returning the results in a
     * newly created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __ge__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.greaterEqual(this, value);
    }}

    /**
     * Perform a greater-than comparison operation between the elements of this
     * cube and the given value, returning the results in a newly created cube.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __gt__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
       return CubeMath.greater(this, value);
    }}
'''

_PRIMITIVE_HYPERCUBE_INTEGER_MATH = '''
    /**
     * Divide all the elements of this cube by the given value to and give
     * back the result as a newly-created cube.
     *
     * <p>The resultant cube will containg floating point values, in order to
     * support a "true" divide.
     */
    @GenericReturnType
    public default Hypercube<{float_object_type}> __truediv__(final Number value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        try {{
            final Hypercube<{float_object_type}> copy =
                {float_object_type}Hypercube.to{float_object_type}Hypercube(this);
            return CubeMath.divide(copy, value.{float_primitive_type}Value(), copy);
        }}
        catch (Exception e) {{
            throw new UnsupportedOperationException(
                "Unable to cast to double cube",
                e
            );
        }}
    }}

    /**
     * Divide all the elements of this cube by the given value to and give
     * back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __floordiv__(final Number value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.divide(this, value.{primitive_type}Value());
    }}

    /**
     * Divide all the elements of this cube by the given value, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<Double> __itruediv__(final Number value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        throw new UnsupportedOperationException(
            "Cannot turn an integer cube into a floating point one"
        );
    }}

    /**
     * Divide all the elements of this cube by the given value, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __ifloordiv__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.divide(this, value, this);
    }}
'''

_PRIMITIVE_HYPERCUBE_FLOAT_MATH = '''
    /**
     * Divide all the elements of this cube by the given value to and give
     * back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __truediv__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.divide(this, value);
    }}

    /**
     * Divide all the elements of this cube by the given value to and give
     * back the result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __floordiv__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.floor(CubeMath.divide(this, value));
    }}

    /**
     * Divide all the elements of this cube by the given value, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __itruediv__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.divide(this, value, this);
    }}

    /**
     * Divide all the elements of this cube by the given value, mutating the
     * elements of this cube in-place and returning it as the result.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __ifloordiv__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        CubeMath.divide(this, value, this);
        return CubeMath.floor(this, this);
    }}
'''

_PRIMITIVE_HYPERCUBE_LOGIC = '''
    /**
     * AND the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __and__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.and(this, value);
    }}

    /**
     * OR the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __or__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.or(this, value);
    }}

    /**
     * XOR the given value to all the elements of this cube and give back the
     * result as a newly-created cube.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __xor__(final {primitive_type} value)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.xor(this, value);
    }}

    /**
     * NOT all the elements of this cube, inverting their logic values, and
     * returning the result as a newly-created instance.
     */
    @GenericReturnType
    public default Hypercube<{object_type}> __invert__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return CubeMath.not(this);
    }}
'''

_PRIMITIVE_HYPERCUBE_PRIVATE = '''
    /**
     * Attempt to do an assignFrom() assuming that the object is a native array.
     */
    private void nativeAssignFrom(final Object object)
        throws IllegalArgumentException
    {{
        // What we are trying
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest("Attempting to assign from " + object);
        }}

        // Can't do anything with a null
        if (object == null) {{
            throw new IllegalArgumentException("Can't assign from null");
        }}

        // Assume we have an array. If it is 1D and of the right size then
        // assume it's a flattened one, if it's multi-dimensional then it needs
        // to be of the same shape.
        final long size = getSize();
        if (object instanceof Number) {{
            // A single value which we want to completely fill the cube with
            @SuppressWarnings("unchecked")
            final {primitive_type} value = ((Number)object).{numeric_primitive_type}Value(){num_to_primitive};
            for (long i=0; i < size; i++) {{
                setAt(i, value);
            }}
        }}
        else if (object.getClass().isArray() &&
                 !object.getClass().getComponentType().isArray() &&
                 Array.getLength(object) == size)
        {{
            // 1D array, attempt to directly unflatten as the native type
            if (LOG.isLoggable(Level.FINEST)) {{
                LOG.finest("Attempting to unflatten from 1D array");
            }}
            try {{
                fromFlattened(({primitive_type}[])object);
            }}
            catch (Exception e) {{
                throw new IllegalArgumentException(
                    "Can't assign from array " + object.getClass(),
                    e
                );
            }}
        }}
        else {{
            // Multi-dimensional array, or one of the wrong absolute size.
            // We walk down and copy over the lowest subarray in each
            // dimension, all of which should all be of the expected length.
            if (LOG.isLoggable(Level.FINEST)) {{
                LOG.finest("Attempting to unflatten multi-dimensional array");
            }}
            final int[] indices = new int[getNDim()-1];
            for (long i = 0; i < size; i += length(getNDim()-1)) {{
                // Compute the indices into the source array. We won't need
                // the last one, which should always be zero.
                long index = i / length(getNDim()-1);
                for (int d = getNDim()-2; d >= 0; d--) {{
                    indices[d] = (int)(index % length(d));
                    index      =       index / length(d);
                }}
                if (LOG.isLoggable(Level.FINEST)) {{
                    LOG.finest("Array indices for offset " + i + " " +
                               "were " + Arrays.toString(indices));
                }}

                // Now we can walk down into the source array
                Object array = object;
                for (int d = 0; d < indices.length; d++) {{
                    try {{
                        // Check the length matches and then grab the next
                        // sub-array
                        final int arrayLength = Array.getLength(array);
                        if (arrayLength != length(d)) {{
                            throw new IllegalArgumentException(
                                "Array at dimension had length " +
                                arrayLength + " which does not match " +
                                length(d)
                            );
                        }}
                        array = Array.get(array, indices[d]);
                    }}
                    catch (ArrayIndexOutOfBoundsException |
                           IllegalArgumentException       |
                           NullPointerException e)
                    {{
                        throw new IllegalArgumentException(
                            "Can't assign from array " + object.getClass(),
                            e
                        );
                    }}
                }}
                if (LOG.isLoggable(Level.FINEST)) {{
                    LOG.finest("Got " + array + " to assign from");
                }}

                // And "object" should now be the right array to copy over
                try {{
                    // Check the length is right and copy over. Any
                    // failures, like casting, will be caught and treated as
                    // fatal.
                    final int dim = getNDim()-1;
                    final int arrayLength = Array.getLength(array);
                    if (arrayLength != length(dim)) {{
                        throw new IllegalArgumentException(
                            "Array at dimension had length " +
                            arrayLength + " which does not match " +
                            length(dim)
                        );
                    }}
                    fromFlattened(({primitive_type}[])array, 0, i, arrayLength);
                }}
                catch (Exception e) {{
                    // Hit an error, give up
                    throw new IllegalArgumentException(
                        "Can't assign from array " + object.getClass(),
                        e
                    );
                }}
            }}
        }}
    }}
}}
'''

def generate(dtype):
    kwargs = params.get_kwargs(dtype)

    # Some additional params just for us
    kwargs['dtype_str'] = kwargs['dtype'].str[1:]

    # Handle non-numeric primitives (i.e. booleans) specially
    if dtype == numpy.bool_:
        kwargs['bytebuffer_type'       ] = ''
        kwargs['numeric_primitive_type'] = 'byte'
        kwargs['size'                  ] = 'Byte.BYTES'
    else:
        kwargs['bytebuffer_type'       ] = kwargs['short_object_type']
        kwargs['numeric_primitive_type'] = kwargs['primitive_type']
        kwargs['size'                  ] = kwargs['object_type'] + '.BYTES'

    # Floating-point equivalents for (mainly integer) types
    if dtype in (numpy.int8, numpy.int16, numpy.int32, numpy.bool_):
        kwargs['float_primitive_type'] = 'float'
        kwargs['float_object_type'   ] = 'Float'
    elif dtype in (numpy.int64,):
        kwargs['float_primitive_type'] = 'double'
        kwargs['float_object_type'   ] = 'Double'
    elif dtype in (numpy.float32, numpy.float64):
        kwargs['float_primitive_type'] = kwargs['primitive_type']
        kwargs['float_object_type'   ] = kwargs['object_type'   ]
    else:
        kwargs['float_primitive_type'] = 'double'
        kwargs['float_object_type'   ] = 'Double'

    # Now generate. Only the numeric cubes should have the math operations.
    template = _PRIMITIVE_HYPERCUBE_MAIN
    if dtype != numpy.bool_:
        template += _PRIMITIVE_HYPERCUBE_MATH
        if dtype in (numpy.int8, numpy.int16, numpy.int32, numpy.int64):
            template += _PRIMITIVE_HYPERCUBE_INTEGER_MATH
            template += _PRIMITIVE_HYPERCUBE_LOGIC
        if dtype in (numpy.float32, numpy.float64):
            template += _PRIMITIVE_HYPERCUBE_FLOAT_MATH
    else:
        template += _PRIMITIVE_HYPERCUBE_LOGIC

    template += _PRIMITIVE_HYPERCUBE_PRIVATE

    # The code for the 'asBlahHypercube' method
    as_primitive_hypercube = ''
    for dt in (numpy.bool_, numpy.int32, numpy.int64, numpy.float32, numpy.float64):
        kws = dict(**kwargs)
        for (k, v) in params.get_kwargs(dt).items():
            kws['cube_' + k] = v
        if dt == dtype:
            as_primitive_hypercube += '''\
        if (cube instanceof {object_type}Hypercube) {{
            return ({object_type}Hypercube)cube;
        }}
'''.format(**kws)
        elif dt != numpy.bool_:
            as_primitive_hypercube += '''\
        if (cube instanceof {cube_object_type}Hypercube) {{
            return new {object_type}From{cube_object_type}Hypercube(({cube_object_type}Hypercube)cube);
        }}
'''.format(**kws)
    kwargs['as_primitive_hypercube'] = as_primitive_hypercube

    return template.format(**kwargs)
