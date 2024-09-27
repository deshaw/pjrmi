package com.deshaw.hypercube;

import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.ArrayLike;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.pjrmi.PJRmi.Kwargs;
import com.deshaw.pjrmi.KwargUtil;
import com.deshaw.pjrmi.PythonSlice;
import com.deshaw.python.DType;
import com.deshaw.util.LongBitSet;
import com.deshaw.util.StringUtil;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.lang.reflect.Array;

import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The abstraction of a multi-dimensional array.
 *
 * <p>Hypercubes are intended to provide a limited {@code numpy.ndarray}
 * duck-type to give natural access to Java multi-dimensional arrays on the
 * Python side.
 *
 * <p>The {@link Iterable} semantics are those of {@code ndarray}s, meaning that
 * it's on a first-axis basis, as opposed to a per-element basis.
 *
 * @param <T> The type of the element which we store.
 */
public interface Hypercube<T>
    extends ArrayLike,
            Iterable
{
    /**
     * Flags duck-type object for {@code ndarray.flags}.
     */
    public static class Flags
    {
        public final Boolean behaved;
        public final Boolean c_contiguous;
        public final Boolean carray;
        public final Boolean contiguous;
        public final Boolean f_contiguous;
        public final Boolean farray;
        public final Boolean fnc;
        public final Boolean forc;
        public final Boolean fortran;
        public final Boolean num;
        public final Boolean owndata;
        public final Boolean updateifcopy;
        public final Boolean writeable;
        public final Boolean writebackifcopy;

        protected Flags(final Map<String,Boolean> map)
        {
            behaved         = map.getOrDefault("behaved",         null);
            c_contiguous    = map.getOrDefault("c_contiguous",    null);
            carray          = map.getOrDefault("carray",          null);
            contiguous      = map.getOrDefault("contiguous",      null);
            f_contiguous    = map.getOrDefault("f_contiguous",    null);
            farray          = map.getOrDefault("farray",          null);
            fnc             = map.getOrDefault("fnc",             null);
            forc            = map.getOrDefault("forc",            null);
            fortran         = map.getOrDefault("fortran",         null);
            num             = map.getOrDefault("num",             null);
            owndata         = map.getOrDefault("owndata",         null);
            updateifcopy    = map.getOrDefault("updateifcopy",    null);
            writeable       = map.getOrDefault("writeable",       null);
            writebackifcopy = map.getOrDefault("writebackifcopy", null);
        }

        @Override
        public String toString()
        {
            final String t = "True\n";
            final String f = "False\n";
            final StringBuilder sb = new StringBuilder();
            if (behaved         != null) sb.append("  BEHAVED : "        + (behaved         ? t : f));
            if (c_contiguous    != null) sb.append("  C_CONTIGUOUS : "   + (c_contiguous    ? t : f));
            if (carray          != null) sb.append("  CARRAY : "         + (carray          ? t : f));
            if (contiguous      != null) sb.append("  CONTIGUOUS : "     + (contiguous      ? t : f));
            if (f_contiguous    != null) sb.append("  F_CONTIGUOUS : "   + (f_contiguous    ? t : f));
            if (farray          != null) sb.append("  FARRAY : "         + (farray          ? t : f));
            if (fnc             != null) sb.append("  FNC : "            + (fnc             ? t : f));
            if (forc            != null) sb.append("  FORC : "           + (forc            ? t : f));
            if (fortran         != null) sb.append("  FORTRAN : "        + (fortran         ? t : f));
            if (num             != null) sb.append("  NUM : "            + (num             ? t : f));
            if (owndata         != null) sb.append("  OWNDATA : "        + (owndata         ? t : f));
            if (updateifcopy    != null) sb.append("  UPDATEIFCOPY : "   + (updateifcopy    ? t : f));
            if (writeable       != null) sb.append("  WRITEABLE : "      + (writeable       ? t : f));
            if (writebackifcopy != null) sb.append("  WRITEBACKIFCOPY : "+ (writebackifcopy ? t : f));
            return sb.toString();
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Logger.
     */
    public static final Logger LOG = Logger.getLogger("com.deshaw.hypercube");

    /**
     * Multi-dimensional cubes can be big so don't go too deep on them in ipython.
     */
    public static final int __repr_pretty_limit__ = 10;

    /**
     * The Hypercube which we wrap (which, for a non-wrapping instance, will be
     * {@code null}). This is here so that instances will duck-type as expected
     * on the Python side.
     */
    public static final Hypercube<?> base = null;

    /**
     * Get the {@link DType} of this hypercube, or {@code null} if it is not known.
     */
    public default DType getDType()
    {
        return null;
    }

    /**
     * Get the type of our elements.
     */
    public Class<T> getElementType();

    /**
     * Get a copy of the dimensions array.
     */
    public Dimension<?>[] getDimensions();

    /**
     * Get the shape of the hypercube. This is the lengths of all the
     * dimensions, in order.
     *
     * @return a newly-created {@code long[]} containing the shape.
     */
    public default long[] getShape()
    {
        final long[] shape = new long[getNDim()];
        for (int i=0; i < shape.length; i++) {
            shape[i] = length(i);
        }
        return shape;
    }

    /**
     * Get the shape of the hypercube as a list of {@link Long}s. This is the
     * lengths of all the dimensions, in order.
     *
     * @return a {@code List<Long>} containing the shape.
     */
    public default List<Long> getBoxedShape()
    {
        final long[] shape = getShape();
        final List<Long> result = new ArrayList<>(shape.length);
        for (long len : shape) {
            result.add(len);
        }
        return result;
    }

    /**
     * How many dimensions this hypercube has.
     */
    public int getNDim();

    /**
     * Get the {@code i}th dimension.
     *
     * @throws IndexOutOfBoundsException If {@code i} was out of bounds.
     */
    public Dimension<?> dim(final int i)
        throws IndexOutOfBoundsException;

    /**
     * Get the length of the {@code i}th dimension.
     *
     * @throws IndexOutOfBoundsException If {@code i} was out of bounds.
     */
    public default long length(final int i)
        throws IndexOutOfBoundsException
    {
        return dim(i).length();
    }

    /**
     * Get the total number of elements in this hypercube.
     */
    public default long getSize()
    {
        long size = 1;
        for (int i = 0, ndim = getNDim(); i < ndim; i++) {
            size *= length(i);
        }
        return size;
    }

    /**
     * Whether this cube is a singleton value; i.e. it has exactly one element
     * regardless of the number of dimensions.
     */
    public default boolean isSingleton()
    {
        for (int i=0; i < getNDim(); i++) {
            if (length(i) != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether this hypercube instance matches the given one. For this to be
     * true, the following properties must match between the two cubes:<ol>
     *   <li>The element type.
     *   <li>The dimensions.
     *   <li>The shape.
     * </ol>
     *
     * <p>Pairwise operations, like addition, for any two matching cubes should
     * be possible and might potentially produce a resultant matching cube.
     */
    public default boolean matches(final Hypercube<T> that)
    {
        return
            // Trivially true for self
            this == that ||
            // Else we check each criteria
            (that != null                                   &&
             getElementType().equals(that.getElementType()) &&
             Arrays.equals(this.getDimensions(),
                           that.getDimensions())            &&
             Arrays.equals(this.getShape(),
                           that.getShape()));
    }

    /**
     * Whether this hypercube instance matches the given one in the higher
     * dimensions. This is essentially saying whether {@code that} is a
     * compatible non-strict subcube of this one.
     *
     * <p>Note that the semantics of this are like those of Java
     * {@code Collection::contains(that)}, where we are seeing this {@code this}
     * contains {@code that}. This is opposite from the Python convention for
     * things like {@code set::issubset(that)}.
     */
    public default boolean submatches(final Hypercube<T> that)
    {
        // Similar to matches(), since simple checks
        if (this == that) {
            return true;
        }
        if (that == null                                    ||
            !getElementType().equals(that.getElementType()) ||
            getNDim() < that.getNDim())
        {
            return false;
        }

        // Now we want to compare the higher dimensions
        final Dimension<?>[] thisDim = this.getDimensions();
        final Dimension<?>[] thatDim = that.getDimensions();
        for (int i = 1; i < thatDim.length; i++) {
            if (thisDim[thisDim.length-i].length() !=
                thatDim[thatDim.length-i].length())
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether this hypercube instance matches the given one in shape. For this
     * to be true, the following properties must match between the two cubes:<ol>
     *   <li>The shape.
     * </ol>
     *
     * <p>The main difference of this method with the method {@code matches} is
     * that it does not check for element type equality or dimension equality.
     */
    public default boolean matchesInShape(final Hypercube<?> that)
    {
        return
            // Trivially true for self
            this == that ||
            // Else we check each criteria
            (that != null &&
             Arrays.equals(this.getShape(),
                           that.getShape()));
    }

    /**
     * Whether this cube matches another one.
     */
    @SuppressWarnings("unchecked")
    public default boolean cubeEquals(final Hypercube<?> that)
    {
        if (!(that instanceof Hypercube)) {
            return false;
        }
        if (that == this) {
            return true;
        }

        // Cast into the type of cube which we expect. The matches() call will
        // ensure that the <T> param is correct.
        final Hypercube<T> cube = (Hypercube<T>)that;
        if (!matches(cube)) {
            return false;
        }

        // Compare all the elements
        return contentEquals(cube);
    }

    /**
     * Compare the contents of another cube to this one.
     */
    public default boolean contentEquals(final Hypercube<?> that)
    {
        if (that == null) {
            return false;
        }
        final long size = getSize();
        if (size != that.getSize()) {
            return false;
        }
        for (long i=0; i < size; i++) {
            if (!Objects.equals(this.getObjectAt(i), that.getObjectAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a new hypercube which is a view on this hypercube, sliced by the
     * given {@link Accessor}s. If any of the accessors are {@link Coordinate}s
     * then the resultant {@link Hypercube} will, accordingly, have fewer
     * dimensions than this source hypercube. This follows the {@code Python}
     * slicing semantics.
     *
     * @param accessors  How to slice this cube. The number of accessors must
     *                   match the number of dimensions of this {@link
     *                   Hypercube}. The accessors must be derived from this
     *                   hypercube's dimensions; however an accessor may be
     *                   {@code null} if it does not apply.
     *
     * @throws DimensionalityException  If the slices did not match this
     *                                  {@link Hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public Hypercube<T> slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * Create a new hypercube which is a view on this hypercube, rolled by the
     * given {@link Roll}s. This follows the {@code NumPy} rolling semantics.
     *
     * @param rolls  How to roll this cube. The number of rolls must match the
     *               number of dimensions of this {@link Hypercube}. The rolls
     *               must be derived from this hypercube's dimensions; however a
     *               roll may be {@code null} if it does not apply.
     *
     * @throws DimensionalityException  If the rolls did not match this
     *                                  {@link Hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public Hypercube<T> roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException;

    /**
     * Create a new hypercube which is a view on this hypercube, rolled by the
     * given shift value. The array is flattened before shifting, after which
     * the original shape is restored. This follows the {@code NumPy} rolling
     * semantics, e.g: <pre>
     *
     *    [[1, 2, 3],        [[9, 1, 2],
     *     [4, 5, 6],   ==>   [3, 4, 5],
     *     [7, 8, 9]]         [6, 7, 8]]
     *
     * </pre>
     *
     * @param shift  The number of elements to shift this cube by.
     */
    public Hypercube<T> rollFlat(final long shift);

    /**
     * Roll the cube along the {@code n}th axis, such that sub-cubes which are
     * pushed off one end appear on the other, e.g: <pre>
     *
     *    [[1, 2, 3],        [[3, 1, 2],
     *     [4, 5, 6],   ==>   [6, 4, 5],
     *     [7, 8, 9]]         [9, 7, 8]]
     *
     * </pre>
     *
     * The result of this call will be a view on the original cube, similar to
     * performing a slice operation.
     *
     * <p>If the kwargs are not supplied, or are empty, then this becomes a call
     * to {@link rollFlat(long)}.
     *
     * <p>This is equivalent to the {@code numpy.roll()} method.
     *
     * @param shift   The (signed) number of elements to roll this cube by.
     * @param kwargs  <ul>
     *                  <li>{@code axis=0} -- The axis to roll.</li>
     *                </ul>
     *
     * @throws IndexOutOfBoundsException If the given axis did not exist.
     *
     * @throws IllegalArgumentException  If there was any other problem with the
     *                                   arguments.
     */
    @Kwargs(value="axis")
    @GenericReturnType
    public default Hypercube<T> roll(final long shift, final Map<String,Object> kwargs)
        throws IndexOutOfBoundsException,
               IllegalArgumentException,
               NullPointerException
    {
        // Per the numpy roll semantics, no kwargs means a flat roll
        if (kwargs == null || kwargs.isEmpty()) {
            return rollFlat(shift);
        }

        // Once we get here we expect the axis kwarg to be set, since it's the
        // only one which is accepted. If it's not then there is a logic error
        // somewhere.
        final Object axisObj = kwargs.get("axis");
        if (axisObj == null) {
            throw new IllegalArgumentException("Missing 'axis' kwarg");
        }

        // Axis needs to be an integer to hand off
        if (axisObj instanceof Number) {
            Number axis = (Number)axisObj;
            return roll(new long[]{shift}, Map.of("axis", new int[]{axis.intValue()}));
        }
        // Otherwise throw an error
        else {
            throw new IllegalArgumentException(
                "Invalid axis argument; axis should be an integer " +
                "but had " + axisObj
            );
        }
    }

    /**
     * Roll the cube along the {@code n}th axis, such that sub-cubes which are
     * pushed off one end appear on the other, e.g: <pre>
     *
     *    [[1, 2, 3],        [[3, 1, 2],
     *     [4, 5, 6],   ==>   [6, 4, 5],
     *     [7, 8, 9]]         [9, 7, 8]]
     *
     * </pre>
     *
     * The result of this call will be a view on the original cube, similar to
     * performing a slice operation.
     *
     * <p>This is equivalent to the {@code numpy.roll()} method.
     *
     * @param shifts  The (signed) number of elements to this cube roll by in
     *                each axis.
     * @param kwargs  <ul>
     *                  <li>{@code axis=[0,1,...]} -- The axes to roll.</li>
     *                </ul>
     *
     * @throws IndexOutOfBoundsException If any of the given axes did not exist.
     *
     * @throws IllegalArgumentException  If there was any other problem with the
     *                                   arguments.
     */
    @Kwargs(value="axis")
    @GenericReturnType
    public default Hypercube<T> roll(final long[]             shifts,
                                     final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               IndexOutOfBoundsException
    {
        if (shifts == null) {
            throw new NullPointerException("Given a null shifts array");
        }

        Object axisObj = (kwargs != null) ? kwargs.get("axis") : null;
        if (axisObj == null) {
            // If we have no axis argument then it (per numpy semantics)
            // devolves to the flat rolling behaviour, for each shift value in
            // the array. This is equivalent to just summing up the shift values
            // and doing it in one operation. We early-out by doing that.
            long shift = 0;
            for (long s : shifts) {
                shift += s;
            }
            return rollFlat(shift);
        }

        // Check if axis is a list which we can turn into an array of integers
        try {
            axisObj = KwargUtil.toIntArray(axisObj);
        }
        catch (IllegalArgumentException e) {
            // Throw an error otherwise
            throw new IllegalArgumentException(
                "Invalid axis argument; needed an array of integers " +
                "but had " + axisObj,
                e
            );
        }

        // Now handle the case where axis is an integer array
        if (axisObj instanceof int[]) {
            final Roll<?>[] rolls = new Roll[getNDim()];
            final int[]     axis  = (int[])axisObj;

            if (axis.length != shifts.length) {
                throw new IllegalArgumentException(
                    "Number of axes did not match the number of shift values; " +
                    "had " + axis  .length + " axis values " +
                    "but " + shifts.length + " length values"
                );
            }

            for (int i=0; i < axis.length; i++) {
                if (axis[i] < 0 || axis[i] >= getNDim()) {
                    throw new IndexOutOfBoundsException(
                        "Given axis #" + axis[i] + " " +
                        "does not exist in the cube with " + getNDim() + " " +
                        "dimensions"
                    );
                }
                if (rolls[axis[i]] != null) {
                    throw new IllegalArgumentException(
                        "Axis " + axis[i] + " given more than once in " +
                        Arrays.toString(axis)
                    );
                }
                rolls[axis[i]] = new Roll<>(dim(axis[i]), shifts[i]);
            }

            return roll(rolls);
        }

        // Invalid axis value
        else {
            throw new IllegalArgumentException(
                "Invalid axis argument; axis should be an array of integers " +
                "but had " + axisObj
            );
        }
    }

    /**
     * Create a new hypercube which is view on this hypercube, reshaped as per
     * the given {@link Dimension}s.
     *
     * @param shape  The new shape of the hypercube
     *
     * @throws DimensionalityException  If the dimensions are not compatible
     *                                  with this {@link Hypercube}'s dimensions.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public default Hypercube<T> reshape(final long[] shape)
    {
        return reshape(Dimension.of(shape));
    }

    /**
     * Create a new hypercube which is view on this hypercube, reshaped as per
     * the given {@link Dimension}s.
     *
     * @param dimensions  The dimensions of the hypercube to be returned.
     *
     * @throws DimensionalityException  If the dimensions are not compatible
     *                                  with this {@link Hypercube}'s dimensions.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public default Hypercube<T> reshape(final Dimension<?>[] dimensions)
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
        return new WrappingHypercube<>(dimensions, this);
    }

    /**
     * Returns a view of this hypercube with axes transposed.
     */
    @GenericReturnType
    public default Hypercube<T> transpose()
    {
        return new TransposedHypercube<>(this);
    }

    /**
     * Set the contents of this hypercube with the given one.
     *
     * @throws DimensionalityException If the source cube had different
     *                                 dimensions.
     * @throws NullPointerException    If the given cube was {@code null}.
     */
    public void set(final Hypercube<T> that)
        throws DimensionalityException,
               NullPointerException;

    /**
     * Fill this hypercube with the given value.
     */
    public default void fill(final T v)
    {
        for (long i=0, size = getSize(); i < size; i++) {
            setObjectAt(i, v);
        }
    }

    /**
     * Clear out the contents of this hypercube.
     */
    public default void clear()
    {
        fill(null);
    }

    /**
     * Flush any cached data.
     */
    public default void flush()
        throws IOException
    {
        // By default this does nothing
    }

    /**
     * Get a flattened view of the elements of this hypercube. These are
     * flattened "C-style"; this is the same as {@code numpy}'s default.
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
    public default void toFlattenedObjs(final long srcPos,
                                        final T[]  dst,
                                        final int  dstPos,
                                        final int  length)
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
            dst[i + dstPos] = getObjectAt(i + srcPos);
        }
    }

    /**
     * Get a flattened view of the elements of this hypercube. These are
     * flattened "C-style"; this is the same as {@code numpy}'s default.
     *
     * @param destination Where to put the results.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default void toFlattenedObjs(final T[] destination)
        throws UnsupportedOperationException
    {
        if (getSize() > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }
        toFlattenedObjs(0L, destination, 0, (int)getSize());
    }

    /**
     * Get a flattened view of the elements of this hypercube as new array
     * instance. These are flattened "C-style"; this is the same as
     * {@code numpy}'s default.
     *
     * <p>Subtypes will specialise the return type since it could be an array of
     * Objects or an array of primitives.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    @GenericReturnType
    public default Object flatten()
        throws UnsupportedOperationException
    {
        // By default we return the object array
        return toObjectArray();
    }

    /**
     * Get a flattened view of the elements of this hypercube as new array
     * instance. These are flattened "C-style"; this is the same as
     * {@code numpy}'s default.
     *
     * @throws UnsupportedOperationException If the cube is larger than
     *                                       {@link Integer#MAX_VALUE} in size,
     *                                       since this cannot be represented in
     *                                       an array.
     */
    public default T[] toObjectArray()
        throws UnsupportedOperationException
    {
        if (getSize() > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "Hypercube to large to flatten, it has " + getSize() + " " +
                "elements which cannot be represented as an int"
            );
        }
        @SuppressWarnings("unchecked")
        final T[] destination =
            (T[])Array.newInstance(getElementType(), (int)getSize());
        toFlattenedObjs(destination);
        return destination;
    }

    /**
     * Write out a flattened view of all the elements of this hypercube, in
     * big-endian format. These are flattened "C-style"; this is the same as
     * {@code numpy}'s default.
     *
     * @param os      Where to write the results to.
     *
     * @throws IOException                   If a write error occurred.
     * @throws NullPointerException          If the given output stream was null.
     * @throws UnsupportedOperationException If unsupported for the elements' type.
     */
    public default void toFlattened(final DataOutputStream os)
        throws IOException,
               IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        toFlattened(0, getSize(), os, ByteOrder.BIG_ENDIAN);
    }

    /**
     * Write out a flattened view of the elements of this hypercube. These are
     * flattened "C-style"; this is the same as {@code numpy}'s default.
     *
     * @param srcPos  The "flattened" position to start copying values from,
     *                within the hypercube.
     * @param length  How many values to copy.
     * @param os      Where to write the results to.
     * @param bo      The byte ordering to use.
     *
     * @throws IOException                   If a write error occurred.
     * @throws IllegalArgumentException      If {@code length} was negative.
     * @throws IndexOutOfBoundsException     If {@code srcPos} was not within the
     *                                       bounds of the hypercube, or if
     *                                       {@code srcPos + length} was greater
     *                                       than the size of the hypercube.
     * @throws NullPointerException          If the given output stream was null.
     * @throws UnsupportedOperationException If unsupported for the elements' type.
     */
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
                "Flattening with srcPos=" + srcPos + "length=" + length
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

        // Do the write
        for (long i = srcPos, end = srcPos + length; i < end; i++) {
            writeElement(getObjectAt(i), os, bo);
        }
        os.flush();
    }

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @param src     The data to unflatten from.
     * @param srcPos  The position in {@code src} to start copying values from.
     * @param dstPos  The position at which, in the flattened view of the
     *                hypercube, to start copying values into.
     * @param length  How many values to copy.
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
    public default void fromFlattenedObjs(final T[]  src,
                                          final int  srcPos,
                                          final long dstPos,
                                          final int  length)
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
            setObjectAt(i + dstPos, src[i + srcPos]);
        }
    }

    /**
     * Set the contents of this hypercube with the given array. This is expected
     * to contain values in the order per the semantics of flatten.
     *
     * @param array The data to unflatten from.
     *
     * @throws IllegalArgumentException If the given array's length did not match
     *                                  this hypercube's size.
     * @throws NullPointerException     If the given array was {@code null}.
     */
    public default void fromFlattenedObjs(final T[] array)
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
        fromFlattenedObjs(array, 0, 0L, array.length);
    }

    /**
     * Read in a flattened view of the all elements of this hypercube. This is
     * expected to contain values in the order per the semantics of flatten.
     *
     * @param is Where to read the results from.
     *
     * @throws UnsupportedOperationException If unsupported for the elements' type.
     */
    public default void fromFlattened(final DataInputStream is)
        throws IOException,
               IllegalArgumentException,
               IndexOutOfBoundsException
    {
        fromFlattened(0, getSize(), is, ByteOrder.BIG_ENDIAN);
    }

    /**
     * Read in a flattened view of the elements of this hypercube. This is
     * expected to contain values in the order per the semantics of flatten.
     *
     * @param dstPos  The position at which, in the flattened view of the
     *                hypercube, to start copying values into.
     * @param length  How many values to copy.
     * @param is      Where to read the results from.
     * @param bo      The byte ordering to use.
     *
     * @throws IllegalArgumentException      If {@code length} was negative.
     * @throws IndexOutOfBoundsException     If {@code dstPos} was not within the
     *                                       bounds of the hypercube.
     *                                       If {@code dstPos + length} was greater
     *                                       than the size of the hypercube.
     * @throws NullPointerException          If {@code src} was {@code null}.
     * @throws UnsupportedOperationException If unsupported for the elements' type.
     */
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
                "Unflattening with dstPos=" + dstPos + " length=" + length
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

        // Safe to set, do it the slow way by default
        for (long i = dstPos, end = dstPos + length; i < end; i++) {
            setObjectAt(i, readElement(is, bo));
        }
    }

    /**
     * Copy the contents of given cube into this one.
     *
     * @throws IllegalArgumentException if the given cube was not compatible for
     *                                  some reason.
     */
    public default void copyFrom(final Hypercube<T> that)
    {
        if (that == null) {
            throw new IllegalArgumentException("Given a null cube to copy from");
        }
        if (!matches(that)) {
            throw new IllegalArgumentException("Given cube is not compatible");
        }
        final long size = getSize();
        for (long i=0; i < size; i++) {
            setObjectAt(i, that.getObjectAt(i));
        }
    }

    /**
     * Get the object instance of the element at the given raw indices. This
     * method provides no dimensionality checking.
     *
     * @param indices The indices of the element in the hypercube.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public T getObj(final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Get the object instance of the element at the given {@link Coordinate}s.
     *
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default T getObj(final Coordinate<?>... coordinates)
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
        return getObj(indices);
    }

    /**
     * Get the element at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public T getObjectAt(final long index)
        throws IndexOutOfBoundsException;

    /**
     * Set the object at the given indices. This method provides no
     * dimensionality checking.
     *
     * @param obj     The object to set.
     * @param indices The indices of the element in the hypercube.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public void setObj(final T obj, final long... indices)
        throws IndexOutOfBoundsException;

    /**
     * Set the object at the given coordinates.
     *
     * @param obj         The object to set with.
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     */
    public default void setObj(final T obj, final Coordinate<?>... coordinates)
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
        setObj(obj, indices);
    }

    /**
     * Set the object at the given indices with the given object, if we can.
     * This method provides no dimensionality checking.
     *
     * @param obj     The object to set.
     * @param indices The indices of the element in the hypercube.
     *
     * @throws ClassCastException        If the given object can't be used.
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    @SuppressWarnings("unchecked")
    public default void setFromObject(final Object obj, final long... indices)
        throws ClassCastException,
               IndexOutOfBoundsException
    {
        // Simple hand-off for the default implementation
        setObj((T)obj, indices);
    }

    /**
     * Set the object at the given coordinates with the given one, if we can.
     *
     * @param obj         The object to set with.
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws ClassCastException        If the given object can't be used.
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    @SuppressWarnings("unchecked")
    public default void setFromObject(final Object obj, final Coordinate<?>... coordinates)
        throws ClassCastException,
               IndexOutOfBoundsException
    {
        // Simple hand-off for the default implementation
        setObj((T)obj, coordinates);
    }

    /**
     * Set the element at the given index in the flattened representation.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public void setObjectAt(final long index, final T object)
        throws IndexOutOfBoundsException;

    /**
     * Set the element at the given index with the given object, if we can, in
     * the flattened representation.
     *
     * @throws ClassCastException        If the given object can't be used.
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    @SuppressWarnings("unchecked")
    public default void setFromObjectAt(final long index, final Object object)
        throws ClassCastException,
               IndexOutOfBoundsException
    {
        // Simple hand-off for the default implementation
        setObjectAt(index, (T)object);
    }

    /**
     * See if an element is set at the given indices.
     *
     * <p>For {@link Hypercube}s holding Java {@link Object}s this is
     * the same as whether the object is {@code null}.
     */
    public default boolean isSet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getObj(indices) != null;
    }

    /**
     * See if an element is set at the given coordinates.
     *
     * <p>For {@link Hypercube}s holding Java {@link Object}s this is
     * the same as whether the object is {@code null}.
     */
    public default boolean isSet(final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        return getObj(coordinates) != null;
    }

    /**
     * See if an element is set at the given index in the flattened
     * representation.
     *
     * <p>For {@link Hypercube}s holding Java {@link Object}s this is
     * the same as whether the object is {@code null}.
     */
    public default boolean isSetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getObjectAt(index) != null;
    }

    /**
     * Write out an element from this hypercube, in big-endian format.
     *
     * <p>Optional operation.
     *
     * @param el  The element to write out.
     * @param os  Where to write the element to.
     *
     * @throws IOException                   If a write error occurred.
     * @throws UnsupportedOperationException If unsupported for the element's type.
     */
    public default void writeElement(final T                el,
                                     final DataOutputStream os)
        throws IOException
    {
        writeElement(el, os, ByteOrder.BIG_ENDIAN);
    }

    /**
     * Write out an element from this hypercube, in big-endian format.
     *
     * <p>Optional operation.
     *
     * @param el  The element to write out.
     * @param os  Where to write the element to.
     * @param bo  The byte ordering to use.
     *
     * @throws IOException                   If a write error occurred.
     * @throws UnsupportedOperationException If unsupported for the element's type.
     */
    public void writeElement(final T                el,
                             final DataOutputStream os,
                             final ByteOrder        bo)
        throws IOException;

    /**
     * Read in an element from this hypercube, in big-endian format.
     *
     * <p>Optional operation.
     *
     * @param os  Where to read the element from.
     *
     * @throws IOException                   If a write error occurred.
     * @throws UnsupportedOperationException If unsupported for the element's type.
     */
    public default T readElement(final DataInputStream os)
        throws IOException
    {
        return readElement(os, ByteOrder.BIG_ENDIAN);
    }

    /**
     * Read in an element from this hypercube, in big-endian format.
     *
     * <p>Optional operation.
     *
     * @param os  Where to read the element from.
     * @param bo  The byte ordering to use.
     *
     * @throws IOException                   If a write error occurred.
     * @throws UnsupportedOperationException If unsupported for the element's type.
     */
    public T readElement(final DataInputStream os,
                         final ByteOrder       bo)
        throws IOException;

    /**
     * Get the absolute offset in the flattened view of the cube for the given
     * indices.
     *
     * @throws IndexOutOfBoundsException  If the indices were not in the bounds
     *                                    in any way.
     * @throws NullPointerException       If the indices were {@code null}.
     */
    public default long toOffset(final long... indices)
        throws IndexOutOfBoundsException,
               NullPointerException
    {
        // This code is duplicated in AbstractHypercube

        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != getNDim()) {
            throw new IndexOutOfBoundsException(
                "Given the wrong number of indices; " +
                "expected " + getNDim() + " " +
                "but had " + indices.length
            );
        }

        // Handle the simple lower-dimensional cases first, if we can,
        // and early out.
        switch (indices.length) {
        case 1:
            // This is simple -- it's just the first (and only) index
            boundsCheck(indices[0], 0);
            return indices[0];

        case 2:
            // Also fairly simple
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            return ((long)
                    indices[0] * length(1) +
                    indices[1]);
        case 3:
            // Getting more complicated now
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            return (((long)
                     indices[0]  * length(1) +
                     indices[1]) * length(2) +
                     indices[2]);

        case 4:
            // Hmm, this is getting hairier
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            boundsCheck(indices[3], 3);
            return ((((long)
                      indices[0]  * length(1) +
                      indices[1]) * length(2) +
                      indices[2]) * length(3) +
                      indices[3]);

        case 5:
            // Okay, this is probably enough
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            boundsCheck(indices[3], 3);
            boundsCheck(indices[4], 4);
            return (((((long)
                       indices[0]  * length(1) +
                       indices[1]) * length(2) +
                       indices[2]) * length(3) +
                       indices[3]) * length(4) +
                       indices[4]);
        }

        // Okay, we have a higher-dimensional cube; do something more
        // programmatic to get the offset.
        //
        // The multiplier for the offsets, we'll wind this back as we walk the
        // indices
        long mult = 1;
        for (int i = 1; i < getNDim(); i++) {
            mult *= length(i);
        }

        // Determine the index in the flattened array
        long offset = 0;
        for (int i=0; i < indices.length; i++) {
            // Handles
            final long length = length(i);
            long idx = indices[i];

            // Indexing from the end?
            if (idx < 0) {
                idx = length - idx;
            }

            // Bounds check
            boundsCheck(idx, i);

            // Okay to add in now
            offset += idx * mult;

            // Wind back the multiplier ready for the next dimension
            if (i < getNDim()-1) {
                mult /= length(i+1);
            }
        }

        // And give it back
        return offset;
    }

    /**
     * Copy the elements of this cube and return a new one.
     *
     * @return The new copied cube
     *
     * @throws UnsupportedOperationException if the cube cannot be duplicated
     *                                       for some reason.
     */
    public default Hypercube<T> copy()
        throws UnsupportedOperationException
    {
        return CubeMath.copy(this);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This has Pythonic semantics in that the iterator is only on the first
     * dimension. This might mean we iterate over values or sub-cubes.
     */
    @Override
    public default Iterator<Object> iterator()
    {
        return new Iterator<Object>() {
            /**
             * Current index.
             */
            private int myIndex = 0;

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean hasNext()
            {
                return myIndex < __len__();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Object next()
                throws NoSuchElementException
            {
                if (myIndex < __len__()) {
                    if (getNDim() == 1) {
                        return getObjectAt(myIndex++);
                    }
                    else {
                        return slice(dim(0).at(myIndex++));
                    }
                }
                else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    /**
     * The iterator over the flattened instance.
     */
    public default Iterator<T> flat()
    {
        return new Iterator<T>() {
            /**
             * Current index.
             */
            private long myIndex = 0;

            /**
             * Ending index, exclusive.
             */
            private long myEnd = getSize();

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean hasNext()
            {
                return myIndex < myEnd;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public T next()
                throws NoSuchElementException
            {
                if (myIndex < myEnd) {
                    return getObjectAt(myIndex++);
                }
                else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    /**
     * Extract the items of this cube using the given boolean cube as a
     * selector. Unless this is a masking, this will always return a flattened
     * cube, per numpy semantics.
     */
    @GenericReturnType
    public default Hypercube<T> mask(final Hypercube<Boolean> mask)
    {
        // Make sure it's compatible
        if (matchesInShape(mask)) {
            return CubeMath.extract(mask, this);
        }
        else if (mask.getNDim() == 1 &&
                 mask.length(0) == length(0))
        {
            // A simple masking. Copy out the mask from the given cube and hand
            // off.
            final BooleanHypercube bhc =
                BooleanHypercube.asBooleanHypercube(mask);
            if (bhc != null) {
                final LongBitSet bs = new LongBitSet(mask.getSize());
                bhc.toFlattened(0, bs, 0, mask.getSize());
                return mask(bs);
            }
        }

        // Could not convert
        throw new IllegalArgumentException(
            "Given an incompatible mask; " +
            "this cube has shape " + Arrays.toString(     getShape()) + ", " +
            "mask has shape "      + Arrays.toString(mask.getShape()) + "; " +
            "or mask could not be converted to a BooleanHypercube"
        );
    }

    /**
     * Extract the items of this cube using the given boolean array as a mask.
     */
    @GenericReturnType
    public default Hypercube<T> mask(final boolean[] mask)
    {
        // Make sure it's compatible
        if (mask == null) {
            throw new NullPointerException("Given a null mask");
        }
        else if (mask.length == length(0)) {
            return new MaskedHypercube<T>(this, mask);
        }
        else {
            throw new IllegalArgumentException(
                "Given mask length, " + mask.length + " " +
                "did not match first dimension length, " + length(0)
            );
        }
    }

    /**
     * Extract the items of this cube using the given bitset as a mask.
     */
    @GenericReturnType
    public default Hypercube<T> mask(final LongBitSet mask)
    {
        // Make sure it's compatible
        if (mask == null) {
            throw new NullPointerException("Given a null mask");
        }
        else {
            return new MaskedHypercube<T>(this, mask);
        }
    }

    /**
     * Assign from an arbitrary object.
     *
     * <p>Attempt to set the values in this cube from the given object, whatever
     * it happens to be. This is done on a best-effort basis and may only
     * partially succeed.
     *
     * @throws IllegalArgumentException if assigning from the given object is
     * not possible.
     */
    @SuppressWarnings("unchecked")
    public default void assignFrom(final Object object)
        throws IllegalArgumentException
    {
        // Say what we're doing before we do it
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Attempting to assign from " + object);
        }

        // Can't do anything with a null
        if (object == null) {
            throw new IllegalArgumentException("Can't assign from null");
        }

        // This is handy in the below, cache it
        final long size = getSize();

        // Try what we know
        if (object instanceof Hypercube) {
            // The object is a hypercube so this is easy, if slow
            final Hypercube<T> that = (Hypercube<T>)object;
            if (!Arrays.equals(that.getShape(), getShape())) {
                throw new IllegalArgumentException(
                    "Cannot assign from given array of shape " +
                    Arrays.toString(that.getShape()) + " to array of shape " +
                    Arrays.toString(this.getShape())
                );
            }

            // Do this using flattening. The buffer is small so we just directly
            // allocate it.
            final long bufsz = 1024;
            final T[] buffer =
                (T[])Array.newInstance(getElementType(), (int)bufsz);
            for (long i = 0; i < size; i += bufsz) {
                final int length = (int)Math.min(bufsz, size - i);
                that.  toFlattenedObjs(i, buffer, 0, length);
                this.fromFlattenedObjs(buffer, 0, i, length);
            }
        }
        else if (object.getClass().isArray()) {
            // We have an array. If it is 1D and of the right size then assume
            // it's a flattened one, if it's multi-dimensional then it needs to
            // be of the same shape.
            if (!object.getClass().getComponentType().isArray() &&
                Array.getLength(object) == size)
            {
                // 1D array, attempt to directly unflatten
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Given array looks like a flattened one");
                }
                try {
                    try {
                        if (LOG.isLoggable(Level.FINEST)) {
                            LOG.finest(
                                "Attempting to unflatten from " + object
                            );
                        }
                        fromFlattenedObjs((T[])object);
                    }
                    catch (ClassCastException e) {
                        if (LOG.isLoggable(Level.FINEST)) {
                            LOG.finest(
                                "Falling back to direct assignment " +
                                "because: " + e
                            );
                        }
                        for (int idx = 0; idx < size; idx++) {
                            setFromObjectAt(idx, (T)Array.get(object, idx));
                        }
                    }
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
                final long[] indices = new long[getNDim()-1];
                for (long i = 0; i < size; i += length(getNDim()-1)) {
                    // Compute the indices into the source array. We won't need
                    // the last one, which should always be zero.
                    long index = i / length(getNDim()-1);
                    for (int d = getNDim()-2; d >= 0; d--) {
                        indices[d] = (int)(index % length(d));
                        index      =       index / length(d);
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
                            array = Array.get(array, (int)indices[d]);
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

                    // And "object" should now be the right array to copy over
                    try {
                        // Check the length is right and copy over. Any
                        // failures, like casting, will be caught and treated as
                        // fatal.
                        final int dim = getNDim()-1;
                        final int arrayLength = Array.getLength(array);
                        if (arrayLength != length(dim)) {
                            throw new IllegalArgumentException(
                                "Array at dimension " + dim + " had length " +
                                arrayLength + " which does not match " +
                                "cube dimension length " + length(dim)
                            );
                        }

                        // Attempt to directly unflatten but fall back to
                        // walking assignment
                        try {
                            if (LOG.isLoggable(Level.FINEST)) {
                                LOG.finest(
                                    "Attempting to unflatten from " + array
                                );
                            }
                            fromFlattenedObjs((T[])array, 0, i, arrayLength);
                        }
                        catch (ClassCastException e) {
                            if (LOG.isLoggable(Level.FINEST)) {
                                LOG.finest(
                                    "Falling back to direct assignment " +
                                    "because: " + e
                                );
                            }
                            for (int idx = 0; idx < arrayLength; idx++) {
                                setObjectAt(i + idx, (T)Array.get(array, idx));
                            }
                        }
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
        else if (getElementType().isAssignableFrom(object.getClass())) {
            // A single element which we want to completely fill the cube with
            @SuppressWarnings("unchecked")
            final T value = (T)object;
            for (long i=0; i < getSize(); i++) {
                setObjectAt(i, value);
            }
        }
        else {
            // I just can't handle this...
            throw new IllegalArgumentException(
                "Can't assign from array " + object.getClass()
            );
        }
    }

    /**
     * Save this hypercube as a {@code npy} file to the given stream, if
     * possible. This can then be read from Python with {@code numpy.load()}.
     */
    public default void save(final DataOutputStream os)
        throws IOException,
               UnsupportedOperationException
    {
        // This only works if we have a dtype since we need its info
        final DType dtype = getDType();
        if (dtype == null) {
            throw new UnsupportedOperationException(
                "Can't write out a hypercube with an unknown dtype"
            );
        }

        // Write the magic header and remember how big it was
        int size = os.size();
        os.write(0x93); os.writeBytes("NUMPY"); // magic word
        os.write(0x1); // major version
        os.write(0x0); // minor version
        size = os.size() - size + 2; // +2: desc length

        // Details, these are padded to align on a 64byte boundary along with
        // the header
        final StringBuilder sb = new StringBuilder();
        sb.append('{')
          .append("'descr': '"       ).append(dtype.str).append("', ")
          .append("'fortran_order': ").append("False"  ).append(", ")
          .append("'shape': ("       );
        for (int i=0; i < getNDim(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(length(i));
        }
        sb.append("), }");
        while (((sb.length() + size + 1) & 0x40) != 0) {
            sb.append(' ');
        }
        sb.append('\n');

        // And write out the details, with their length as a little-endian short
        os.write((sb.length()     ) & 0xff);
        os.write((sb.length() >> 8) & 0xff);
        os.writeBytes(sb.toString());

        // Now we just put in all the data. Make sure that we get the byte order
        // correct.
        toFlattened(0,
                    getSize(),
                    os,
                    dtype.bigEndian() ? ByteOrder.BIG_ENDIAN
                                      : ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Save this hypercube as a {@code npy} file, if possible. This can then be
     * read from Python with {@code numpy.load()}.
     */
    public default void save(final String filename)
        throws IOException,
               UnsupportedOperationException
    {
        try (
            DataOutputStream os =
                new DataOutputStream(
                    new BufferedOutputStream(
                        new FileOutputStream(
                            filename
                         )
                    )
                )
        ) {
            save(os);
        }
    }

    /**
     * Give back a {@link String} which describes this hypercube.
     */
    public default String toDescriptiveString()
    {
        final StringBuilder sb =
            new StringBuilder(getClass().getSimpleName().toString());
        sb.append('<').append(getElementType().getSimpleName()).append(">[");
        for (int i=0; i < getNDim(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(dim(i));
        }
        sb.append(']');
        return sb.toString();
    }


    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    // Python data-model methods

    // We annotate with GenericReturnType since we want a the lowest type on the
    // Python side, since these are the most fully-featured.
    //
    // Don't overload the __getitem__ and __setitem__ methods since it can lead
    // to annoying misbinding of arguments, since they take an Object[] which is
    // very general. Instead you should handle the binding internally which is,
    // ironically, very Pythonic.

    /**
     * The {@code __getitem__()} version which takes a single key argument.
     */
    @GenericReturnType
    public default Object __getitem__(final Object key)
    {
        return __getitem__(new Object[] { key });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public default Object __getitem__(final Object[] key)
    {
        // Check args before we use them
        if (key == null) {
            throw new NullPointerException("Given a null key");
        }

        // See if this is a masking
        if (key.length == 1 && key[0] instanceof boolean[]) {
            return mask((boolean[])key[0]);
        }
        if (key.length == 1 && key[0] instanceof Object[]) {
            final Object[] objects = (Object[])key[0];
            if (objects.length > 0 && objects[0] instanceof Boolean) {
                final boolean[] mask = new boolean[objects.length];
                for (int i=0; i < objects.length; i++) {
                    mask[i] = Boolean.TRUE.equals(objects[i]);
                }
                return mask(mask);
            }
        }
        if (key.length == 1 &&
            key[0] instanceof Hypercube &&
            ((Hypercube)key[0]).getElementType().equals(Boolean.class))
        {
            @SuppressWarnings("unchecked")
            final Hypercube<Boolean> mask = (Hypercube<Boolean>)(Hypercube)key[0];
            return mask(mask);
        }

        // Looks like a selection.

        // Make sure it's compatible
        if (key.length > getNDim()) {
            throw new ArrayIndexOutOfBoundsException(
                "Too many indices for array; " +
                "array is " + getNDim() + "-dimensional, " +
                "but " + key.length + " were indexed"
            );
        }

        // Turn the key into a direct lookup or a slice definition
        boolean isSlice = key.length < getNDim();
        if (!isSlice) {
            for (Object k : key) {
                if (k instanceof PythonSlice) {
                    isSlice = true;
                    break;
                }
            }
        }

        // Now use whatever mechanism and hand back the desired value
        return isSlice ? slice (subscriptToAccessors(key))
                       : getObj(subscriptToIndices  (key));
    }

    /**
     * The {@code __setitem__()} version which takes a single key argument.
     */
    public default void __setitem__(final Object key, final Object value)
        throws IllegalArgumentException
    {
        __setitem__(new Object[] { key }, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default void __setitem__(final Object[] key, final Object value)
        throws IllegalArgumentException
    {
        // Make sure it's compatible
        if (key == null) {
            throw new NullPointerException("Given a null key");
        }

        // What we are attempting
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Setting at " + Arrays.toString(key) + " with " + value
            );
        }

        // See if this is a masking
        if (key.length == 1 && key[0] instanceof boolean[]) {
            mask((boolean[])key[0]).assignFrom(value);
            return;
        }
        if (key.length == 1 && key[0] instanceof Object[]) {
            final Object[] objects = (Object[])key[0];
            if (objects.length > 0 && objects[0] instanceof Boolean) {
                final boolean[] mask = new boolean[objects.length];
                for (int i=0; i < objects.length; i++) {
                    mask[i] = Boolean.TRUE.equals(objects[i]);
                }
                mask(mask).assignFrom(value);
                return;
            }
        }
        if (key.length == 1 &&
            key[0] instanceof Hypercube &&
            ((Hypercube)key[0]).getElementType().equals(Boolean.class))
        {
            @SuppressWarnings("unchecked")
            final Hypercube<Boolean> mask = (Hypercube<Boolean>)(Hypercube)key[0];
            mask(mask).assignFrom(value);
            return;
        }

        // Looks like a selection.

        // Check the key looks good
        if (key.length > getNDim()) {
            throw new ArrayIndexOutOfBoundsException(
                "Too many indices for array; " +
                "array is " + getNDim() + "-dimensional, " +
                "but " + key.length + " were indexed"
            );
        }

        // Turn the key into a direct lookup or a slice definition
        boolean isSlice = key.length < getNDim();
        if (!isSlice) {
            for (Object k : key) {
                if (k instanceof PythonSlice) {
                    isSlice = true;
                    break;
                }
            }
        }

        // If it's a slice then we are setting in a different way than if it's a
        // direct access
        if (isSlice) {
            // Get the hypercube for the given slice and attempt to assign from
            // the value which we were given
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Key has some slices, using accessors");
            }
            slice(subscriptToAccessors(key)).assignFrom(value);
        }
        else {
            // Try a direct set
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Key has no slices, using indices");
            }
            try {
                setFromObject(value, subscriptToIndices(key));
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "Given value was not convertable to the required type", e
                );
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public default long __len__()
    {
        // By Python semantics this is the length of the first dimension
        return length(0);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Negate the elements of this cube, returning the result in a new instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __neg__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.negative(this);
    }

    /**
     * Take the absolute value of the elements of this cube, returning the
     * result in a new instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __abs__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.abs(this);
    }

    /**
     * Round the elements of this cube to the nearest integer, returning the
     * result in a new instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __round__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.round(this);
    }

    /**
     * Round the elements of this cube down to the nearest integer, returning
     * the result in a new instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __floor__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.floor(this);
    }

    /**
     * Round the elements of this cube up to the nearest integer, returning
     * the result in a new instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __ceil__()
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.ceil(this);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Pairwise AND this cube to the elements of the given one and return the
     * result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __and__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.and(this, that);
    }

    /**
     * Pairwise OR this cube to the elements of the given one and return the
     * result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __or__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.or(this, that);
    }

    /**
     * Pairwise xor this cube to the elements of the given one and return the
     * result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __xor__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.xor(this, that);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Pairwise add this cube to the elements of the given one and return the
     * result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __add__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.add(this, that);
    }

    /**
     * Pairwise subtract the given cube from this one and return the result as a
     * newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __sub__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.subtract(this, that);
    }

    /**
     * Pairwise multiply this cube by the elements of the given one and return
     * the result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __mul__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.multiply(this, that);
    }

    /**
     * Perform a matrix multiple with this cube and the given one and return the
     * result as a newly-created instance or a scalar value, depending on the
     * shape of the given cubes.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Object __matmul__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return (getNDim() == 1 && that != null && that.getNDim() == 1)
            ? CubeMath.dotprod(this, that)
            : CubeMath.matmul (this, that);
    }

    /**
     * Pairwise divide this cube by the elements of the given one and return the
     * result as a newly-created instance.
     *
     * <p>This attempts a "true" divide which may result in fractional values
     * and so cannot be performed on integer types.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __truediv__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Doing a divide on an integer type will currently yield another
        // integer type, i.e. it will be a __floordiv__. This won't catch
        // subclasses doing __truediv__ outside of these understood types, but
        // we do our best.
        final Class<T> klass = getElementType();
        if (Byte   .class.equals(klass) ||
            Short  .class.equals(klass) ||
            Integer.class.equals(klass) ||
            Long   .class.equals(klass))
        {
            // It would be preferable to cast these to doubles and handle it,
            // but we don't yet do that and it makes the typing of this function
            // tricky
            throw new UnsupportedOperationException(
                "Casting " + klass.getSimpleName() + " " +
                "to a floating point type is not currently supported"
            );
        }

        return CubeMath.divide(this, that);
    }

    /**
     * Pairwise floored-divide this cube by the elements of the given one and
     * return the result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __floordiv__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Do the divide to get most of the way
        final Hypercube<T> result = CubeMath.divide(this, that);
        try {
            // And attempt to floor the result, in place, if we can. This will
            // natrually fail for integer types.
            return CubeMath.floor(result, result);
        }
        catch (UnsupportedOperationException e) {
            // Just give back what we had
            return result;
        }
    }

    /**
     * Compute the pairwise modulo operation on this cube by the elements of the
     * given one and return the result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __mod__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.mod(this, that);
    }

    /**
     * Pairwise raise the elements of this cube to the power of those in the
     * given one and return the result as a newly-created instance.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __pow__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.power(this, that);
    }

    /**
     * Pairwise add this cube to the elements of the given one, mutating the
     * elements of this cube in-place and returning it as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __iadd__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.add(this, that, this);
    }

    /**
     * Pairwise subtract the given cube from this one, mutating the
     * elements of this cube in-place and returning it as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __isub__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.subtract(this, that, this);
    }

    /**
     * Pairwise multiply this cube by the elements of the given one, mutating
     * the elements of this cube in-place and returning it as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __imul__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.multiply(this, that, this);
    }

    /**
     * Pairwise divide this cube by the elements of the given one, mutating the
     * elements of this cube in-place and returning it as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __itruediv__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.divide(this, that, this);
    }

    /**
     * Pairwise divide this cube by the elements of the given one, mutating the
     * elements of this cube in-place and returning it as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __ifloordiv__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        CubeMath.divide(this, that, this);
        return CubeMath.floor(this, this);
    }

    /**
     * Pairwise raise the elements of this cube to the power of those in the
     * given one, mutating the elements of this cube in-place and returning it
     * as the result.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<T> __ipow__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.power(this, that, this);
    }

    /**
     * Perform a pairwise less-than comparison operation between the elements of
     * this cube and the given one, returning the results in a newly created
     * cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __lt__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.less(this, that);
    }

    /**
     * Perform a pairwise less-than-or-equals comparison operation between the
     * elements of this cube and the given one, returning the results in a newly
     * created cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __le__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.lessEqual(this, that);
    }

    /**
     * Perform a pairwise equality comparison operation between the elements of
     * this cube and the given one, returning the results in a newly created
     * cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __eq__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.equal(this, that);
    }

    /**
     * Perform a pairwise inequality comparison operation between the elements
     * of this cube and the given one, returning the results in a newly created
     * cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __ne__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.notEqual(this, that);
    }

    /**
     * Perform a pairwise greater-than-or-equals comparison operation between
     * the elements of this cube and the given one, returning the results in a
     * newly created cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __ge__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.greaterEqual(this, that);
    }

    /**
     * Perform a pairwise greater-than comparison operation between the elements
     * of this cube and the given one, returning the results in a newly created
     * cube.
     *
     * <p>Optional operation.
     */
    @GenericReturnType
    public default Hypercube<Boolean> __gt__(final Hypercube<T> that)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return CubeMath.greater(this, that);
    }

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    /**
     * Bounds check an index in the {@code i}th dimension.
     */
    private void boundsCheck(final long idx, final int i)
        throws IndexOutOfBoundsException
    {
        // This code is duplicated in AbstractHypercube

        if (i < 0 || i >= getNDim()) {
            throw new IndexOutOfBoundsException(
                "Index #" + i + ", was out of dimensional bounds, " +
                getNDim()
            );
        }
        if (idx < 0 || idx >= length(i)) {
            throw new IndexOutOfBoundsException(
                "Index #" + i + ", with value " + idx + ", " +
                "was out of bounds in dimension " +
                dim(i) + ", length " + length(i)
            );
        }
    }

    /**
     * Take a Python subscript, like as used for {@code __getitem__()}, and turn
     * it into accessors.
     */
    private Dimension.Accessor<?>[] subscriptToAccessors(final Object[] key)
    {
        // Make sure it's compatible
        if (key == null) {
            throw new NullPointerException("Given a null key");
        }

        // What we are transforming
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Making accessors from " + Arrays.toString(key));
        }

        // Check it can be done
        if (key.length > getNDim()) {
            throw new ArrayIndexOutOfBoundsException(
                "Too many indices for array; " +
                "array is " + getNDim() + "-dimensional, " +
                "but " + key.length + " were indexed"
            );
        }

        // What we'll hand back
        final Dimension.Accessor<?>[] accessors =
            new Dimension.Accessor<?>[key.length];

        // Turn each part of the key into an accessor
        for (int i=0; i < key.length; i++) {
            final Dimension.Accessor<?> accessor;
            final Object k = key[i];
            if (LOG.isLoggable(Level.FINEST)) {
                if (k == null) {
                    LOG.finest("key[" + i + "] is null");
                }
                else {
                    LOG.finest("key[" + i + "] is a " +
                               k.getClass().getSimpleName() + " " +
                               "with value " + k);
                }
            }

            // Different types make for different accessors
            if (k instanceof Number) {
                final Number number = (Number)k;
                if (strictInt64(number)) {
                    accessor = dim(i).at(number.longValue());
                }
                else {
                    throw new IllegalArgumentException(
                        "Array index was not an int64 " + k + " in key " +
                        Arrays.toString(key)
                    );
                }
            }
            else if (k instanceof PythonSlice) {
                // We only support integer slice indices, and no stepping
                final PythonSlice s = (PythonSlice)k;
                if (s.step  != null && s.step != 1L          ||
                    s.start != null && !strictInt64(s.start) ||
                    s.stop  != null && !strictInt64(s.stop))
                {
                    throw new UnsupportedOperationException(
                        "Can't handle a slice of the form " + s
                    );
                }
                accessor = dim(i).slice(
                    (s.start == null) ? 0         : s.start.intValue(),
                    (s.stop  == null) ? length(i) : s.stop .intValue()
                );
            }
            else {
                throw new IllegalArgumentException(
                    "Don't know how to index with '" +
                    StringUtil.toString(k,   10) + "' in key '" +
                    StringUtil.toString(key, 10) + "'"
                );
            }

            // And save what we made
            accessors[i] = accessor;
        }

        // And give back the constructed accessors
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Created accessors " + Arrays.toString(accessors));
        }
        return accessors;
    }

    /**
     * Take a Python subscript, like as used for {@code __getitem__()} and turn
     * it into integer indices.
     */
    private long[] subscriptToIndices(final Object[] key)
    {
        // Make sure it's compatible
        if (key == null) {
            throw new NullPointerException("Given a null key");
        }
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Making indices from " + Arrays.toString(key));
        }

        // Ensure it can be done
        if (key.length > getNDim()) {
            throw new ArrayIndexOutOfBoundsException(
                "Too many indices for array; " +
                "array is " + getNDim() + "-dimensional, " +
                "but " + key.length + " were indexed"
            );
        }

        // This is a direct access so we just figure out the coordinates
        final long[] indices = new long[key.length];
        for (int i=0; i < key.length; i++) {
            final Object k = key[i];
            if (LOG.isLoggable(Level.FINEST)) {
                if (k == null) {
                    LOG.finest("key[" + i + "] is null");
                }
                else {
                    LOG.finest("key[" + i + "] is a " +
                               k.getClass().getSimpleName() + " " +
                               "with value " + k);
                }
            }
            if (k instanceof Number) {
                final Number number = (Number)k;
                if (strictInt64(number)) {
                    indices[i] = number.longValue();
                }
                else {
                    throw new IllegalArgumentException(
                        "Array index was not an int64 '" + k + "' in key " +
                        Arrays.toString(key)
                    );
                }
            }
            else {
                throw new IllegalArgumentException(
                    "Don't know how to index with " + k + " in key " +
                    Arrays.toString(key)
                );
            }
        }
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Created indices " + Arrays.toString(indices));
        }
        return indices;
    }

    /**
     * Ensure a {@link Number} is an int64.
     */
    private boolean strictInt64(final Number number)
    {
        if (number == null) {
            return false;
        }
        if (number instanceof Float ||
            number instanceof Double)
        {
            final double dbl = number.doubleValue();
            if (dbl != Math.round(dbl)) {
                return false;
            }
        }
        return true;
    }
}
