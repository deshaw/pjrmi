package com.deshaw.hypercube;

import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The base class from which most {@link Hypercube} implementations will derive.
 *
 * @param <T> The type of the element which we store.
 */
public abstract class AbstractHypercube<T>
    implements Hypercube<T>
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transposed view of this cube.
     */
    public final Hypercube<T> T;

    /**
     * The shape of the cube, as a public member to mirror {@code numpy}'s
     * semantics.
     */
    public final List<Long> shape;

    /**
     * The number of elements in this cube.
     */
    public final long size;

    /**
     * How many dimensions this hypercube has.
     */
    public final int ndim;

    /**
     * The ndarray-like flags associated with the Hypercube.
     */
    public final Flags flags;

    // ----------------------------------------------------------------------

    /**
     * The dimensions of this hypercube.
     */
    private final Dimension<?>[] myDimensions;

    /**
     * The dimension lengths.
     */
    private final long[] myLengths;

    /**
     * The type of the elements stored in this hypercube.
     */
    private final Class<T> myElementType;

    /**
     * The memory barrier.
     */
    private volatile boolean myBarrier;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     */
    protected AbstractHypercube(final Dimension<?>[] dimensions,
                                final Class<T>       elementType)
        throws IllegalArgumentException,
               NullPointerException
    {
        this(dimensions, elementType, null);
    }

    /**
     * Constructor.
     */
    protected AbstractHypercube(final Dimension<?>[] dimensions,
                                final Class<T>       elementType,
                                final Hypercube<T>   transposed)
        throws IllegalArgumentException,
               NullPointerException
    {
        if (dimensions == null) {
            throw new NullPointerException(
                "Given a null for the dimensions"
            );
        }
        if (dimensions.length == 0) {
            throw new IllegalArgumentException(
                "Given empty dimensions"
            );
        }
        for (Dimension<?> dimension : dimensions) {
            if (dimension == null) {
                throw new IllegalArgumentException(
                    "Dimensions contained a null element: " +
                    Arrays.toString(dimensions)
                );
            }
        }
        if (elementType == null) {
            throw new NullPointerException(
                "Given a null transposed cube"
            );
        }

        myDimensions       = Arrays.copyOf(dimensions, dimensions.length);
        myLengths          = new long[dimensions.length];
        myElementType      = elementType;
        T                  = (transposed == null) ? new TransposedHypercube<>(this)
                                                  : transposed;

        for (int i=0; i < dimensions.length; i++) {
            myLengths[i] = myDimensions[i].length();
        }

        final List<Long> shape_ = new ArrayList<>();
        for (long d : getShape()) {
            shape_.add(d);
        }
        shape = Collections.unmodifiableList(shape_);
        size  = Hypercube.super.getSize();
        ndim  = myDimensions.length;
        flags = new Flags(createFlags());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dimension<?>[] getDimensions()
    {
        return Arrays.copyOf(myDimensions, myDimensions.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getElementType()
    {
        return myElementType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getBoxedShape()
    {
        return shape;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNDim()
    {
        return myDimensions.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dimension<?> dim(final int i)
        throws IndexOutOfBoundsException
    {
        return myDimensions[i];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long length(final int i)
        throws IndexOutOfBoundsException
    {
        return myLengths[i];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSize()
    {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Hypercube<T> slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        // And give back the sub-cube
        return new SlicedHypercube<T>(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Hypercube<T> rollFlat(final long shift)
    {
        // And give back the flat-rolled cube
        return new FlatRolledHypercube<T>(this, shift);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Hypercube<T> roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        // And give back the axis-rolled cube
        return new AxisRolledHypercube<T>(this, rolls);
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public Hypercube<T> transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final Hypercube<T> that)
        throws DimensionalityException,
               NullPointerException
    {
        // Make sure that it matches
        assertCompatibility(that);

        // Safe to set, do it the slow way by default
        for (long i = 0; i < size; i++) {
            setObjectAt(i, that.getObjectAt(i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObj(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getObjectAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObj(final T obj, final long... indices)
        throws IndexOutOfBoundsException
    {
        setObjectAt(toOffset(indices), obj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObj(final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        // Make sure that they match dimensionally
        if (coordinates == null) {
            throw new NullPointerException("Given null coordinates");
        }
        if (coordinates.length != ndim) {
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + ndim
            );
        }

        // Handle the common cases
        switch (coordinates.length) {
        case 1:
            dimensionCheck(coordinates[0], 0);
            return getObj(coordinates[0].get());

        case 2:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            return getObj(coordinates[0].get(),
                          coordinates[1].get());

        case 3:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            return getObj(coordinates[0].get(),
                          coordinates[1].get(),
                          coordinates[2].get());

        case 4:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            return getObj(coordinates[0].get(),
                          coordinates[1].get(),
                          coordinates[2].get(),
                          coordinates[3].get());

        case 5:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            dimensionCheck(coordinates[4], 4);
            return getObj(coordinates[0].get(),
                          coordinates[1].get(),
                          coordinates[2].get(),
                          coordinates[3].get(),
                          coordinates[4].get());
        }

        // Higher dimensions. Transform to the given indices.
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {
            final Coordinate<?> coordinate = coordinates[i];
            dimensionCheck(coordinate, i);
            indices[i] = coordinate.get();
        }

        // And hand off
        return getObj(indices);
    }

    /**
     * Set the object at the given coordinates.
     *
     * @param obj         The object to set.
     * @param coordinates The coordinates of the element in the hypercube.
     *
     * @throws DimensionalityException  If the coordinates were bad.
     * @throws MissingDataException     If the data could not be retrieved.
     */
    public void setObj(final T obj, final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        // Make sure that they match dimensionally
        if (coordinates == null) {
            throw new NullPointerException("Given null coordinates");
        }
        if (coordinates.length != ndim) {
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + ndim
            );
        }

        // Handle the common cases
        switch (coordinates.length) {
        case 1:
            dimensionCheck(coordinates[0], 0);
            setObj(obj,
                   coordinates[0].get());
            break;

        case 2:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            setObj(obj,
                   coordinates[0].get(),
                   coordinates[1].get());
            break;

        case 3:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            setObj(obj,
                   coordinates[0].get(),
                   coordinates[1].get(),
                   coordinates[2].get());
            break;

        case 4:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            setObj(obj,
                   coordinates[0].get(),
                   coordinates[1].get(),
                   coordinates[2].get(),
                   coordinates[3].get());
            break;

        case 5:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            dimensionCheck(coordinates[4], 4);
            setObj(obj,
                   coordinates[0].get(),
                   coordinates[1].get(),
                   coordinates[2].get(),
                   coordinates[3].get(),
                   coordinates[4].get());
            break;
        }

        // Higher dimensions. Transform to the given indices.
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {
            final Coordinate<?> coordinate = coordinates[i];
            dimensionCheck(coordinate, i);
            indices[i] = coordinate.get();
        }

        // And hand off
        setObj(obj, indices);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return isSetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSet(final Coordinate<?>... coordinates)
        throws DimensionalityException
    {
        // Make sure that they match dimensionally
        if (coordinates == null) {
            throw new NullPointerException("Given null coordinates");
        }
        if (coordinates.length != ndim) {
            throw new DimensionalityException(
                "Number of coordinates, " + coordinates.length + ", " +
                "did not match number of dimensions, " + ndim
            );
        }

        // Handle the common cases
        switch (coordinates.length) {
        case 1:
            dimensionCheck(coordinates[0], 0);
            return isSet(coordinates[0].get());

        case 2:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            return isSet(coordinates[0].get(),
                         coordinates[1].get());

        case 3:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            return isSet(coordinates[0].get(),
                         coordinates[1].get(),
                         coordinates[2].get());

        case 4:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            return isSet(coordinates[0].get(),
                         coordinates[1].get(),
                         coordinates[2].get(),
                         coordinates[3].get());

        case 5:
            dimensionCheck(coordinates[0], 0);
            dimensionCheck(coordinates[1], 1);
            dimensionCheck(coordinates[2], 2);
            dimensionCheck(coordinates[3], 3);
            dimensionCheck(coordinates[4], 4);
            return isSet(coordinates[0].get(),
                         coordinates[1].get(),
                         coordinates[2].get(),
                         coordinates[3].get(),
                         coordinates[4].get());
        }

        // Higher dimensions. Transform to the given indices.
        final long[] indices = new long[coordinates.length];
        for (int i=0; i < coordinates.length; i++) {
            final Coordinate<?> coordinate = coordinates[i];
            dimensionCheck(coordinate, i);
            indices[i] = coordinate.get();
        }

        // And hand off
        return isSet(indices);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long toOffset(final long... indices)
        throws IndexOutOfBoundsException
    {
        // Same code as in Hypercube but with more efficent variable access

        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != myLengths.length) {
            throw new IndexOutOfBoundsException(
                "Given the wrong number of indices; " +
                "expected " + myLengths.length + " " +
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
                    indices[0] * myLengths[1] +
                    indices[1]);
        case 3:
            // Getting more complicated now
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            return (((long)
                     indices[0]  * myLengths[1] +
                     indices[1]) * myLengths[2] +
                     indices[2]);

        case 4:
            // Hmm, this is getting hairier
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            boundsCheck(indices[3], 3);
            return ((((long)
                      indices[0]  * myLengths[1] +
                      indices[1]) * myLengths[2] +
                      indices[2]) * myLengths[3] +
                      indices[3]);

        case 5:
            // Okay, this is probably enough
            boundsCheck(indices[0], 0);
            boundsCheck(indices[1], 1);
            boundsCheck(indices[2], 2);
            boundsCheck(indices[3], 3);
            boundsCheck(indices[4], 4);
            return (((((long)
                       indices[0]  * myLengths[1] +
                       indices[1]) * myLengths[2] +
                       indices[2]) * myLengths[3] +
                       indices[3]) * myLengths[4] +
                       indices[4]);
        }

        // Okay, we have a higher-dimensional cube; do something more
        // programmatic to get the offset.
        //
        // The multiplier for the offsets, we'll wind this back as we walk the
        // indices
        long mult = 1;
        for (int i = 1; i < myLengths.length; i++) {
            mult *= myLengths[i];
        }

        // Determine the index in the flattened array
        long offset = 0;
        for (int i=0; i < indices.length; i++) {
            // Handles
            final long length = myLengths[i];
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
            if (i < myLengths.length-1) {
                mult /= myLengths[i+1];
            }
        }

        // And give it back
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")    
    public boolean equals(final Object that)
    {
        if (!(that instanceof Hypercube)) {
            return false;
        }
        if (that == this) {
            return true;
        }
        return cubeEquals((Hypercube<T>)that);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        // Hand off to the helper method, with the right starting parameters
        return toString(0, new long[ndim]);
    }

    // ----------------------------------------------------------------------

    /**
     * Compute the flags map. This method is called from the constructor and so
     * should not rely on member variables being set.
     */
    protected Map<String,Boolean> createFlags()
    {
        // Create this with some vaguely sensible defaults
        final Map<String,Boolean> result = new HashMap<>();
        result.put("aligned",      false);
        result.put("c_contiguous", false);
        result.put("f_contiguous", false);
        result.put("owndata",      false);
        result.put("writeable",    true );
        return result;
    }

    /**
     * Dimension check a coordinate in the {@code i}th dimension.
     */
    protected void dimensionCheck(final Coordinate<?> coordinate, final int i)
        throws DimensionalityException,
               IndexOutOfBoundsException
    {
        if (coordinate == null) {
            throw new NullPointerException(
                "Coordinate[" + i + "] was null"
            );
        }
        if (i < 0 || i >= myLengths.length) {
            throw new IndexOutOfBoundsException(
                "Coordinate #" + i + ", was out of dimensional bounds, " +
                myLengths.length
            );
        }
        if (!myDimensions[i].equals(coordinate.getDimension())) {
            throw new NullPointerException(
                "Coordinate[" + i + "] has dimension " +
                coordinate.getDimension() + " " +
                "but expected " + myDimensions[i]
            );
        }
    }

    /**
     * Bounds check an index in the {@code i}th dimension.
     */
    protected void boundsCheck(final long idx, final int i)
        throws IndexOutOfBoundsException
    {
        if (i < 0 || i >= myLengths.length) {
            throw new IndexOutOfBoundsException(
                "Index #" + i + ", was out of dimensional bounds, " +
                myLengths.length
            );
        }
        if (idx < 0 || idx >= myLengths[i]) {
            throw new IndexOutOfBoundsException(
                "Index #" + i + ", with value " + idx + ", " +
                "was out of bounds in dimension " +
                myDimensions[i] + ", length " + myLengths[i]
            );
        }
    }

    /**
     * Check the arguments given to a {@code toFlattened()} method.
     */
    protected void checkFlattenArgs(final long   srcPos,
                                    final Object dst,
                                    final int    dstPos,
                                    final int    length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        // Checks
        if (length < 0) {
            throw new IllegalArgumentException("Bad length: " + length);
        }
        if (dst == null) {
            throw new IllegalArgumentException("Null destination");
        }
        if (!dst.getClass().isArray()) {
            throw new IllegalArgumentException(
                "Destination was not an array: " + dst.getClass()
            );
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
        if (srcPos + length > size) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + size
            );
        }
        if (dstPos + length > Array.getLength(dst)) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the destination size, " +
                Array.getLength(dst)
            );
        }
    }

    /**
     * Check the arguments given to a {@code fromFlattened()} method.
     */
    protected void checkUnflattenArgs(final int  srcPos,
                                      final long dstPos,
                                      final int  length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        // checks
        if (length <= 0) {
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
        if (dstPos + length > size) {
            throw new IndexOutOfBoundsException(
                "Destination position, " + dstPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the size, " + size
            );
        }
    }

    /**
     * Check to make sure that the given hypercube is compatible for
     * setting this one.
     *
     * @throws DimensionalityException If the source cube had different
     *                                 dimensions.
     * @throws NullPointerException    If the source cube was {@code null}.
     */
    protected void assertCompatibility(final Hypercube<T> that)
        throws DimensionalityException,
               NullPointerException
    {
        if (that == null) {
            throw new NullPointerException("Given a null cube");
        }
        if (this.ndim != that.getNDim()) {
            throw new DimensionalityException(
                "Source had " + that.getNDim() + " dimensions " +
                "but destination had " + this.ndim + " dimensions"
            );
        }
        for (int i=0; i < ndim; i++) {
            final Dimension<?> thisDim = this.dim(i);
            final Dimension<?> thatDim = that.dim(i);
            if (!thisDim.equals(thatDim)) {
                throw new DimensionalityException(
                    "Source dimension[" + i + "] was " + thatDim + ", " +
                    "but destination dimension was " + thisDim
                );
            }
        }
        if (this.size != that.getSize()) {
            // This should never happen if the sizes are the same
            throw new RuntimeException("Dimensions match but sizes differ!");
        }
    }

    /**
     * Determine the indices from a given offset.
     */
    protected void fromOffset(long offset, final long[] indices)
        throws IndexOutOfBoundsException
    {
        if (offset > size) {
            throw new IndexOutOfBoundsException(
                "Offset " + offset + " was greater than size " + size
            );
        }
        if (indices.length != myDimensions.length) {
            throw new IndexOutOfBoundsException(
                "Given the wrong number of indices; " +
                "expected " + myDimensions.length + " " +
                "but had " + indices.length
            );
        }

        // Now just walk them back
        for (int i = indices.length - 1; i >= 0; i--) {
            final long length = myLengths[i];
            indices[i] = offset % length;
            offset    /= length;
        }
    }

    /**
     * Call before a read to ensure that the memory barrier is flushed.
     */
    protected void preRead()
    {
        final boolean dummy = myBarrier;
        if (dummy && !dummy) throw new IllegalStateException();
    }

    /**
     * Call after a write to ensure that the memory barrier is set up.
     */
    protected void postWrite()
    {
        myBarrier = !myBarrier;
    }

    /**
     * The {@link #toString()} helper method. This is designed to create
     * something which looks like the {@code numpy} printed version of an array.
     */
    protected String toString(final int    dim,
                              final long[] indices)
    {
        final StringBuilder sb = new StringBuilder();
        if (dim > 0 && indices[dim-1] > 0) {
            sb.append('\n');
            for (int i=0; i < dim; i++) {
                sb.append("  ");
            }
        }
        sb.append("[ ");
        for (int i=0; i < length(dim); i++) {
            indices[dim] = i;
            if (i > 0) {
                sb.append(", ");
            }
            if (dim == indices.length-1) {
                sb.append(getObj(indices));
            }
            else {
                sb.append(toString(dim + 1, indices));
            }
        }
        sb.append(" ]");
        return sb.toString();
    }
}
