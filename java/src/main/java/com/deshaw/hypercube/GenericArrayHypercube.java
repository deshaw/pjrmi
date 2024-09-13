package com.deshaw.hypercube;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.lang.reflect.Array;

import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has Java {@link Object}s as its elements and stores them in
 * a plain Java array. (This limits its size to {@code Integer.MAX_VALUE}.
 *
 * @param <T> The type of the element which we store.
 */
public class GenericArrayHypercube<T>
    extends AbstractHypercube<T>
    implements GenericHypercube<T>
{
    /**
     * The array of elements which we hold.
     */
    private final T[] myElements;

    // -------------------------------------------------------------------------

    /**
     * Constructor.
     */
    @SuppressWarnings("unchecked")
    public GenericArrayHypercube(final Dimension<?>[] dimensions,
                                 final Class<T>       elementType)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, elementType);

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Unable to represent a cube of size " + size + " with an array"
            );
        }

        myElements = (T[])Array.newInstance(getElementType(), (int)size);
    }

    /**
     * Constructor.
     *
     * @param dimensions The dimensions of the hypercube.
     * @param elements   The elements to copy. Must be non-empty.
     */
    @SuppressWarnings("unchecked")
    public GenericArrayHypercube(final Dimension<?>[] dimensions,
                                 final List<T>        elements)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, (Class<T>)elements.get(0).getClass());

        int size = 1;
        for (Dimension<?> dim : dimensions) {
            size *= dim.length();
        }

        if (elements.size() != size) {
            throw new IllegalArgumentException(
                "Number of elements, " + elements.size() + ", " +
                "does not match expected size, " + size + " " +
                "for dimensions " + Arrays.toString(dimensions)
            );
        }

        myElements = (T[])elements.toArray();
    }

    /**
     * Constructor.
     *
     * <p>This will wrap the given array directly, it won't make a copy.
     *
     * @param dimensions The dimensions of the hypercube.
     * @param elements   The elements to wrap. Must be non-empty.
     */
    @SuppressWarnings("unchecked")
    public GenericArrayHypercube(final Dimension<?>[] dimensions,
                                 final T[]            elements)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, (Class<T>)elements[0].getClass());

        int size = 1;
        for (Dimension<?> dim : dimensions) {
            size *= dim.length();
        }

        if (elements.length != size) {
            throw new IllegalArgumentException(
                "Number of elements, " + elements.length + ", " +
                "does not match expected size, " + size + " " +
                "for dimensions " + Arrays.toString(dimensions)
            );
        }

        myElements = elements;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
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

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        // Copy the values over
        preRead();
        System.arraycopy(myElements, (int)srcPos,
                         dst,             dstPos,
                         length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void fromFlattenedObjs(final T[]  src,
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

        // Sanitise input
        checkUnflattenArgs(srcPos, dstPos, length);
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

        // Copy in
        System.arraycopy(src,             srcPos,
                         myElements, (int)dstPos,
                         length);
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        preRead();
        return myElements[(int)index];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final T obj)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        myElements[(int)index] = obj;
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeElement(final T                el,
                             final DataOutputStream os,
                             final ByteOrder        bo)
        throws IOException
    {
        // It's up to users to implement this
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T readElement(final DataInputStream is,
                         final ByteOrder       bo)
        throws IOException
    {
        // It's up to users to implement this
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("ALIGNED",      false);
        result.put("BEHAVED",      false);
        result.put("C_CONTIGUOUS", false);
        result.put("OWNDATA",      true);
        result.put("WRITEABLE",    true);
        return result;
    }
}
