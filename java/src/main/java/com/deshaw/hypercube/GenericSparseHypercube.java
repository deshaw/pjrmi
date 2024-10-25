package com.deshaw.hypercube;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * A hypercube which has Java {@link Object}s as its elements and stores them in
 * a {@link ConcurrentHashMap}.
 *
 * @param <T> The type of the element which we store.
 */
public class GenericSparseHypercube<T>
    extends AbstractHypercube<T>
    implements GenericHypercube<T>
{
    /**
     * The sparse of elements which we hold.
     */
    private final Map<Long,T> myElements;

    // -------------------------------------------------------------------------

    /**
     * Constructor.
     */
    @SuppressWarnings("unchecked")
    public GenericSparseHypercube(final Dimension<?>[] dimensions,
                                  final Class<T>       elementType)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, elementType);

        myElements = new ConcurrentHashMap<>();
    }

    /**
     * Constructor.
     *
     * @param dimensions The dimensions of the hypercube.
     * @param elements   The elements to copy. Must be non-empty.
     */
    @SuppressWarnings("unchecked")
    public GenericSparseHypercube(final Dimension<?>[] dimensions,
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

        myElements = new ConcurrentHashMap<>();
        for (int i=0; i < elements.size(); i++) {
            final T value = elements.get(i);
            if (value != null) {
                myElements.put(Long.valueOf(i), value);
            }
        }
    }

    /**
     * Constructor.
     *
     * @param dimensions The dimensions of the hypercube.
     * @param elements   The elements to wrap. Must be non-empty.
     */
    @SuppressWarnings("unchecked")
    public GenericSparseHypercube(final Dimension<?>[] dimensions,
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

        myElements = new ConcurrentHashMap<>();
        for (int i=0; i < elements.length; i++) {
            final T value = elements[i];
            if (value != null) {
                myElements.put(Long.valueOf(i), value);
            }
        }
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
        for (int i=0; i < length; i++) {
            dst[dstPos + i] = myElements.get(srcPos + i);
        }
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
            throw new NullPointerException("Given a null sparse");
        }
        if (src.length - srcPos < length) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the sparse size, " + src.length
            );
        }

        // Copy in
        for (int i=0; i < length; i++) {
            final T value = src[srcPos + i];
            if (value == null) {
                myElements.remove(dstPos + i);
            }
            else {
                myElements.put(dstPos + i, value);
            }
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        preRead();
        return myElements.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final T obj)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        if (obj == null) {
            myElements.remove(index);
        }
        else {
            myElements.put(index, obj);
        }
        postWrite();
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
