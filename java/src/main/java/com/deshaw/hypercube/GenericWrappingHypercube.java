package com.deshaw.hypercube;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has Java {@link Object}s as its elements which come from a
 * user-supplied {@link List}.
 *
 * @param <T> The type of the element which we store.
 */
public class GenericWrappingHypercube<T>
    extends AbstractHypercube<T>
    implements GenericHypercube<T>
{
    /**
     * A {@link List} which is a flattened view of a list of lists, or a list of
     * list of lists, or a [yeah, alright Stan...].
     */
    private static class FlattenedList<T>
        extends AbstractList<T>
    {
        /**
         * A copy of the list which we wrap.
         */
        private final List<Object> myList;

        /**
         * The stride of the indexing. If this is negative then it means that the
         * wrapped list contains elements, not sub-lists.
         */
        private final int myStride;

        /**
         * Constructor.
         *
         * @param list  The list to wrap.
         */
        @SuppressWarnings("unchecked")
        public FlattenedList(final List<?> list)
        {
            // Check input
            if (list == null) {
                throw new NullPointerException("Given a null list");
            }

            // Figure out the stride. If we have a non-multi-dimenstional list then
            // we can just wrap it directly.
            if (list.isEmpty() || !(list.get(0) instanceof List)) {
                myList = (List)list;
                myStride = -1;
                return;
            }

            // Determine the dimensionality of the list
            @SuppressWarnings("unchecked")
                final List<?> subList = (List<?>)list.get(0);
            if (subList.isEmpty() || !(subList.get(0) instanceof List)) {
                // It's 2D so we can wrap it directly and set the stride
                // accordingly
                myList = (List)list;
                myStride = subList.size();
                return;
            }

            // Okay, we are wrapping a list which is more than 2D, we need to make a
            // copy which wraps those higher dimensions, making it 2D
            final List<Object> lst = new ArrayList<Object>(list.size());
            int stride = -1;
            for (int i=0; i < list.size(); i++) {
                final FlattenedList<T> flat =
                    new FlattenedList<T>((List<?>)list.get(i));
                lst.add(flat);
                if (i == 0) {
                    stride = flat.size();
                }
            }
            myList   = lst;
            myStride = stride;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int size()
        {
            return (myStride < 0)
                ? myList.size()
                : (int)Math.min(Integer.MAX_VALUE,
                                (long)myList.size() * myStride);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        public T get(int idx)
        {
            return (T)((myStride < 0)
                ? myList.get(idx)
                : ((List)myList.get(idx / myStride)).get(idx % myStride)
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        public T set(int idx, T el)
        {
            return (T)((myStride < 0)
                ? myList.set(idx, el)
                : ((List)myList.get(idx / myStride)).set(idx % myStride, el)
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * The array of elements which we hold.
     */
    private final List<T> myElements;

    // -----------------------------------------------------------------------

    /**
     * Compute the element type for the given list.
     */
    @SuppressWarnings("unchecked")
    private static final Class<?> computeElementType(final List<?> elements)
    {
        for (int i=0; i < elements.size(); i++) {
            try {
                final Object element = elements.get(i);
                return (element instanceof List)
                    ? computeElementType((List<?>)element)
                    : element.getClass();
            }
            catch (NullPointerException e) {
                // Try the next one...
            }
        }
        throw new NullPointerException("No non-null elements found");
    }

    /**
     * Constructor.
     *
     * Some assumptions are made about the given {@code elements}:<ul>
     *  <li>It is non-empty.</li>
     *  <li>At least one element of the list is non-{@code null}.</li>
     *  <li>If the list is multi-dimensional then all the dimensions are
     *      represented as {@link List}s.</li>
     *  <li>All the sub-elements' dimensions are homogenous in size.</li>
     *  <li>The list's shape will not be changed in any way after it's been
     *      given to this class to wrap.</li>
     *  <li>All the elements are assignable to {@code T}.</li>
     *  <li>There will be no more than {@link Integer}{@code .MAX_VALUE}
     *      elements in total.</li>
     * </ul>
     *
     * <p>The given list will be effectively flattened and reshaped according to
     * the given dimensions.
     *
     * @param dimensions The dimensions of the hypercube.
     * @param elements   The elements to wrap. Must be non-empty.
     */
    @SuppressWarnings("unchecked")
    public GenericWrappingHypercube(final Dimension<?>[] dimensions,
                                    final List<?>        elements)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {
        super(dimensions, (Class<T>)computeElementType(elements));

        int size = 1;
        for (Dimension<?> dim : dimensions) {
            size *= dim.length();
        }

        // Flatten the given List. If it looks like it was already flat then
        // throw away the view.
        List<T> toWrap = new FlattenedList<>(elements);
        if (toWrap.size() == elements.size()) {
            toWrap = (List<T>)(List)elements;
        }

        // Now check
        if (toWrap.size() != size) {
            throw new IllegalArgumentException(
                "Number of elements, " + toWrap.size() + ", " +
                "does not match expected size, " + size + " " +
                "for dimensions " + Arrays.toString(dimensions)
            );
        }

        myElements = toWrap;
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
            dst[dstPos + i] = myElements.get((int)(srcPos + i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
        for (int i=0; i < length; i++) {
            myElements.set((int)(dstPos + i), src[(int)(srcPos + i)]);
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
        if (index > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        preRead();
        return myElements.get((int)index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final T obj)
        throws IndexOutOfBoundsException
    {
        if (index > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Bad index: " + index);
        }
        myElements.set((int)index, obj);
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
