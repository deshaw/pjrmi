package com.deshaw.python;

import com.deshaw.python.DType.Type;
import com.deshaw.util.StringUtil;

import java.lang.reflect.Array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Simple {@code numpy} array implementation.
 *
 * <p>Caveats: <ul>
 *   <li>Signedness of integers is ignored.
 *   <li>Arrays greater than 2GB are not supported.
 * </ul>
 */
public class NumpyArray
{
    /**
     * Callback interface for {@link #visitElements(ElementVisitor)}.
     */
    public static interface ElementVisitor
    {
        /**
         * Callback visiting an element of an array at index {@code ixs}.
         *
         * @param array  The array to visit.
         * @param ixs    The index.
         */
        public void visit(final NumpyArray array, final int[] ixs);
    }

    // ----------------------------------------------------------------------

    /**
     * Array's dtype.
     */
    private DType myDType;

    /**
     * Type of an array element.
     */
    private Type myType;

    /**
     * Byte buffer wrapping the underlying data.
     */
    private ByteBuffer myByteBuffer;

    /**
     * Number of dimensions.
     */
    private int myNumDimensions;

    /**
     * Array shape.
     */
    private int[] myShape;

    /**
     * Array strides.
     */
    private int[] myStrides;

    /**
     * Whether array data is stored in Fortran order (by column).
     */
    private boolean myIsFortran;

    // ----------------------------------------------------------------------

    /**
     * Estimate the shape of a (possibly multidimensional) array
     * and the ultimate component type.
     *
     * <p>The answer may be wrong if the array is ragged.
     *
     * @param array  The array to get the shape of.
     *
     * @return the numpy-style array shape.
     */
    public static int[] getJavaArrayShape(final Object array)
    {
        List<Integer> shape = new ArrayList<>();
        getArrayShapeAndType(array, shape);
        int[] result = new int[shape.size()];
        for (int i=0; i < shape.size(); i++) {
            result[i] = shape.get(i);
        }
        return result;
    }

    /**
     * Helper for extracting an int from what we know is a
     * 1D {@code NumpyArray}.
     *
     * @param array  The array to get the value from.
     * @param ix     The index of the element to get.
     *
     * @return the integer at that index.
     */
    public static int getInt(final Object array, final int ix)
    {
        return ((NumpyArray) array).getInt(ix);
    }

    /**
     * Helper for extracting a long from what we know is a
     * 1D {@code NumpyArray}.
     *
     * @param array  The array to get the value from.
     * @param ix     The index of the element to get.
     *
     * @return the long at that index.
     */
    public static long getLong(final Object array, final int ix)
    {
        return ((NumpyArray) array).getLong(ix);
    }

    /**
     * Helper for extracting a double from what we know is a
     * 1D {@code NumpyArray}.
     *
     * @param array  The array to get the value from.
     * @param ix     The index of the element to get.
     *
     * @return the double at that index.
     */
    public static double getDouble(Object array, int ix)
    {
        return ((NumpyArray) array).getDouble(ix);
    }

    /**
     * Construct an array of zeros.
     *
     * @param dtype      The type of the resultant array.
     * @param isFortran  Whether the array layout is Fortran or C style.
     * @param shape      The shape of the array.
     *
     * @return the generated array.
     */
    public static NumpyArray zeros(final DType   dtype,
                                   final boolean isFortran,
                                   final int...  shape)
    {
        if (shape.length == 0) {
            throw new IllegalArgumentException("0-d arrays are not supported");
        }

        int size = dtype.size();
        for (int dimLength: shape) {
            size = Math.multiplyExact(size, dimLength);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        return new NumpyArray(dtype, isFortran, shape, byteBuffer);
    }

    /**
     * Construct a numpy array around a multi-dimensional Java array.
     *
     * @param dtype        The type of the resultant array.
     * @param isFortran    Whether the array layout is Fortran or C style.
     * @param sourceArray  The Java array to copy from.
     *
     * @return the generated array.
     */
    public static NumpyArray fromObject(final DType   dtype,
                                        final boolean isFortran,
                                        final Object  sourceArray)
    {
        final int[] shape = getJavaArrayShape(sourceArray);

        // Allocate storage
        final NumpyArray rv = zeros(dtype, isFortran, shape);

        // Copy data
        final int[] ixs = new int[shape.length];
        internalCopyArrayData(sourceArray, rv, ixs, 0, 0);

        return rv;
    }

    /**
     * Estimate the shape of a (possibly multidimensional) array
     * and the element type.
     *
     * <p>The answer may be wrong if the array is ragged.
     *
     * @param array     The array to interrogate.
     * @param outShape  Where to put the shape.
     *
     * @return the element type.
     */
    public static Class<?> getArrayShapeAndType(final Object        array,
                                                final List<Integer> outShape)
    {
        Objects.requireNonNull(array);
        Objects.requireNonNull(outShape);

        outShape.clear();

        Object   slice    = array;
        Class<?> sliceCls = slice.getClass();

        if (!sliceCls.isArray()) {
            throw new IllegalArgumentException("Not at array");
        }

        // Repeatedly descend into the 0th element along each
        // dimension.
        int dim = 0;
        Class<?> elementType = null;
        while (sliceCls.getComponentType() != null) {
            elementType = sliceCls.getComponentType();

            int dimLength;
            if (slice != null) {
                dimLength = Array.getLength(slice);
            }
            else {
                dimLength = 0;
            }

            outShape.add(dimLength);

            if (dimLength != 0) {
                slice = Array.get(slice, 0);
                if (slice == null) {
                    throw new NullPointerException(
                        "null subarray in dimension " + dim
                    );
                }
                sliceCls = slice.getClass();
            }
            else {
                // Once we encounter a dimension of length 0, we
                // generally lose all information about the types
                // and dimensions that follow. However, we may
                // still learn the original number of dimensions
                // and the component type if the array was declared
                // using a multidimensional type from the start.
                slice = null;
                sliceCls = sliceCls.getComponentType();
            }

            dim++;
        }

        return elementType;
    }

    /**
     * Whether the given DType is integer, or else floating point. If neither we
     * throw an {@link UnsupportedOperationException}.
     *
     * @param dtype  The type to examine.
     *
     * @return whether the type is integer.
     */
    private static boolean integralType(final DType dtype)
        throws UnsupportedOperationException
    {
        if (dtype == null) {
            throw new UnsupportedOperationException("Can't cast to null DType");
        }

        switch (dtype.type()) {
        case BOOLEAN:
        case INT8:
        case INT16:
        case INT32:
        case INT64:
            return true;

        case FLOAT32:
        case FLOAT64:
            return false;

        default:
            throw new UnsupportedOperationException("Can't cast to " + dtype);
        }
    }

    /**
     * Copy data from a Java source array to a numpy destination array.
     *
     * @param source       Where to copy from.
     * @param destination  Where to copy to.
     * @param ixs          The indices to work from.
     * @param dimStart     The dimension we are working for.
     * @param dstLinearIx  The index into the distination array.
     *
     * @return the generated array.
     */
    private static void internalCopyArrayData(final Object     source,
                                              final NumpyArray destination,
                                              final int[]      ixs,
                                              final int        dimStart,
                                              int              dstLinearIx)
    {
        final int size   = destination.shape (dimStart);
        final int stride = destination.stride(dimStart);

        if (source == null) {
            throw new NullPointerException(
                "null subarray at index " +
                    Arrays.toString(Arrays.copyOf(ixs, dimStart + 1))
            );
        }
        if (Array.getLength(source) != size) {
            throw new IllegalArgumentException(
                "ragged array at index " +
                    Arrays.toString(Arrays.copyOf(ixs, dimStart + 1))
            );
        }

        if (dimStart < destination.numDimensions() - 1) {
            // Recursively copy the next dimension
            for (int i = 0; i < size; i++) {
                ixs[dimStart] = i;
                internalCopyArrayData(
                    Array.get(source, i),
                    destination,
                    ixs,
                    dimStart + 1,
                    dstLinearIx
                );
                dstLinearIx += stride;
            }
            return;
        }

        // Copy the last dimension directly
        final boolean integralDestinationType = integralType(destination.dtype());
        final Class<?> componentType = source.getClass().getComponentType();

        if (componentType.isPrimitive()) {
            if (integralDestinationType &&
                componentType != Float.TYPE &&
                componentType != Double.TYPE)
            {
                for (int i = 0; i < size; i++) {
                    destination.set(dstLinearIx, Array.getLong(source, i));
                    dstLinearIx += stride;
                }
            }
            else {
                for (int i = 0; i < size; i++) {
                    destination.set(dstLinearIx, Array.getDouble(source, i));
                    dstLinearIx += stride;
                }
            }
        }
        else {
            for (int i = 0; i < size; i++) {
                Object v = Array.get(source, i);

                try {
                    Number n = (Number) v;
                    if (integralDestinationType) {
                        destination.set(dstLinearIx, n.longValue());
                    }
                    else {
                        destination.set(dstLinearIx, n.doubleValue());
                    }
                }
                catch (NullPointerException e) {
                    ixs[dimStart] = i;
                    throw new IllegalArgumentException(
                        "null element at index " + Arrays.toString(ixs)
                    );
                }
                catch (ClassCastException e) {
                    ixs[dimStart] = i;
                    throw new IllegalArgumentException(
                        "non-number element " + v + " " +
                            "at index " + Arrays.toString(ixs)
                    );
                }

                dstLinearIx += stride;
            }
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Only allow the pickle framework to create uninitialized instances. Handle
     * this by making this CTOR protected. (Not perfect but...)
     */
    protected NumpyArray()
    {
        // Nothing
    }

    /**
     * Instantiate a contiguous array and compute strides automatically.
     *
     * @param dtype      The type of the resultant array.
     * @param isFortran  Whether the array layout is Fortran or C style.
     * @param shape      The shape of the resultant arrau.
     * @param data       The data to initialize the array with.
     */
    public NumpyArray(final DType      dtype,
                      final boolean    isFortran,
                      final int[]      shape,
                      final ByteBuffer data)
    {
        initArray(dtype, isFortran, shape, null, data);
    }

    /**
     * Instantiate an array with explicit strides.
     *
     * @param dtype      The type of the resultant array.
     * @param isFortran  Whether the array layout is Fortran or C style.
     * @param shape      The shape of the resultant arrau.
     * @param strides    The numpy-style array strides.
     * @param data       The data to initialize the array with.
     */
    public NumpyArray(final DType      dtype,
                      final boolean    isFortran,
                      final int[]      shape,
                      final int[]      strides,
                      final ByteBuffer data)
    {
        initArray(dtype, isFortran, shape, strides, data);
    }

    /**
     * Total number of elements in the array.
     *
     * @return the array's size.
     */
    public int size()
    {
        int size = 1;
        for (int i = 0; i < myNumDimensions; i++) {
            size = Math.multiplyExact(size, myShape[i]);
        }
        return size;
    }

    /**
     * Returns a view into the array, selecting a particular
     * element along a dimension.
     *
     * @param dim  The dimension to select along.
     * @param ix   The index of the element in that dimension.
     *
     * @return the resultant view.
     */
    public NumpyArray select(final int dim, final int ix)
    {
        if (numDimensions() < 2) {
            throw new IllegalArgumentException(
                "select may not be used with arrays with fewer than 2 dimensions"
            );
        }

        NumpyArray collapsedDim = slice(dim, ix, ix + 1);
        assert collapsedDim.shape(dim) == 1:
               "dim = " + dim + ", " +
               "original shape = " + Arrays.toString(shape()) + ", " +
               "shape after selecting = " + Arrays.toString(collapsedDim.shape());

        // Drop the dimension we selected into
        int[] oldShape = collapsedDim.shape();
        int[] newShape = new int[oldShape.length - 1];
        for (int i=0, j=0; i < oldShape.length; i++) {
            if (i != dim) {
                newShape[j++] = oldShape[i];
            }
        }
        int[] oldStrides = collapsedDim.strides();
        int[] newStrides = new int[oldStrides.length - 1];
        for (int i=0, j=0; i < oldStrides.length; i++) {
            if (i != dim) {
                newStrides[j++] = oldStrides[i];
            }
        }

        return new NumpyArray(collapsedDim.dtype(),
                              collapsedDim.isFortran(),
                              newShape,
                              newStrides,
                              collapsedDim.myByteBuffer);
    }

    /**
     * Returns a view into the array with one of the dimensions
     * restricted to a range.
     *
     * @param dim   The dimension to select along.
     * @param from  The inclusive starting index of the element in that
     *              dimension.
     * @param to    The non-inclusive ending index of the element in that
     *              dimension.
     *
     * @return the resultant view.
     */
    public NumpyArray slice(int dim, int from, int to)
    {
        if (dim < 0 || dim >= myNumDimensions) {
            throw new IllegalArgumentException(
                "Dimension must be between 0 and " + (myNumDimensions - 1) + ", " +
                    "got " + dim
            );
        }

        // Fancy indexing mechanisms of numpy are not supported
        int dimSize = myShape[dim];
        if (from < 0 || from >= dimSize || to < from || to > dimSize) {
            throw new IllegalArgumentException(
                "Invalid slice range [" + from + ", " + to + ")"
            );
        }

        // Update the shape. Slicing does not affect strides, so we
        // do not need to do anything about them.
        int[] newShape = myShape.clone();
        newShape[dim] = to - from;

        // Offset the starting point of the data
        ByteBuffer view = myByteBuffer.duplicate();
        view.position(view.position() + from * myStrides[dim]);

        // Create a sliced view of the array
        return new NumpyArray(myDType, myIsFortran, newShape, myStrides, view);
    }

    /**
     * Return data at a given linear index as an int. Will silently
     * truncate types that don't fit in an int (matches numpy behaviour).
     *
     * @param linearIx  The index to get from.
     *
     * @return the value at the given index.
     */
    public int _int(int linearIx)
    {
        switch (myType) {
            case INT8:    return      myByteBuffer.get      (linearIx);
            case INT16:   return      myByteBuffer.getShort (linearIx);
            case INT32:   return      myByteBuffer.getInt   (linearIx);
            case INT64:   return (int)myByteBuffer.getLong  (linearIx);
            case FLOAT32: return (int)myByteBuffer.getFloat (linearIx);
            case FLOAT64: return (int)myByteBuffer.getDouble(linearIx);
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Set int data at a given linear index. Will silently truncate
     * types that don't fit in a destination (matches numpy behaviour).
     *
     * @param linearIx  The index to set at.
     * @param v         The value to set with.
     */
    public void set(int linearIx, int v)
    {
        switch (myType) {
            case INT8:    myByteBuffer.put      (linearIx, (byte)  v); break;
            case INT16:   myByteBuffer.putShort (linearIx, (short) v); break;
            case INT32:   myByteBuffer.putInt   (linearIx,         v); break;
            case INT64:   myByteBuffer.putLong  (linearIx,         v); break;
            case FLOAT32: myByteBuffer.putFloat (linearIx,         v); break;
            case FLOAT64: myByteBuffer.putDouble(linearIx,         v); break;
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Return data at a given linear index as a long. Will silently
     * truncate types that don't fit in a long (matches numpy behaviour).
     *
     * @param linearIx  The index to get from.
     *
     * @return the value at the given index.
     */
    public long _long(int linearIx)
    {
        switch (myType) {
            case INT8:    return       myByteBuffer.get      (linearIx);
            case INT16:   return       myByteBuffer.getShort (linearIx);
            case INT32:   return       myByteBuffer.getInt   (linearIx);
            case INT64:   return       myByteBuffer.getLong  (linearIx);
            case FLOAT32: return (long)myByteBuffer.getFloat (linearIx);
            case FLOAT64: return (long)myByteBuffer.getDouble(linearIx);
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Return data at a given linear index as a long. Will silently
     * truncate types that don't fit in a long (matches numpy behaviour).
     *
     * @param linearIx  The index to set at.
     * @param v         The value to set with.
     */
    public void set(int linearIx, long v)
    {
        switch (myType) {
            case INT8:    myByteBuffer.put      (linearIx, (byte)  v); break;
            case INT16:   myByteBuffer.putShort (linearIx, (short) v); break;
            case INT32:   myByteBuffer.putInt   (linearIx, (int)   v); break;
            case INT64:   myByteBuffer.putLong  (linearIx,         v); break;
            case FLOAT32: myByteBuffer.putFloat (linearIx,         v); break;
            case FLOAT64: myByteBuffer.putDouble(linearIx,         v); break;
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Return data at a given linear index as a double.
     *
     * @param linearIx  The index to get from.
     *
     * @return the value at the given index.
     */
    public double _double(int linearIx)
    {
        switch (myType) {
            case INT8:    return myByteBuffer.get      (linearIx);
            case INT16:   return myByteBuffer.getShort (linearIx);
            case INT32:   return myByteBuffer.getInt   (linearIx);
            case INT64:   return myByteBuffer.getLong  (linearIx);
            case FLOAT32: return myByteBuffer.getFloat (linearIx);
            case FLOAT64: return myByteBuffer.getDouble(linearIx);
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Return data at a given linear index as a double.
     *
     * @param linearIx  The index to set at.
     * @param v         The value to set with.
     */
    public void set(int linearIx, double v)
    {
        switch (myType) {
            case INT8:    myByteBuffer.put      (linearIx, (byte)  v); break;
            case INT16:   myByteBuffer.putShort (linearIx, (short) v); break;
            case INT32:   myByteBuffer.putInt   (linearIx, (int)   v); break;
            case INT64:   myByteBuffer.putLong  (linearIx, (long)  v); break;
            case FLOAT32: myByteBuffer.putFloat (linearIx, (float) v); break;
            case FLOAT64: myByteBuffer.putDouble(linearIx,         v); break;
            default:
                throw new UnsupportedOperationException(
                    "Unrecognized type " + myType + " of dtype " + myDType
                );
        }
    }

    /**
     * Length of a particular dimension.
     *
     * @param dim  The dimension to query.
     *
     * @return the shape/length in that dimension.
     */
    public int shape(int dim)
    {
        return myShape[dim];
    }

    /**
     * Stride of a particular dimension.
     *
     * @param dim  The dimension to query.
     *
     * @return the stride in that dimension.
     */
    public int stride(int dim)
    {
        return myStrides[dim];
    }

    /**
     * Convert index along the first axis into a linear index.
     *
     * @param x1  The index along the first axis.
     *
     * @return the linear index.
     */
    public int ix(int x1)
    {
        return myStrides[0] * x1;
    }

    /**
     * Convert indices along the first two axes into a linear index.
     *
     * @param x1  The index along the first axis.
     * @param x2  The index along the second axis.
     *
     * @return the linear index.
     */
    public int ix(int x1, int x2)
    {
        return myStrides[0] * x1 +
               myStrides[1] * x2;
    }

    /**
     * Convert indices along the first three axes into a linear index.
     *
     * @param x1  The index along the first axis.
     * @param x2  The index along the second axis.
     * @param x3  The index along the third axis.
     *
     * @return the linear index.
     */
    public int ix(int x1, int x2, int x3)
    {
        return myStrides[0] * x1 +
               myStrides[1] * x2 +
               myStrides[2] * x3;
    }

    /**
     * Convert an arbitrary-dimensional index into a linear index.
     * Any unspecified trailing indices are presumed to be 0.
     *
     * @param x  The indices.
     *
     * @return the linear index.
     */
    @SuppressWarnings({ "OverloadedVarargsMethod" })
    public int ix(int... x)
    {
        int ix = 0;
        for (int i = 0, l = x.length; i < l; i++) {
            ix += x[i] * myStrides[i];
        }
        return ix;
    }

    /**
     * Visit every element of the array.
     *
     * @param visitor  The visitor to use.
     */
    public void visitElements(ElementVisitor visitor)
    {
        int[] ixs = new int[numDimensions()];
        internalVisitElements(visitor, ixs, 0);
    }

    /**
     * Recursive helper used to implement {@link #visitElements(ElementVisitor)}.
     *
     * @param visitor  The visitor to use.
     * @param ixs      The indices to visit along.
     * @param dim      The dimension which we are visiting.
     */
    private void internalVisitElements(ElementVisitor visitor, int[] ixs, int dim)
    {
        boolean lastDimension = dim == numDimensions() - 1;
        for (int i = 0, size = shape(dim); i < size; i++) {
            ixs[dim] = i;
            if (lastDimension) {
                visitor.visit(this, ixs);
            }
            else {
                internalVisitElements(visitor, ixs, dim + 1);
            }
        }
    }

    /**
     * Roll an axis from one position to another. Returns a view into the array.
     *
     * <p>This method behaves like {@code moveaxis} method from numpy 1.11.
     *
     * @param from  Old index of an axis.
     * @param to    New index of an axis.
     *
     * @return a rolled view of this array.
     */
    public NumpyArray rollAxis(int from, int to)
    {
        final int ndim = numDimensions();

        // Figure out how the axes are going to move
        final int[] srcIxs = new int[ndim];
        for (int i=0, j=0; i < ndim; i++) {
            if (i != from) {
                srcIxs[j++] = i;
            }
            if (i == to) {
                srcIxs[j++] = from;
            }
        }

        final int[] newShape   = new int[ndim];
        final int[] newStrides = new int[ndim];
        for (int i = 0; i < ndim; i++) {
            newShape  [i] = myShape  [srcIxs[i]];
            newStrides[i] = myStrides[srcIxs[i]];
        }

        return new NumpyArray(dtype(),
                              myIsFortran,
                              newShape,
                              newStrides,
                              myByteBuffer);
    }

    /**
     * Make a copy of the array. The copy will be allocated on the heap.
     *
     * @return the copy.
     */
    public NumpyArray copy()
    {
        return asType(myDType, true, myIsFortran, false);
    }

    /**
     * Return the array with the same contents as this array, cast to a specific
     * type.
     *
     * @param dtype           The new dtype for the array.
     * @param copy            When {@code false}, do not make a copy if the
     *                        array is already of the correct type; otherwise,
     *                        always make a copy.
     * @param isFortran       Whether the resultant array should be Fortran style.
     * @param allocateDirect  Whether the new array should be backed by a
     *                        "direct" buffer.
     *
     * @throws UnsupportedOperationException if it can't be cast to the given type.
     *
     * @return the copy.
     */
    public NumpyArray asType(final DType   dtype,
                             final boolean copy,
                             final boolean isFortran,
                             final boolean allocateDirect)
        throws UnsupportedOperationException
    {
        if (!copy) {
            // Verify compatibility
            if (dtype.equals(myDType)         &&
                isFortran      == myIsFortran &&
                allocateDirect == myByteBuffer.isDirect())
            {
                return this;
            }
        }

        // Allocate new storage
        final int newSizeBytes = Math.multiplyExact(size(), dtype.size());
        final NumpyArray dst =
            new NumpyArray(
                dtype,
                isFortran,
                shape(),
                allocateDirect ? ByteBuffer.allocateDirect(newSizeBytes)
                               : ByteBuffer.allocate      (newSizeBytes)
            );

        final boolean integralDestinationType = integralType(dtype);

        visitElements(
            (src, ixs) -> {
                final int srcIx = src.ix(ixs);
                final int dstIx = dst.ix(ixs);
                if (integralDestinationType) {
                    dst.set(dstIx, _long(srcIx));
                }
                else {
                    dst.set(dstIx, _double(srcIx));
                }
            }
        );
        return dst;
    }

    /**
     * Return the underlying byte buffer. Use with care.
     *
     * @return the buffer backing this array.
     */
    public ByteBuffer getByteBuffer()
    {
        return myByteBuffer;
    }

    /**
     * Array dtype.
     *
     * @return the dtype of this array.
     */
    public DType dtype()
    {
        return myDType;
    }

    /**
     * Number of array dimensions (same as {@code shape().length}).
     *
     * @return the dimensionality of this array.
     */
    public int numDimensions()
    {
        return myNumDimensions;
    }

    /**
     * Array shape.
     *
     * @return the shape of this array.
     */
    public int[] shape()
    {
        return myShape;
    }

    /**
     * Array strides. Use with care.
     *
     * @return the array containing the strides of this array.
     */
    public int[] strides()
    {
        return myStrides;
    }

    /**
     * Whether array data is stored in Fortran order (by column).
     *
     * @return whether the array is Fortran ordering.
     */
    public boolean isFortran()
    {
        return myIsFortran;
    }

    /**
     * Helper for accessing an int at a given index in a 1D array.
     *
     * @param ix  The index to look up at.
     *
     * @return the value at the given index.
     */
    public int getInt(int ix)
    {
        return _int(ix(ix));
    }

    /**
     * Helper for accessing a long at a given index in a 1D array.
     *
     * @param ix  The index to look up at.
     *
     * @return the value at the given index.
     */
    public long getLong(int ix)
    {
        return _long(ix(ix));
    }

    /**
     * Helper for accessing a double at a given index in a 1D array.
     *
     * @param ix  The index to look up at.
     *
     * @return the value at the given index.
     */
    public double getDouble(int ix)
    {
        return _double(ix(ix));
    }

    /**
     * Validate array's dimensions.
     *
     * @param name           The name of the array, for error messages.
     * @param expectedShape  Expected shape of the array. Axes where
     *                       the shape dimensions are -1 are excluded
     *                       from checking, though the number of
     *                       dimensions must still match exactly.
     */
    public void validateShape(String name, int... expectedShape)
    {
        if (numDimensions() != expectedShape.length) {
            throw new IllegalArgumentException(
                "'" + name + "' must have exactly " + expectedShape.length + " " +
                "dimension" + (expectedShape.length == 1 ? "" : "s") + "; " +
                "received " + this
            );
        }

        int[] shape = shape();
        for (int i = 0; i < expectedShape.length; i++) {
            // Allow entries in expectedShape to be -1 to represent
            // placeholders.
            if (expectedShape[i] == -1) {
                continue;
            }

            // Everything else must match perfectly
            if (shape[i] != expectedShape[i]) {
                throw new IllegalArgumentException(
                    "'" + name + "' shape must be " +
                    Arrays.toString(expectedShape) + "; " +
                    "received " + this
                );
            }
        }
    }

    /**
     * Convert a {@code NumpyArray} to the corresponding, possibly
     * multi-dimensional, array of doubles.
     *
     * @return The multi-dimensional array.
     */
    public Object toDoubleArray()
    {
        int[] shape = shape();
        Object rv = Array.newInstance(Double.TYPE, shape);
        if (shape.length == 1) {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, getDouble(i));
            }
        }
        else {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, select(0, i).toDoubleArray());
            }
        }
        return rv;
    }

    /**
     * Convert a {@code NumpyArray} to the corresponding, possibly
     * multi-dimensional, array of ints.
     *
     * @return The multi-dimensional array.
     */
    public Object toIntArray()
    {
        int[] shape = shape();
        Object rv = Array.newInstance(Integer.TYPE, shape);
        if (shape.length == 1) {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, getInt(i));
            }
        }
        else {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, select(0, i).toIntArray());
            }
        }
        return rv;
    }

    /**
     * Convert a {@code NumpyArray} to the corresponding, possibly
     * multi-dimensional, array of longs.
     *
     * @return The multi-dimensional array.
     */
    public Object toLongArray()
    {
        int[] shape = shape();
        Object rv = Array.newInstance(Long.TYPE, shape);
        if (shape.length == 1) {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, getLong(i));
            }
        }
        else {
            for (int i = 0, size = shape[0]; i < size; i++) {
                Array.set(rv, i, select(0, i).toLongArray());
            }
        }
        return rv;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "<" +
            numDimensions() + "-d " +
            dtype() + " array: " +
            Arrays.toString(shape()) +
        ">";
    }

    /**
     * Initialize array structures. This method builds the array around
     * the passed-in data without making a copy.
     *
     * @param dtype       The type of the resultant array.
     * @param isFortran   Whether the array layout is Fortran or C style.
     * @param shapeArray  The shape of the resultant arrau.
     * @param strides     The numpy-style array strides.
     * @param data        The data to initialize the array with.
     */
    protected void initArray(final DType      dtype,
                             final boolean    isFortran,
                             final int[]      shapeArray,
                             final int[]      strides,
                             final ByteBuffer data)
    {
        myDType = dtype;
        myIsFortran = isFortran;

        myShape = shapeArray.clone();
        myNumDimensions = myShape.length;

        myType = myDType.type();

        // Setup strides
        if (strides == null) {
            int currStride = myDType.size();
            myStrides = new int[myNumDimensions];
            for (int i = 0; i < myNumDimensions; i++) {
                int dim = myIsFortran ? i : myNumDimensions - i - 1;
                myStrides[dim] = currStride;
                currStride = Math.multiplyExact(currStride, myShape[dim]);
            }
        }
        else {
            if (strides.length != myNumDimensions) {
                throw new IllegalArgumentException(
                    "strides array must have the same length as the shape array"
                );
            }
            myStrides = strides.clone();
        }

        myByteBuffer = data.slice();
        myByteBuffer.order(myDType.bigEndian() ? ByteOrder.BIG_ENDIAN
                                               : ByteOrder.LITTLE_ENDIAN);
    }
}
