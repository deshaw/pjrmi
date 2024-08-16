import params

_PRIMITIVE_SPARSE_HYPERCUBE = '''\
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap;
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap.Iterator;

import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has {primitive_type} values as its elements and stores them
 * in a sparse map.
 *
 * <p>The capacity of these sparse cubes currently maxes out somewhere around
 * 5e8 elements. The actual limit will depend on the distribution of your
 * entries in the cube.
 */
public class {object_type}SparseHypercube
    extends Abstract{object_type}Hypercube
{{
    /**
     * The map which we use to store the values.
     */
    private final LongToLongConcurrentCuckooHashMap myMap;

    /**
     * The {{@code {primitive_type}}} null value as a {{@code long}}.
     */
    private final long myNull;

    // ----------------------------------------------------------------------

    // Some simple mapping functions; named like this for ease of cogging.
    // They should be trivially inlined by the JVM.

    /**
     * Convert a {{@code long}} to a {{@code double}}.
     */
    private static double long2double(final long v)
    {{
        return Double.longBitsToDouble(v);
    }}

    /**
     * Convert a {{@code double}} to a {{@code long}}.
     */
    private static long double2long(final double v)
    {{
        return Double.doubleToRawLongBits(v);
    }}

    /**
     * Convert a {{@code long}} to a {{@code float}}.
     */
    private static float long2float(final long v)
    {{
        return Float.intBitsToFloat((int)(v & 0xffffffffL));
    }}

    /**
     * Convert a {{@code float}} to a {{@code long}}.
     */
    private static long float2long(final float v)
    {{
        return ((long)Float.floatToRawIntBits(v)) & 0xffffffffL;
    }}

    /**
     * Convert a {{@code long}} to an {{@code int}}.
     */
    private static int long2int(final long v)
    {{
        return (int)v;
    }}

    /**
     * Convert an {{@code int}} to a {{@code long}}.
     */
    private static long int2long(final int v)
    {{
        return v;
    }}

    /**
     * Convert a {{@code long}} to a {{@code long}}.
     */
    private static long long2long(final long v)
    {{
        return v;
    }}

    // ----------------------------------------------------------------------

    /**
     * Constructor with a default {{@code null}} value, and a loading of
     * {{@code 0.1}}.
     */
    public {object_type}SparseHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {{
        this(dimensions, {primitive_from_null}, 0.1);
    }}

    /**
     * Constructor with a given {{@code null}} value and loading.
     *
     * @param nullValue  The value used to for missing entries.
     * @param loading    The value used to determine the initial backing space
     *                   capacity as a function of the logical size of the
     *                   hypercube.
     */
    public {object_type}SparseHypercube(
        final Dimension<?>[] dimensions,
        final {primitive_type} nullValue,
        final double loading
    )
        throws IllegalArgumentException,
               NullPointerException
    {{
        super(dimensions);

        if (Double.isNaN(loading)) {{
            throw new IllegalArgumentException("Given a NaN loading value");
        }}
        final int capacity =
            (int)Math.max(13,
                          Math.min(Integer.MAX_VALUE,
                                   getSize() * Math.max(0.0, Math.min(1.0, loading))));
        myMap = new LongToLongConcurrentCuckooHashMap(capacity);
        myNull = {primitive_type}2long(nullValue);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
                                final {object_type}[] dst,
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

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        preRead();
        for (int i=0; i < length; i++) {{
            dst[dstPos + i] = long2{primitive_type}(myMap.get(srcPos + i));
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void fromFlattenedObjs(final {object_type}[] src,
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

        // Check input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {{
            throw new NullPointerException("Given a null sparse");
        }}
        if (src.length - srcPos < length) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the sparse size, " + src.length
            );
        }}

        // Safe to copy in
        for (int i=0; i < length; i++) {{
            final {object_type} value = src[srcPos + i];
            mapPut(
                dstPos + i,
                {primitive_type}2long(
                    (value == null) ? {primitive_from_null} : value.{primitive_type}Value()
                )
            );
        }}
        postWrite();
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final {primitive_type}[] dst,
                            final int       dstPos,
                            final int       length)
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

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        preRead();
        for (int i=0; i < length; i++) {{
            dst[dstPos + i] = long2{primitive_type}(myMap.get(srcPos + i, myNull));
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void fromFlattened(final {primitive_type}[] src,
                              final int       srcPos,
                              final long      dstPos,
                              final int       length)
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

        // Sanitise input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {{
            throw new NullPointerException("Given a null sparse");
        }}

        // Safe to copy in
        for (int i=0; i < length; i++) {{
            mapPut(dstPos + i, {primitive_type}2long(src[srcPos + i]));
        }}
        postWrite();
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException
    {{
        return getAt(toOffset(indices));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void set(final {primitive_type} d, final long... indices)
        throws IndexOutOfBoundsException
    {{
        setAt(toOffset(indices), d);
        postWrite();
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {object_type} getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {{
        preRead();
        return {object_type}.valueOf(getAt(index));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setObjectAt(final long index, final {object_type} value)
        throws IndexOutOfBoundsException
    {{
        setAt(index, (value == null) ? {primitive_from_null} : value.{primitive_type}Value());
        postWrite();
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException
    {{
        if (index < 0 || index >= getSize()) {{
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }}
        preRead();
        return long2{primitive_type}(myMap.get(index, myNull));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
        if (index < 0 || index >= getSize()) {{
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }}
        mapPut(index, {primitive_type}2long(value));
        postWrite();
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {{
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      false);
        result.put("behaved",      false);
        result.put("c_contiguous", false);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }}

   /**
    * Put a value into the map, in such a way that understand null values.
    */
    private void mapPut(final long index, final long value)
    {{
        // If we happen to be inserting a null then that really means we are
        // removing an entry from the sparse map
        if (value == myNull) {{
            myMap.remove(index);
        }}
        else {{
            myMap.put(index, value);
        }}
    }}
}}
'''

def generate(dtype):
    return _PRIMITIVE_SPARSE_HYPERCUBE.format(**params.get_kwargs(dtype))
