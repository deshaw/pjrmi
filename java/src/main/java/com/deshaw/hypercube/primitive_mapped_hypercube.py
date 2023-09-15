import params

_PRIMITIVE_MAPPED_HYPERCUBE = '''\
import java.io.IOException;
import java.io.UncheckedIOException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.{short_object_type}Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has {{@code {primitive_type}}} values as its elements and stores
 * them in a memory-mapped file.
 *
 * <p>The expected layout of the memory-mapped file is such that a {{@code numpy}}
 * array can also wrap it on the Python side using something like:
 * <pre>
 *    array = numpy.memmap(filename, dtype=numpy.{dtype}, mode='w+', shape=shape, order='C')
 * </pre>
 */
public class {object_type}MappedHypercube
    extends Abstract{object_type}Hypercube
{{
    /**
     * The shift for the max buffer size.
     */
    private static final int MAX_BUFFER_SHIFT = 30;

    /**
     * The largest buffer size.
     */
    private static final long MAX_BUFFER_SIZE = (1L << MAX_BUFFER_SHIFT);

    /**
     * The mask for buffer index tweaking.
     */
    private static final long MAX_BUFFER_MASK = MAX_BUFFER_SIZE - 1;

    /**
     * The array of buffers which we hold as the underlying
     * {{@link MappedByteBuffer}}.
     */
    private final MappedByteBuffer[] myMappedBuffers;

    /**
     * The array of buffers-of-elements which we hold. We have multiple buffers
     * since we might have a size which is larger than what can be represented
     * by a single array. (I.e. more than 2^30 elements.)
     */
    private final {short_object_type}Buffer[] myBuffers;

    /**
     * Constructor.
     */
    public {object_type}MappedHypercube(final String path,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               IOException,
               NullPointerException
    {{
        this(FileChannel.open(Path.of(path),
                              StandardOpenOption.READ, StandardOpenOption.WRITE),
             dimensions);
    }}

    /**
     * Constructor.
     */
    public {object_type}MappedHypercube(final FileChannel channel,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               IOException,
               NullPointerException
    {{
        super(dimensions);

        int numBuffers = (int)(size >>> MAX_BUFFER_SHIFT);
        if (numBuffers * MAX_BUFFER_SIZE < size) {{
            numBuffers++;
        }}
        myMappedBuffers = new MappedByteBuffer[numBuffers];
        myBuffers       = new {short_object_type}Buffer[numBuffers];

        final long tail = (size & MAX_BUFFER_MASK);
        for (int i=0; i < numBuffers; i++) {{
            final long sz =
                ((i+1 < numBuffers) ? MAX_BUFFER_SIZE : tail) * {object_type}.BYTES;
            final long position = (long)i * MAX_BUFFER_SIZE * {object_type}.BYTES;
            if (LOG.isLoggable(Level.FINEST)) {{
                LOG.finest(
                    "Mapping in buffer[" + i + "] from " + channel + " " +
                    "at position " + position + " and size " + sz
                );
            }}
            final MappedByteBuffer buffer =
                channel.map(FileChannel.MapMode.READ_WRITE, position, sz);
            buffer.order(ByteOrder.nativeOrder());
            myMappedBuffers[i] = buffer;
            myBuffers      [i] = buffer.as{short_object_type}Buffer();
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void flush()
        throws IOException
    {{
        try {{
            for (MappedByteBuffer buffer : myMappedBuffers) {{
                buffer.force();
            }}
        }}
        catch (UncheckedIOException e) {{
            throw e.getCause();
        }}
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

        for (int i=0; i < length; i++) {{
            final long pos = srcPos + i;
            final {short_object_type}Buffer buffer =
                myBuffers[(int)(pos >>> MAX_BUFFER_SHIFT)];
            final {primitive_type} d = buffer.get((int)(pos & MAX_BUFFER_MASK));
            dst[dstPos + i] = {object_type}.valueOf(d);
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void fromFlattenedObjs(final {object_type}[] src,
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

        // Check input
        checkUnflattenArgs(srcPos, dstPos, length);
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

        // Safe to copy in
        for (int i=0; i < length; i++) {{
            final long pos = dstPos + i;
            final {short_object_type}Buffer buffer =
                myBuffers[(int)(pos >>> MAX_BUFFER_SHIFT)];
            final {object_type} value = src[srcPos + i];
            buffer.put(
                (int)(pos & MAX_BUFFER_MASK),
                (value == null) ? {primitive_from_null} : value.{primitive_type}Value()
            );
        }}
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
        if (dst == null) {{
            throw new NullPointerException("Given a null array");
        }}

        // These can never differ by more than 1 since length is an int. And the
        // end is non-inclusive since we're counting fence, not posts.
        final int startIdx = (int)((srcPos             ) >>> MAX_BUFFER_SHIFT);
        final int endIdx   = (int)((srcPos + length - 1) >>> MAX_BUFFER_SHIFT);
        if (startIdx == endIdx) {{
            // What to copy? Try to avoid the overhead of the system call. If we are
            // striding through the cube then we may well have just the one.
            final {short_object_type}Buffer buffer = myBuffers[startIdx];
            switch (length) {{
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                dst[dstPos] = buffer.get((int)(srcPos & MAX_BUFFER_MASK));
                break;

            default:
                // Copy within the same sub-array
                if (buffer != null) {{
                    // Move the buffer pointer and do the bulk operation
                    try {{
                        buffer.position((int)(srcPos & MAX_BUFFER_MASK));
                        buffer.get(dst, dstPos, length);
                    }}
                    catch (BufferOverflowException e) {{
                        throw new IndexOutOfBoundsException(e.toString());
                    }}
                }}
                else {{
                    Arrays.fill(dst, dstPos, dstPos + length, {primitive_from_null});
                }}
            }}
        }}
        else {{
            // Split into two copies
            final {short_object_type}Buffer startBuffer = myBuffers[startIdx];
            final {short_object_type}Buffer endBuffer   = myBuffers[endIdx  ];
            if (startBuffer != null && endBuffer != null) {{
                final int startPos    = (int)(srcPos & MAX_BUFFER_MASK);
                final int startLength = length - (startBuffer.limit() - startPos);
                final int endLength   = length - startLength;
                try {{
                    startBuffer.position((int)(startPos & MAX_BUFFER_MASK));
                    endBuffer  .position(0);
                    startBuffer.get(dst, dstPos,               startLength);
                    endBuffer  .get(dst, dstPos + startLength, endLength);
                }}
                catch (BufferUnderflowException e) {{
                    throw new IndexOutOfBoundsException(e.toString());
                }}
            }}
            else {{
                Arrays.fill(dst, dstPos, dstPos + length, {primitive_from_null});
            }}
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
            throw new NullPointerException("Given a null array");
        }}

        // These can never differ by more than 1 since length is an int
        final int startIdx = (int)((dstPos             ) >>> MAX_BUFFER_SHIFT);
        final int endIdx   = (int)((dstPos + length - 1) >>> MAX_BUFFER_SHIFT);

        // What to copy? Try to avoid the overhead of the system call. If we are
        // striding through the cube then we may well have just the one.
        if (startIdx == endIdx) {{
            // Get the buffer
            final {short_object_type}Buffer buffer = myBuffers[startIdx];

            // And handle it
            switch (length) {{
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                if (srcPos >= src.length) {{
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }}
                buffer.put((int)(dstPos & MAX_BUFFER_MASK), src[srcPos]);
                break;

            default:
                // Standard copy within the same sub-buffer
                if (src.length - srcPos < length) {{
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }}
                buffer.position((int)(dstPos & MAX_BUFFER_MASK));
                buffer.put(src, srcPos, length);
                break;
            }}
        }}
        else {{
            // Split into two copies
            final {short_object_type}Buffer startBuffer = myBuffers[startIdx];
            final {short_object_type}Buffer endBuffer   = myBuffers[endIdx  ];
            final int startPos    = (int)(dstPos & MAX_BUFFER_MASK);
            final int startLength = length - (startBuffer.limit() - startPos);
            final int endLength   = length - startLength;
            try {{
                startBuffer.position((int)(startPos & MAX_BUFFER_MASK));
                endBuffer  .position(0);
                startBuffer.put(src, srcPos,               startLength);
                endBuffer  .put(src, srcPos + startLength, endLength);
            }}
            catch (BufferOverflowException e) {{
                throw new IndexOutOfBoundsException(e.toString());
            }}
        }}
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
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {object_type} getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {{
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
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException
    {{

        final {short_object_type}Buffer buffer =
            myBuffers[(int)(index >>> MAX_BUFFER_SHIFT)];
        return buffer.get((int)(index & MAX_BUFFER_MASK));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
        final {short_object_type}Buffer buffer =
            myBuffers[(int)(index >>> MAX_BUFFER_SHIFT)];
        buffer.put((int)(index & MAX_BUFFER_MASK), value);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {{
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      true);
        result.put("behaved",      true);
        result.put("c_contiguous", true);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }}
}}
'''

def generate(dtype):
    return _PRIMITIVE_MAPPED_HYPERCUBE.format(**params.get_kwargs(dtype))
