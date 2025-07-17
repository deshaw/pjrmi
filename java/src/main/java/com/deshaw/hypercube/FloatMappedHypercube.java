package com.deshaw.hypercube;

// Recreate with `cog -rc FloatMappedHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_mapped_hypercube
//
//     cog.outl(primitive_mapped_hypercube.generate(numpy.float32))
// ]]]
import java.io.IOException;
import java.io.UncheckedIOException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has {@code float} values as its elements and stores
 * them in a memory-mapped file.
 *
 * <p>The expected layout of the memory-mapped file is such that a {@code numpy}
 * array can also wrap it on the Python side using something like:
 * <pre>
 *    array = numpy.memmap(filename, dtype=numpy.float32, mode='w+', shape=shape, order='C')
 * </pre>
 */
public class FloatMappedHypercube
    extends AbstractFloatHypercube
{
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
     * Read-only open options.
     */
    private static final OpenOption[] READ_ONLY =
        new OpenOption[] { StandardOpenOption.READ };

    /**
     * Read-write open options.
     */
    private static final OpenOption[] READ_WRITE =
        new OpenOption[] { StandardOpenOption.READ, StandardOpenOption.WRITE };

    /**
     * The array of buffers which we hold as the underlying
     * {@link MappedByteBuffer}.
     */
    private final MappedByteBuffer[] myMappedBuffers;

    /**
     * The array of buffers-of-elements which we hold. We have multiple buffers
     * since we might have a size which is larger than what can be represented
     * by a single array. (I.e. more than 2^30 elements.)
     */
    private final FloatBuffer[] myBuffers;

    /**
     * Constructor.
     */
    public FloatMappedHypercube(final String path,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               IOException,
               NullPointerException
    {
        this(path, false, dimensions);
    }

    /**
     * Constructor.
     */
    public FloatMappedHypercube(final String path,
                                        final boolean readonly,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               IOException,
               NullPointerException
    {
        this(FileChannel.open(Path.of(path), readonly ? READ_ONLY : READ_WRITE),
             dimensions);
    }

    /**
     * Constructor.
     */
    public FloatMappedHypercube(final FileChannel channel,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               IOException,
               NullPointerException
    {
        super(dimensions);

        int numBuffers = (int)(size >>> MAX_BUFFER_SHIFT);
        if (numBuffers * MAX_BUFFER_SIZE < size) {
            numBuffers++;
        }
        myMappedBuffers = new MappedByteBuffer[numBuffers];
        myBuffers       = new FloatBuffer[numBuffers];

        final long tail = (size & MAX_BUFFER_MASK);
        for (int i=0; i < numBuffers; i++) {
            final long sz =
                ((i+1 < numBuffers) ? MAX_BUFFER_SIZE : tail) * Float.BYTES;
            final long position = (long)i * MAX_BUFFER_SIZE * Float.BYTES;
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Mapping in buffer[" + i + "] from " + channel + " " +
                    "at position " + position + " and size " + sz
                );
            }
            final MappedByteBuffer buffer =
                channel.map(FileChannel.MapMode.READ_WRITE, position, sz);
            buffer.order(ByteOrder.nativeOrder());
            myMappedBuffers[i] = buffer;
            myBuffers      [i] = buffer.asFloatBuffer();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush()
        throws IOException
    {
        try {
            for (MappedByteBuffer buffer : myMappedBuffers) {
                buffer.force();
            }
        }
        catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
                                final Float[] dst,
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

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        for (int i=0; i < length; i++) {
            final long pos = srcPos + i;
            final FloatBuffer buffer =
                myBuffers[(int)(pos >>> MAX_BUFFER_SHIFT)];
            final float d = buffer.get((int)(pos & MAX_BUFFER_MASK));
            dst[dstPos + i] = Float.valueOf(d);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final Float[] src,
                                  final int       srcPos,
                                  final long      dstPos,
                                  final int       length)
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

        // Check input
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

        // Safe to copy in
        for (int i=0; i < length; i++) {
            final long pos = dstPos + i;
            final FloatBuffer buffer =
                myBuffers[(int)(pos >>> MAX_BUFFER_SHIFT)];
            final Float value = src[srcPos + i];
            buffer.put(
                (int)(pos & MAX_BUFFER_MASK),
                (value == null) ? Float.NaN : value.floatValue()
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final float[] dst,
                            final int       dstPos,
                            final int       length)
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
        if (dst == null) {
            throw new NullPointerException("Given a null array");
        }

        // These can never differ by more than 1 since length is an int. And the
        // end is non-inclusive since we're counting fence, not posts.
        final int startIdx = (int)((srcPos             ) >>> MAX_BUFFER_SHIFT);
        final int endIdx   = (int)((srcPos + length - 1) >>> MAX_BUFFER_SHIFT);
        if (startIdx == endIdx) {
            // What to copy? Try to avoid the overhead of the system call. If we are
            // striding through the cube then we may well have just the one.
            final FloatBuffer buffer = myBuffers[startIdx];
            switch (length) {
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                dst[dstPos] = buffer.get((int)(srcPos & MAX_BUFFER_MASK));
                break;

            default:
                // Copy within the same sub-array
                if (buffer != null) {
                    // Move the buffer pointer and do the bulk operation
                    try {
                        buffer.position((int)(srcPos & MAX_BUFFER_MASK));
                        buffer.get(dst, dstPos, length);
                    }
                    catch (BufferOverflowException e) {
                        throw new IndexOutOfBoundsException(e.toString());
                    }
                }
                else {
                    Arrays.fill(dst, dstPos, dstPos + length, Float.NaN);
                }
            }
        }
        else {
            // Split into two copies
            final FloatBuffer startBuffer = myBuffers[startIdx];
            final FloatBuffer endBuffer   = myBuffers[endIdx  ];
            if (startBuffer != null && endBuffer != null) {
                final int startPos    = (int)(srcPos & MAX_BUFFER_MASK);
                final int startLength = length - (startBuffer.limit() - startPos);
                final int endLength   = length - startLength;
                try {
                    startBuffer.position((int)(startPos & MAX_BUFFER_MASK));
                    endBuffer  .position(0);
                    startBuffer.get(dst, dstPos,               startLength);
                    endBuffer  .get(dst, dstPos + startLength, endLength);
                }
                catch (BufferUnderflowException e) {
                    throw new IndexOutOfBoundsException(e.toString());
                }
            }
            else {
                Arrays.fill(dst, dstPos, dstPos + length, Float.NaN);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final float[] src,
                              final int       srcPos,
                              final long      dstPos,
                              final int       length)
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

        // These can never differ by more than 1 since length is an int
        final int startIdx = (int)((dstPos             ) >>> MAX_BUFFER_SHIFT);
        final int endIdx   = (int)((dstPos + length - 1) >>> MAX_BUFFER_SHIFT);

        // What to copy? Try to avoid the overhead of the system call. If we are
        // striding through the cube then we may well have just the one.
        if (startIdx == endIdx) {
            // Get the buffer
            final FloatBuffer buffer = myBuffers[startIdx];

            // And handle it
            switch (length) {
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                if (srcPos >= src.length) {
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }
                buffer.put((int)(dstPos & MAX_BUFFER_MASK), src[srcPos]);
                break;

            default:
                // Standard copy within the same sub-buffer
                if (src.length - srcPos < length) {
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }
                buffer.position((int)(dstPos & MAX_BUFFER_MASK));
                buffer.put(src, srcPos, length);
                break;
            }
        }
        else {
            // Split into two copies
            final FloatBuffer startBuffer = myBuffers[startIdx];
            final FloatBuffer endBuffer   = myBuffers[endIdx  ];
            final int startPos    = (int)(dstPos & MAX_BUFFER_MASK);
            final int startLength = length - (startBuffer.limit() - startPos);
            final int endLength   = length - startLength;
            try {
                startBuffer.position((int)(startPos & MAX_BUFFER_MASK));
                endBuffer  .position(0);
                startBuffer.put(src, srcPos,               startLength);
                endBuffer  .put(src, srcPos + startLength, endLength);
            }
            catch (BufferOverflowException e) {
                throw new IndexOutOfBoundsException(e.toString());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final float d, final long... indices)
        throws IndexOutOfBoundsException
    {
        weakSetAt(toOffset(indices), d);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Float.valueOf(weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Float value)
        throws IndexOutOfBoundsException
    {
        weakSetAt(index, (value == null) ? Float.NaN : value.floatValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {

        final FloatBuffer buffer =
            myBuffers[(int)(index >>> MAX_BUFFER_SHIFT)];
        return buffer.get((int)(index & MAX_BUFFER_MASK));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final float value)
        throws IndexOutOfBoundsException
    {
        final FloatBuffer buffer =
            myBuffers[(int)(index >>> MAX_BUFFER_SHIFT)];
        buffer.put((int)(index & MAX_BUFFER_MASK), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      true);
        result.put("behaved",      true);
        result.put("c_contiguous", true);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }
}

// [[[end]]] (checksum: 0f53d0a0c6e0eee7a0b749c62609c8d7)
