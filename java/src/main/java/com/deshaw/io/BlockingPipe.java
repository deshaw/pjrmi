package com.deshaw.io;

import java.util.concurrent.locks.LockSupport;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A very simple pipe of fixed length which allows users to write data in one
 * end and read it out of another. The reader or writer will block if the pipe's
 * buffer is empty or full, respectively.
 *
 * <p>This class is only safe for one reader thread and one writer thread to use
 * at any given time. Reading and writing from the same thread may cause
 * deadlocks.
 *
 * <p>See also {@link java.io.PipedInputStream} and {@link java.io.PipedOutputStream}
 * which do roughly the same thing but more slowly.
 */
public class BlockingPipe
{
    /**
     * How to read from the pipe.
     */
    private class Input
        extends InputStream
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public int read()
            throws IOException
        {
            return BlockingPipe.this.read();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
            throws IOException
        {
            BlockingPipe.this.close();
        }
    }

    /**
     * How to write to the pipe.
     */
    private class Output
        extends OutputStream
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void write(int b)
            throws IOException
        {
            BlockingPipe.this.write(b);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
            throws IOException
        {
            BlockingPipe.this.close();
        }
    }

    /**
     * The head of the pipe.
     */
    private volatile long myHead;

    /**
     * The tail of the pipe.
     */
    private volatile long myTail;

    /**
     * The reader thread.
     */
    private volatile Thread myReader;

    /**
     * The writer thread.
     */
    private volatile Thread myWriter;

    /**
     * The input stream.
     */
    private final InputStream myInput;

    /**
     * The output stream.
     */
    private final OutputStream myOutput;

    /**
     * The pipe itself.
     */
    private final byte[] myData;

    /**
     * Whether the pipe is closed or not.
     */
    private volatile boolean myClosed;

    /**
     * Constructor.
     *
     * @param size  The size of the pipe.
     */
    public BlockingPipe(final int size)
    {
        if (size <= 0) {
            throw new IllegalArgumentException("Bad size: " + size);
        }

        myHead   = 0;
        myTail   = 0;
        myInput  = new Input();
        myOutput = new Output();
        myData   = new byte[size];
        myClosed = false;
    }

    /**
     * Get the input stream for reading from the pipe.
     *
     * @return the input stream.
     */
    public InputStream getInputStream()
    {
        return myInput;
    }

    /**
     * Get the output stream for writing to the pipe.
     *
     * @return the output stream.
     */
    public OutputStream getOutputStream()
    {
        return myOutput;
    }

    /**
     * Read an unsigned byte value out.
     *
     * <p>It is not safe to call this method from multiple threads.
     *
     * @return the byte we read, as an unsigned value, or {@code -1} if
     *          end-of-file was encountered.
     */
    protected int read()
    {
        // Wait for data to appear
        while (myHead == myTail) {
            // We've got no more data so this means EOF if we're closed
            if (myClosed) {
                return -1;
            }

            // Otherwise we wait for data
            myReader = Thread.currentThread();
            LockSupport.parkNanos(10000);
        }

        // Read it out
        final byte result = myData[(int)(myHead % myData.length)];
        myHead++;

        // Kick the other thread, which could be waiting
        LockSupport.unpark(myWriter);

        // Give back what we read
        return Byte.toUnsignedInt(result);
    }

    /**
     * Write a byte in.
     *
     * <p>It is not safe to call this method from multiple threads.
     *
     * @param b  The byte to write.
     *
     * @throws IOException if the pipe is closed.
     */
    protected void write(int b)
        throws IOException
    {
        // Can't write to a closed pipe
        if (myClosed) {
            throw new IOException("Broken pipe");
        }

        // Wait for the pipe to have room
        while (myTail - myHead >= myData.length) {
            myWriter = Thread.currentThread();
            LockSupport.parkNanos(10000);
        }

        // Write in the byte
        myData[(int)(myTail % myData.length)] = (byte)b;
        myTail++;

        // Kick the other thread if it's waiting
        LockSupport.unpark(myReader);
    }

    /**
     * Close the pipe.
     */
    protected void close()
    {
        myClosed = true;
    }
}
