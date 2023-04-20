package com.deshaw.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A reentrant lock whose notion of which thread holds it corresponds to a
 * virtual thread.
 *
 * <p>The idea is that, one may have multiple threads all comprising a single
 * "virtual" thread. If one of these threads has acquired the lock then another
 * thread, which is part of the same virtual thread, may also acquire it. A
 * thread which is not a part of that virtual thread, may not. This mirrors what
 * a ReentrantLock does in a regular thread.
 */
public class VirtualThreadLock
    implements Lock
{
    /**
     * A virtual thread.
     */
    public static final class VirtualThread
        extends Thread
    {
        /**
         * The thread's toString representation.
         */
        private final String myString;

        /**
         * Constructor.
         */
        public VirtualThread(final String name)
        {
            super(name);
            myString = "VirtualThread[" + name + "]";
        }

        /**
         * We don't support actually running...
         */
        @Override
        public void run()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myString;
        }
    }

    // ----------------------------------------------------------------------

    /**
     * How this thread determines what its virtual thread is
     */
    private static final ThreadLocal<VirtualThread> ourVirtualThread =
        new ThreadLocal<VirtualThread>() {
            @Override public VirtualThread initialValue() { return null; }
        };

    /**
     * The lock we use to protect myCount and myHolder (as well as for
     * signalling).
     */
    private final Lock myLock = new ReentrantLock();

    /**
     * The condition on which we can signal that the virtual lock may attempt to
     * be acquired.
     */
    private final Condition myCondition = myLock.newCondition();

    /**
     * How many holders there are of this lock.
     */
    private int myCount = 0;

    /**
     * The thread which is holding the lock.
     */
    private VirtualThread myHolder = null;

    // ----------------------------------------------------------------------

    /**
     * Set the "virtual thread" for this thread.
     * <p>
     * If the given thread was null then this will unset any association. You
     * must explicitly remove <i>any</i> existing association (even if you are
     * trying to associate with the same VirtualThread) before creating a new
     * one; this is to guard against errors.
     *
     * @throws IllegalArgumentException if there was already an associated
     *                                  thread.
     */
    public static void setThread(final VirtualThread thread)
        throws IllegalArgumentException
    {
        // Ensure we are not overwriting
        if (thread != null && ourVirtualThread.get() != null) {
            throw new IllegalArgumentException(
                "Thread " + Thread.currentThread() + " " +
                "tried to associate with " + thread + " " +
                "but is already associated with " + ourVirtualThread.get()
            );
        }

        // Okay to set
        ourVirtualThread.set(thread);
    }

    /**
     * Get the "virtual thread" for this thread.
     *
     * @throws IllegalArgumentException if there is no associated thread.
     */
    public static VirtualThread getThread()
        throws IllegalArgumentException
    {
        final VirtualThread thread = ourVirtualThread.get();
        if (thread == null) {
            throw new IllegalArgumentException(
                "No virtual thread associated with " + Thread.currentThread()
            );
        }
        return thread;
    }

    // ----------------------------------------------------------------------

    /**
     * Our name, if any.
     */
    private final String myName;

    /**
     * Default constructor.
     */
    public VirtualThreadLock()
    {
        this(null);
    }

    /**
     * Constructor with a name.
     */
    public VirtualThreadLock(final String name)
    {
        myName = name;
    }

    /**
     * Get our name, if any (may be null).
     */
    public String getName()
    {
        return myName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void lock()
    {
        final VirtualThread thread = getThread();
        myLock.lock();
        try {
            while (true) {
                // Different states of the world:
                if (myCount == 0) {
                    // We are the first to acquire the lock; remember our
                    // virtual thread and up the count and we're done
                    myCount++;
                    myHolder = thread;
                    return;
                }
                else if (myHolder == thread) {
                    // We're not the first but we have the right virtual thread
                    // so just up the count
                    myCount++;
                    return;
                }
                else {
                    // This lock is being held by another virtual thread; wait
                    // for it to be released
                    try {
                        myCondition.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, just go around and try again
                    }
                }
            }
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void lockInterruptibly()
        throws InterruptedException
    {
        final VirtualThread thread = getThread();
        myLock.lock();
        try {
            while (true) {
                // Different states of the world:
                if (myCount == 0) {
                    // We are the first to acquire the lock; remember our
                    // virtual thread and up the count and we're done
                    myCount++;
                    myHolder = thread;
                    return;
                }
                else if (myHolder == thread) {
                    // We're not the first but we have the right virtual thread
                    // so just up the count
                    myCount++;
                    return;
                }
                else {
                    // This lock is being held by another virtual thread; wait
                    // for it to be released
                    myCondition.await();
                }
            }
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryLock()
    {
        final VirtualThread thread = getThread();
        myLock.lock();
        try {
            while (true) {
                // Different states of the world:
                if (myCount == 0) {
                    // We are the first to acquire the lock; remember our
                    // virtual thread and up the count and we're done
                    myCount++;
                    myHolder = thread;
                    return true;
                }
                else if (myHolder == thread) {
                    // We're not the first but we have the right virtual thread
                    // so just up the count
                    myCount++;
                    return true;
                }
                else {
                    // This lock is being held by another virtual thread, failed to lock
                    return false;
                }
            }
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException
    {
        // Bail after this amount of time
        final long startNs   = System.nanoTime();
        final long timeoutNs = unit.toNanos(time);

        final VirtualThread thread = getThread();
        myLock.lock();
        try {
            do {
                // Different states of the world:
                if (myCount == 0) {
                    // We are the first to acquire the lock; remember our
                    // virtual thread and up the count and we're done
                    myCount++;
                    myHolder = thread;
                    return true;
                }
                else if (myHolder == thread) {
                    // We're not the first but we have the right virtual thread
                    // so just up the count
                    myCount++;
                    return true;
                }
                else {
                    // This lock is being held by another virtual thread; wait
                    // for it to be released
                    try {
                        myCondition.awaitNanos(timeoutNs - (System.nanoTime() - startNs));
                    }
                    catch (InterruptedException e) {
                        // Nothing, just go around and try again, provided that
                        // we're under the timeout
                    }
                }
            } while (System.nanoTime() - startNs < timeoutNs);
        }
        finally {
            myLock.unlock();
        }

        // If we got here we failed to lock
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock()
        throws IllegalMonitorStateException
    {
        final VirtualThread thread = getThread();
        myLock.lock();
        try {
            // Different states of the world:
            if (myCount == 0) {
                // This lock isn't held by anyone
                throw new IllegalMonitorStateException("Lock is not held");
            }
            else if (myHolder != thread) {
                // This lock isn't held by us
                throw new IllegalMonitorStateException(
                    "Lock is held by " + myHolder + ", not by " + thread
                );
            }
            else {
                // We were the holder so we drop it
                if (--myCount == 0) {
                    // We were the last holder
                    myHolder = null;
                }

                // Tell everyone that the lock has been released; they will have
                // to figure out who is actually trying to acquire it
                myCondition.signalAll();
            }
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Not supported (yet).
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the number of holds on this lock by the current virtual thread (if
     * any).
     */
    public int getHoldCount()
    {
        myLock.lock();
        try {
            return (myHolder == getThread()) ? myCount : 0;
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * Return whether the lock is held by the current virtual thread.
     */
    public boolean isHeldByCurrentThread()
    {
        myLock.lock();
        try {
            return (myHolder == getThread());
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * Return whether the lock is held by any virtual thread.
     */
    public boolean isLocked()
    {
        myLock.lock();
        try {
            return (myCount > 0);
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * Returns the virtual thread that currently owns this lock (if any).
     */
    public VirtualThread getOwner()
    {
        myLock.lock();
        try {
            return myHolder;
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        VirtualThread o = getOwner();
        return ((myName == null) ? super.toString() : myName) +
               ((o == null) ?
                "[Unlocked]" :
                "[Locked by thread " + o + "]");
    }
}
