package com.deshaw.util.concurrent;

import com.deshaw.util.Instrumentor;
import com.deshaw.util.Pool;
import com.deshaw.util.StringUtil;
import com.deshaw.util.SwappingPool;
import com.deshaw.util.ThreadLocalStringBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.deshaw.util.Instrumentor.INSTRUMENTOR_FACTORY;

// There are some alternative approaches to using this LockManager class which I
// will outline here. They all have their trade-offs.
//
//  o Spawn a watcher thread to periodically call the findDeadlockedThreads in
//    java.lang.management.ThreadMXBean (accessed via the
//    java.lang.management.ManagementFactory.getThreadMXBean() method) and deal
//    with any deadlocks which it finds. (Unclear how though.)
//
//  o Rather than using Lock.lock() we can always use tryLock(long,TimeUnit)
//    with a long timeout and assume that its failure implies a deadlock. (This
//    might not actually be the case however.)
//
// The below LockManager implementation is pretty good but it still employs a
// global lock in certain circumstances. Making this lock-free would be good but
// is non-trivial.

/**
 * A lock manager for reentrant locks. This provides basic locking primitives
 * and deadlock detection. All the locks which this class provides are
 * referenced by {@link CharSequence} handles. If you wish to avoid garbage
 * creation then you should use one of {@link StringUtil}'s hash-friendly
 * {@link CharSequence} implementations for these handles.
 *
 * <p>This class manages deadlock detection since it's possible for external
 * threads to acquire locks in ways which would cause them to hang forever. As
 * such it is important that _all_ locking is done via this class's methods.
 *
 * <p>Currently, if many short-lived threads interact with this class, it's
 * possible to exhaust the JVM's heap.
 */
public class LockManager
{
    /**
     * Our default logger.
     */
    private static final Logger LOG =
        Logger.getLogger("com.deshaw.util.concurrent.LockManager");

    /**
     * Thrown when we encounter a deadlock when trying to acquire a lock.
     * <p>
     * This is a RuntimeException since it may be thrown from Lock#lock() which
     * only throws RuntimeExceptions.
     */
    public static class DeadlockException
        extends RuntimeException
    {
        /**
         * CTOR.
         *
         * @param message  The exception message.
         */
        public DeadlockException(final String message)
        {
            super(message);
        }
    }

    /**
     * A Lock shim over a NamedLock instance. This will perform its various
     * operations via the LockManager so should be safe from deadlock.
     *
     * <p>These locks may be used in an {@link AutoCloseable} try-with-resources
     * context:<pre>
     *    try (SafeLock l = lock.acquire()) {
     *       // Do things under the lock
     *       ...
     *    }
     * </pre>
     */
    public class SafeLock
        implements AutoCloseable,
                   Lock
    {
        /**
         * The exception thrown when {@code tryAcquire()} fails.
         */
        public class AcquireFailedException
            extends Exception
        {
            /**
             * CTOR.
             */
            private AcquireFailedException()
            {
                super("Failed to get lock for " + myLock.getName());
            }
        }

        /**
         * What we're wrapped around.
         */
        private final LockManagerLock myLock;

        /**
         * Whether the lock is exclusive or not.
         */
        private final boolean myExclusive;

        /**
         * CTOR.
         */
        private SafeLock(final LockManagerLock lock, final boolean isExclusive)
        {
            myLock      = lock;
            myExclusive = isExclusive;
        }

        /**
         * {@inheritDoc}
         *
         * @throws DeadlockException if acquiring the lock would result in deadlock.
         */
        @Override
        public void lock()
        {
            LockManager.this.lock(myLock, myExclusive);
        }

        /**
         * {@inheritDoc}
         *
         * <b>Not supported.</b>
         */
        @Override
        public void lockInterruptibly()
            throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryLock()
        {
            return LockManager.this.tryLock(myLock, myExclusive);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException
        {
            return LockManager.this.tryLock(myLock, myExclusive, time, unit);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalMonitorStateException If the current thread does not hold
         *                                      this lock.
         */
        @Override
        public void unlock()
        {
            LockManager.this.unlock(myLock, myExclusive);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Condition newCondition()
        {
            return myLock.newCondition(myExclusive);
        }

        /**
         * A synonym for {@link #lock()} which returns the instance back. This
         * is intended for use in an {@link AutoCloseable} try-with-resources
         * context.
         *
         * @return the safe-lock which was acquired.
         *
         * @throws DeadlockException if acquiring the lock would result in deadlock.
         */
        public SafeLock acquire()
        {
            LockManager.this.lock(myLock, myExclusive);
            return this;
        }

        /**
         * A synonym for {@link #tryLock()} which returns the instance back.
         * This is intended for use in an {@link AutoCloseable}
         * try-with-resources context:
         * <pre>
         *     try (SafeLock l = lock.tryAcquire()) {
         *         do things...
         *     }
         *     catch (AcquireFailedException e) {
         *         do other things...
         *     }
         * </pre>
         * We could return {@code null} upon failure here but that would mean
         * that, unless the user explicitly checks the value of {@code l} is not
         * {@code null} then they might assume that they got the lock. This
         * method, instead, provides a simple equivalent to the {@code if...else}
         * idiom of the sibling {@code tryLock()} method.
         *
         * @return the safe-lock which was acquired.
         *
         * @throws AcquireFailedException if the lock wasn't acquired.
         */
        public SafeLock tryAcquire()
            throws AcquireFailedException
        {
            if (LockManager.this.tryLock(myLock, myExclusive)) {
                return this;
            }
            else {
                throw new AcquireFailedException();
            }
        }

        /**
         * A synonym for {@link #tryLock(long,TimeUnit)} which returns the
         * instance back. This is intended for use in an {@link AutoCloseable}
         * try-with-resources context:
         * <pre>
         *     try (SafeLock l = lock.tryAcquire(1, TimeUnit.SECONDS)) {
         *         do things...
         *     }
         *     catch (AcquireFailedException | InterruptedException e) {
         *         do other things...
         *     }
         * </pre>
         * We could return {@code null} upon failure here but that would mean
         * that, unless the user explicitly checks the value of {@code l} is not
         * {@code null} then they might assume that they got the lock. This
         * method, instead, provides a simple equivalent to the {@code if...else}
         * idiom of the sibling {@code tryLock()} method.
         *
         * @param time  How long to wait before timing out.
         * @param unit  The units of {@code time}.
         *
         * @return the safe-lock which was acquired.
         *
         * @throws AcquireFailedException if the lock wasn't acquired.
         * @throws InterruptedException   if the acquisition was interrupted.
         */
        public SafeLock tryAcquire(long time, TimeUnit unit)
            throws AcquireFailedException,
                   InterruptedException
        {
            if (LockManager.this.tryLock(myLock, myExclusive, time, unit)) {
                return this;
            }
            else {
                throw new AcquireFailedException();
            }
        }

        /**
         * Unlock this lock. This is provided for use in a {@link AutoCloseable}
         * try-with-resources context.
         *
         * @throws IllegalMonitorStateException If the current thread does not hold
         *                                      this lock.
         */
        @Override
        public void close()
        {
            unlock();
        }

        /**
         * Whether this is an exclusive lock or, else, a shared one.
         *
         * @return whether it is exclusive.
         */
        public boolean isExclusive()
        {
            return myExclusive;
        }

        /**
         * Whether the named lock is held by the current thread.
         *
         * @return whether it is held.
         */
        public boolean isHeldByCurrentThread()
        {
            return myLock.isHeldByCurrentThread(myExclusive);
        }

        /**
         * Set the log level associated with this lock, to enable lock
         * debugging.
         *
         * @param level  The new level.
         */
        public void setLogLevel(final Level level)
        {
            myLock.setLogLevel(level);
        }
    }

    /**
     * The "safe" version of a read-write lock pair.
     *
     * <p>In the semantics of this class a read lock is a shared lock, and a
     * write lock is an exclusive lock.
     */
    public static class SafeReadWriteLock
        implements ReadWriteLock
    {
        /**
         * The read (shared) lock.
         */
        private final SafeLock myReadLock;

        /**
         * The write (exclusive) lock.
         */
        private final SafeLock myWriteLock;

        /**
         * CTOR.
         */
        private SafeReadWriteLock(final SafeLock readLock,
                                  final SafeLock writeLock)
        {
            myReadLock  = readLock;
            myWriteLock = writeLock;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SafeLock readLock()
        {
            return myReadLock;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SafeLock writeLock()
        {
            return myWriteLock;
        }

        /**
         * Get the shared lock.
         *
         * <p>This is a semantic sugar method which is a synonym for
         * {@link #readLock()}.
         *
         * @return the shared lock.
         */
        public SafeLock sharedLock()
        {
            return readLock();
        }

        /**
         * Get the exclusive lock.
         *
         * <p>This is a semantic sugar method which is a synonym for
         * {@link #writeLock()}.
         *
         * @return the exclusive lock.
         */
        public SafeLock exclusiveLock()
        {
            return writeLock();
        }
    }

    /**
     * The state of a thread's locks. This can be used to remember what a
     * particular thread holds at any one time and, at a later point, to release
     * any locks acquired in the interim. It may be used in a try-with-resources
     * context or standalone.
     *
     * <p>For example:<pre>
     *     // Acquire Lock1 and Lock2
     *     lm.lock("Lock1");
     *     lm.lock("Lock2");
     *
     *     // At this point we have 1 lock on each of Lock1 and Lock2
     *     try (ThreadLockState state = lm.saveLockState()) {
     *         lm.lock("Lock2");
     *         lm.lock("Lock3");
     *         // We now have 1 lock on Lock1 and Lock3 and 2 locks on Lock2
     *     }
     *     // Upon exiting the try block we now are back to 1 lock on Lock1 and Lock2
     * </pre>
     *
     * <p>Note that, if any locks have been freed in the interim, such that
     * restoring the locking state would actually entail <i>acquiring</i> locks,
     * then this is considered an illegal state. This is to catch programming
     * errors.
     */
    public class ThreadLockState
        implements AutoCloseable
    {
        /**
         * The details of a lock which we hold.
         */
        public class Details
        {
            /**
             * The entry in the map which we correspond to.
             */
            private final Map.Entry<LockManagerLock,int[]> myEntry;

            /**
             * CTOR.
             *
             * @param entry  The entry in the map which we correspond to.
             */
            protected Details(final Map.Entry<LockManagerLock,int[]> entry)
            {
                myEntry = entry;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                final StringBuilder sb = new StringBuilder();
                sb.append(getName()).append('[');
                sb.append("EXCL:").append(numExclusive()).append(',');
                sb.append("SHRD:").append(numShared());
                sb.append(']');
                return sb.toString();
            }

            /**
             * Get the name of the lock.
             *
             * @return the name.
             */
            public String getName()
            {
                return myEntry.getKey().getName();
            }

            /**
             * Get the number of exclusive holds this thread has on the lock.
             *
             * @return the number of exclusive holds.
             */
            public int numExclusive()
            {
                return myEntry.getValue()[0];
            }

            /**
             * Get the number of shared holds this thread has on the lock.
             *
             * @return the number of shared holds.
             */
            public int numShared()
            {
                return myEntry.getValue()[1];
            }
        }

        /**
         * The thread associated with this state.
         */
        private final Thread myThread;

        /**
         * Mapping from lock to {@code (exclusive,shared)} lock counts.
         */
        private final Map<LockManagerLock,int[]> myLockCounts;

        /**
         * CTOR. Only called by {@code LockManager#saveLockState().}

         * @param thread  The thread.
         * @param counts  The locks and their counts.
         */
        protected ThreadLockState(final Thread thread,
                                  final Map<LockManagerLock,int[]> counts)
        {
            myThread     = thread;
            myLockCounts = counts;
        }

        /**
         * Restore the lock state.
         *
         * @throws IllegalMonitorStateException if we would have to acquire a
         *                                      lock in order to restore the
         *                                      state correctly.
         */
        public void restore()
            throws IllegalMonitorStateException
        {
            restoreLockState(this);
        }

        /**
         * Filter out any locks for which the given {@code acceptor} predicate
         * returns {@code false}. A {@code null} acceptor will be ignored.
         *
         * <p>The {@code acceptor} should not retain any {@link Details} pointer
         * outside the duration of its {@code test()} invocation.
         *
         * @param acceptor  How to filter.
         *
         * @return whether the collection of locks changed at all.
         */
        public boolean filter(final Predicate<Details> acceptor)
        {
            // NOP
            if (acceptor == null) {
                return false;
            }

            // What we will return
            boolean changed = false;

            // Walk and check
            final Iterator<Map.Entry<LockManagerLock,int[]>> itr =
                myLockCounts.entrySet().iterator();
            while (itr.hasNext()) {
                final Details details = new Details(itr.next());
                if (!acceptor.test(details)) {
                    if (getLogger().isLoggable(Level.FINEST)) {
                        getLogger().finest("Removing entry " + details);
                    }
                    itr.remove();
                    changed = true;
                }
                else if (getLogger().isLoggable(Level.FINEST)) {
                    getLogger().finest("Keeping entry " + details);
                }
            }

            // Give back whether we changed the collection at all
            return changed;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
        {
            restore();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append(getThread().getName()).append('{');
            String comma = "";
            for (Map.Entry<LockManagerLock,int[]> entry :
                     getLockCounts().entrySet())
            {
                sb.append(comma);
                sb.append(new Details(entry));
                comma = ",";
            }
            sb.append('}');
            return sb.toString();
        }

        /**
         * Get the thread associated with this state.
         *
         * @return the thread.
         */
        protected Thread getThread()
        {
            return myThread;
        }

        /**
         * Get the lock counts.
         *
         * @return the counts.
         */
        protected Map<LockManagerLock,int[]> getLockCounts()
        {
            return myLockCounts;
        }
    }

    /**
     * A {@link List} implementation tagged with a "colour". This basically
     * allows us to mark the list in some way.
     */
    protected static class ColouredList<T>
        extends ArrayList<T>
    {
        /**
         * The colour all instances start with. The result of getNextColour()
         * must never return this else we might accidently "match".
         */
        private static final int START_COLOUR = 0;

        /**
         * The next "colour" from a global stash.
         *
         * Technically we want all the colours to be unique. However, since we
         * are going to visit all the instances of this class in lockWalksTo()
         * it probably just needs to be a different value from the previous one.
         * Given that, 2^32-1 different values should suffice (and will be
         * aligned on a word boundary in the instance).
         */
        private static int ourNextColour = START_COLOUR + 1;

        /**
         * The colour of our list. This starts off at a value which will never
         * match the result of getNextColour().
         */
        private int myColour = START_COLOUR;

        /**
         * Get the next colour to use for colouring.
         *
         * Should only be called under the state lock.
         *
         * @return The next value.
         */
        public static int getNextColour()
        {
            final int result = ourNextColour++;
            if (ourNextColour == START_COLOUR) {
                ourNextColour++;
            }
            return result;
        }

        /**
         * CTOR with capacity.
         *
         * @param capacity  The desired capacity.
         */
        public ColouredList(int capacity)
        {
            super(capacity);
        }

        /**
         * Set the colour of our list, returning true if it was different from
         * the previous colour.
         *
         * @param colour  The colour of the list.
         *
         * @return whether the colour was changed.
         */
        public boolean setColour(int colour)
        {
            final boolean result = (colour != myColour);
            myColour = colour;
            return result;
        }
    }

    /**
     * A lock which has all the functions required by the LockManager.
     */
    protected static interface LockManagerLock
    {
        /**
         * Details of a possible holder of a lock, and how.
         *
         * <p>This is something which is keyed by the locking thread and the
         * lock type (exclusive or shared). It also carries state, denoting
         * whether the lock is actually held by the thread.
         */
        public static class Locker
        {
            /**
             * The locking thread. This makes part of the Locker "key".
             */
            private Thread myThread = null;

            /**
             * The type of lock being held; exclusive or, else, shared. This
             * makes part of the Locker "key".
             */
            private boolean myExclusive = false;

            /**
             * Whether the lock is actually being held.
             */
            private boolean myHolding = false;

            /**
             * The stack-traces of each acquire for this lock. Mainly for debugging.
             */
            private final List<Throwable> myLockStackTraces =
                ourLogLockStackTraces ? new ArrayList<>() : null;

            /**
             * Get the locking thread.
             *
             * @return the thread.
             */
            public Thread getThread()
            {
                return myThread;
            }

            /**
             * Get the if type of lock being held is exclusive or, else, shared.
             *
             * @return whether the lock is held exclusively.
             */
            public boolean isExclusive()
            {
                return myExclusive;
            }

            /**
             * Set the thread and lock type.
             *
             * @param thread       The thread to set with.
             * @param isExclusive  Whether the lock is held exclusively.
             */
            public void set(final Thread thread, final boolean isExclusive)
            {
                myThread    = thread;
                myExclusive = isExclusive;
            }

            /**
             * Set the thread and lock type, and whether it's being held.
             *
             * @param thread       The thread to set with.
             * @param isExclusive  Whether the lock is held exclusively.
             * @param isHolding    Whether the lock is actually held.
             */
            public void set(final Thread  thread,
                            final boolean isExclusive,
                            final boolean isHolding)
            {
                myThread    = thread;
                myExclusive = isExclusive;
                myHolding   = isHolding;
            }

            /**
             * Make like new.
             */
            public void clear()
            {
                myThread    = null;
                myExclusive = false;
                myHolding   = false;
                if (myLockStackTraces != null) {
                    myLockStackTraces.clear();
                }
            }

            /**
             * Get whether the lock is actually being held.
             *
             * @return whether the lock is held.
             */
            public boolean isHolding()
            {
                return myHolding;
            }

            /**
             * Set whether the lock is being held.
             *
             * @param isHolding  Whether the lock is held.
             */
            public void setHolding(final boolean isHolding)
            {
                myHolding = isHolding;
            }

            /**
             * If we are tracking lock stack-traces then record one.
             */
            public void logLock()
            {
                if (myLockStackTraces != null) {
                    myLockStackTraces.add(new Throwable(myThread.getName()));
                }
            }

            /**
             * Whether this instance matches the given thread and lock type.
             *
             * @param thread       The thread to check against.
             * @param isExclusive  The type of holding.
             *
             * @return Whether the details match.
             */
            public boolean matches(final Thread  thread,
                                   final boolean isExclusive)
            {
                return (myThread.equals(thread) && myExclusive == isExclusive);
            }

            /**
             * Get the lock stack-traces, if any.
             *
             * @return the stack-traces, if any.
             */
            public List<Throwable> getLockStackTraces()
            {
                return myLockStackTraces;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                return "'" + getThread().getName() + "'[" +
                    (isHolding()   ? "HOLDS" : "WANTS") + ":" +
                    (isExclusive() ? "EXCL"  : "SHRD" ) +
                    (myLockStackTraces == null
                     ? ""
                     : "\n" + myLockStackTraces.stream()
                                              .map(StringUtil::stackTraceToString)
                                              .collect(Collectors.joining("\n"))) +
                "]";
            }
        }

        /**
         * Get the name of the lock.
         *
         * @return the lock name.
         */
        public String getName();

        /**
         * Acquire this lock in an exclusive or shared manner.
         *
         * @param isExclusive  Whether to lock exclusively.
         */
        public void lock(final boolean isExclusive);

        /**
         * Attempt to acquire this lock in an exclusive or shared manner.
         *
         * @param isExclusive  Whether to lock exclusively.
         *
         * @return whether the lock was acquired.
         */
        public boolean tryLock(final boolean isExclusive);

        /**
         * Release a lock.
         *
         * @param isExclusive  Whether to release a lock which was acquired
         *                     exclusively.
         */
        public void unlock(final boolean isExclusive);

        /**
         * Returns a new {@link Condition} instance that is bound to this
         * instance, for the exclusive or shared lock.
         *
         * @param isExclusive  Whether to lock exclusively.
         *
         * @return the new condition.
         */
        public Condition newCondition(final boolean isExclusive);

        /**
         * Whether this lock is held by the current thread in an exclusive
         * or shared manner.
         *
         * @param isExclusive  Whether the check is for an exclusive lock.
         *
         * @return whether the lock is held.
         */
        public boolean isHeldByCurrentThread(final boolean isExclusive);

        /**
         * Get the hold count for this thread, either in an exclusive
         * or shared manner.
         *
         * @param isExclusive  Whether the check is for an exclusive lock.
         *
         * @return how many times this thread holds the lock.
         */
        public int getHoldCountForCurrentThread(final boolean isExclusive);

        /**
         * Get the list of threads which are holding, or trying to acquire, this
         * lock. This should only be accessed under the state lock.
         *
         * @return the list of lockers.
         */
        public ColouredList<Locker> getLockers();

        /**
         * Add a thread which will be locking this lock. This should only be
         * called under the state lock.
         *
         * <p>Callers should take care not to add any {@code (Thread,LockType)}
         * more than once. This will not be checked here.
         *
         * @param thread       The thread to add.
         * @param isHolding    Whether the thread is already holding the lock.
         * @param isExclusive  Whether the lock is held exclusively.
         */
        public default void addLocker(final Thread  thread,
                                      final boolean isHolding,
                                      final boolean isExclusive)
        {
            final Locker locker = ourLockerPool.getInstance();
            locker.set(thread, isExclusive, isHolding);
            locker.logLock();
            getLockers().add(locker);
        }

        /**
         * Mark this thread as actually holding the lock. This should only be
         * called under the state lock.
         *
         * @param thread       The thread to mark with.
         * @param isExclusive  Whether the lock is held exclusively.
         *
         * @return whether the call was successful.
         */
        public default boolean setLockerHolding(final Thread  thread,
                                                final boolean isExclusive)
        {
            final List<Locker> lockers = getLockers();
            for (int i=0; i < lockers.size(); i++) {
                final Locker locker = lockers.get(i);
                if (locker.matches(thread, isExclusive)) {
                    locker.setHolding(true);
                    locker.logLock();
                    return true;
                }
            }
            return false;
        }

        /**
         * Remove the given thread as a locker. This should only be called under
         * the state lock.
         *
         * @param thread       The thread to unmark with.
         * @param isExclusive  Whether the lock is held exclusively.
         *
         * @return whether the call was successful.
         */
        public default boolean removeLocker(final Thread thread,
                                            final boolean isExclusive)
        {
            final List<Locker> lockers = getLockers();
            int i;
            for (i=0; i < lockers.size(); i++) {
                final Locker locker = lockers.get(i);
                if (locker.matches(thread, isExclusive)) {
                    break;
                }
            }
            if (i < lockers.size()) {
                ourLockerPool.releaseInstance(lockers.remove(i));
                return true;
            }
            else {
                return false;
            }
        }

        /**
         * Whether the given thread is blocked waiting to acquire this lock.
         * This should only be called under the state lock.
         *
         * @param thread  The thread to check.
         *
         * @return whether the thread is blocked.
         */
        public default boolean isThreadAwaiting(final Thread thread)
        {
            // Null threads can't be waiting
            if (thread == null) {
                return false;
            }

            // Look for all the occurances of this thread, if it is marked as
            // not actually holding the lock then it is waiting on it. We have
            // to do an exhaustive search until we find unheld instances since
            // we could be locking the lock either shared or exclusively.
            boolean awaiting = false;
            final List<Locker> lockers = getLockers();
            for (int i=0; i < lockers.size(); i++) {
                final Locker locker = lockers.get(i);
                if (thread.equals(locker.getThread()) && !locker.isHolding()) {
                    awaiting = true;
                    break;
                }
            }
            return awaiting;
        }

        /**
         * Get the log level associated with this lock, to enable lock
         * debugging.
         *
         * @return the logging level.
         */
        public Level getLogLevel();

        /**
         * Set the log level associated with this lock, to enable lock
         * debugging.
         *
         * @param level  The new logging level.
         */
        public void setLogLevel(final Level level);
    }
    /** Pool for the Locker instances -- for use in the above only. */
    private static final Pool<LockManagerLock.Locker> ourLockerPool =
        new SwappingPool<>(1024, 8) {
            @Override protected void prepareForReuse(LockManagerLock.Locker locker) {
                // Release any references held etc.
                locker.clear();
            }
            @Override protected LockManagerLock.Locker newInstance() {
                return new LockManagerLock.Locker();
            }
        };

    /**
     * A re-entrant lock with a name.
     */
    private static class NamedReentrantLock
        extends    ReentrantReadWriteLock
        implements LockManagerLock
    {
        /**
         * Our name.
         */
        private final String myName;

        /**
         * Our list of threads which hold us, or are trying to acquire us, and
         * how they hold us.
         */
        private final ColouredList<Locker> myLockers;

        /**
         * Our logging level.
         */
        private volatile Level myLogLevel;

        /**
         * CTOR
         */
        public NamedReentrantLock(final CharSequence name)
        {
            if (name == null) {
                throw new NullPointerException("Given a null name");
            }
            myName     = name.toString();
            myLockers  = new ColouredList<>(4);
            myLogLevel = Level.FINEST;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getName()
        {
            return myName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void lock(final boolean isExclusive)
        {
            if (isExclusive) {
                writeLock().lock();
            }
            else {
                readLock().lock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryLock(final boolean isExclusive)
        {
            if (isExclusive) {
                return writeLock().tryLock();
            }
            else {
                if (readLock().tryLock()) {
                    return true;
                }
                else {
                    return false;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void unlock(final boolean isExclusive)
        {
            if (isExclusive) {
                writeLock().unlock();
            }
            else {
                readLock().unlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Condition newCondition(final boolean isExclusive)
        {
            return (isExclusive ? writeLock() : readLock()).newCondition();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isHeldByCurrentThread(final boolean isExclusive)
        {
            return getHoldCountForCurrentThread(isExclusive) > 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getHoldCountForCurrentThread(final boolean isExclusive)
        {
            return isExclusive ? getWriteHoldCount() : getReadHoldCount();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ColouredList<Locker> getLockers()
        {
            return myLockers;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Level getLogLevel()
        {
            return myLogLevel;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setLogLevel(final Level level)
        {
            if (level == null) {
                throw new NullPointerException("Given a null level");
            }
            myLogLevel = level;
        }

        /**
         * {@inheritDoc}
         *
         * <p>This should only be called under the synchronized(LockManager.this)
         * lock since it references myLockers.
         */
        @Override
        public String toString()
        {
            // We add the name to the parent's toString() information. We don't
            // cache this since what the parent's method can return may change
            // depending on if it's held or not.
            final StringBuilder sb = new StringBuilder();
            sb.append(myName).append(':')
              .append(super.toString())
              .append("[Lockers = ")
              .append(myLockers.stream()
                               .map(Locker::toString)
                               .collect(Collectors.joining(",")))
              .append(']');
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------ //

    /**
     * Whether to log stacktraces of locks. Mostly for debugging.
     */
    private static final boolean ourLogLockStackTraces;
    static {
        final String prop =
            "com.deshaw.util.concurrent.LockManager.logStackTraces";
        try {
            ourLogLockStackTraces =
                StringUtil.parseBoolean(System.getProperty(prop, "false"));
        }
        catch (Exception e) {
            throw new RuntimeException("Bad value for " + prop, e);
        }
    }

    /**
     * The StringBuilder used to track calls to lockWalksTo(), if any.
     */
    private static final ThreadLocalStringBuilder ourLockWalksToSb =
        ourLogLockStackTraces ? new ThreadLocalStringBuilder(1024) : null;

    // Instrumentation
    private static final Instrumentor ourGetNamedLockInstr;
    private static final Instrumentor ourLockInstr;
    private static final Instrumentor ourLockWalksToInstr;
    private static final Instrumentor ourTryLockTUInstr;
    private static final Instrumentor ourTryLockInstr;
    private static final Instrumentor ourUnlockInstr;
    static {
        ourGetNamedLockInstr = INSTRUMENTOR_FACTORY.getInstance("LockManager#getNamedLock()");
        ourLockInstr         = INSTRUMENTOR_FACTORY.getInstance("LockManager#lock()");
        ourLockWalksToInstr  = INSTRUMENTOR_FACTORY.getInstance("LockManager#lockWalksTo()");
        ourTryLockTUInstr    = INSTRUMENTOR_FACTORY.getInstance("LockManager#tryLock(time,unit)");
        ourTryLockInstr      = INSTRUMENTOR_FACTORY.getInstance("LockManager#tryLock()");
        ourUnlockInstr       = INSTRUMENTOR_FACTORY.getInstance("LockManager#unlock()");

        // We get a lot of these
        ourLockInstr  .setIntervalMod(1000);
        ourUnlockInstr.setIntervalMod(1000);
    }

    // ------------------------------------------------------------------ //

    /**
     * User-defined locks. These allow clients to coordinate with one another
     * since we are effectively dealing with a multi-threaded application
     */
    private final ConcurrentMap<CharSequence,LockManagerLock> myNamedLocks;

    // We effectively two maps, these are from threads to locks and locks to
    // threads. Together they form a dependency graph which we will walk in
    // lockWalksTo(). It's that walk which we perform in order to detect
    // deadlock. The lock-threads map is actually done by associating the
    // threads with the lock instance itself (in getLockers()).
    //
    // We use Lists since, though their remove() operation is potentially slow,
    // it's fast to iterate over them. Also, we expect the lists to be
    // relatively short which makes operations on them slightly quicker than
    // using Sets. Since we're using Lists and not Sets it's important that we
    // only add to them if the element is not there already.

    /**
     * Mapping from threads to locks which they hold or locks they are
     * trying to acquire.
     *
     * This should only be accessed under synchronized(this).
     */
    private final Map<Thread,ColouredList<LockManagerLock>> myThreadToLocks;

    /**
     * An empty lock state for the "current" thread which, for example, can be
     * used to drop all the thread locks.
     */
    private final ThreadLockState myEmptyLockState =
        new ThreadLockState(null, Collections.emptyMap()) {
            @Override protected Thread getThread() {
                return currentThread();
            }
        };

    // ------------------------------------------------------------------ //

    /**
     * CTOR.
     */
    public LockManager()
    {
        myNamedLocks    = new ConcurrentHashMap<>();
        myThreadToLocks = new HashMap<>();
    }

    /**
     * Get an exclusive {@link SafeLock} instance for a lock of a given name.
     *
     * <p>The implementation returned will perform all its locking operations via
     * the lock manager.
     *
     * @param lockName  The name of the lock.
     *
     * @return the lock.
     *
     * @throws UnsupportedOperationException if the lock type is not supported.
     */
    public SafeLock getExclusiveLockFor(final CharSequence lockName)
        throws UnsupportedOperationException
    {
        return new SafeLock(getNamedLock(lockName), true);
    }

    /**
     * Get a shared {@link SafeLock} instance for a lock of a given name.
     *
     * <p>The implementation returned will perform all its locking operations via
     * the lock manager.
     *
     * @param lockName  The name of the lock.
     *
     * @return the lock.
     *
     * @throws UnsupportedOperationException if the lock type is not supported.
     */
    public SafeLock getSharedLockFor(final CharSequence lockName)
        throws UnsupportedOperationException
    {
        return new SafeLock(getNamedLock(lockName), false);
    }

    /**
     * Get a read-write version of a {@link SafeLock} instance for a lock of a
     * given name. For out purposes "read" means "shared" and "write" means
     * "exclusive".
     *
     * <p>The implementation returned will perform all its locking operations via
     * the lock manager.
     *
     * @param lockName  The name of the lock.
     *
     * @return the lock.
     */
    public SafeReadWriteLock getReadWriteLockFor(final CharSequence lockName)
    {
        return new SafeReadWriteLock(getSharedLockFor   (lockName),
                                     getExclusiveLockFor(lockName));
    }

    /**
     * Drop any locks held by the current thread, returning whether any were
     * dropped or not.
     *
     * @return any dropped.
     */
    public boolean dropAllThreadLocks()
    {
        // Drop all the locks by "restoring" an empty state
        if (getLogger().isLoggable(Level.FINEST)) {
            getLogger().finest("Releasing all locks");
        }
        return restoreLockState(myEmptyLockState);
    }

    /**
     * Save the lock state for this thread.
     *
     * @return the lock state.
     */
    public ThreadLockState saveLockState()
    {
        // What we'll build up. This is a map from lock to (exclusive,shared)
        // counts.
        final Map<LockManagerLock,int[]> counts;

        // The thread which we are working for
        final Thread me = currentThread();

        // We need to do this under the lock, since we are accessing our locks'
        // state
        synchronized(this) {
            final ColouredList<LockManagerLock> locks = myThreadToLocks.get(me);
            if (locks != null && !locks.isEmpty()) {
                // We have data to build up the map with
                counts = new HashMap<>();
                for (LockManagerLock lock : locks) {
                    final int exclusive = lock.getHoldCountForCurrentThread(true);
                    final int shared    = lock.getHoldCountForCurrentThread(false);
                    if (exclusive > 0 || shared > 0) {
                        counts.put(lock, new int[] { exclusive, shared });
                    }
                }
            }
            else {
                // No locks means no counts
                counts = Collections.emptyMap();
            }
        }

        // Finally we can give back the state
        final ThreadLockState state = new ThreadLockState(me, counts);
        if (getLogger().isLoggable(Level.FINEST)) {
            getLogger().finest("Created state: " + state);
        }
        return state;
    }

    /**
     * Restore the lock state for this thread. A {@code null} state is ignored.
     *
     * @param state  The state to restore with.
     *
     * @return whether any locks were dropped.
     *
     * @throws IllegalArgumentException     if we were given a state for a
     *                                      different thread.
     * @throws IllegalMonitorStateException if we would have to acquire a lock
     *                                      in order to restore the state
     *                                      correctly.
     */
    public boolean restoreLockState(final ThreadLockState state)
        throws IllegalArgumentException,
               IllegalMonitorStateException
    {
        // Just ignore a null state
        if (state == null) {
            return false;
        }
        if (getLogger().isLoggable(Level.FINER)) {
            getLogger().finer("Restoring state: " + state);
        }

        // Should be the same thread
        final Thread me = currentThread();
        if (!me.equals(state.getThread())) {
            throw new IllegalArgumentException(
                "Given a state for a different thread: " +
                me + " " + state.getThread()
            );
        }

        // Say what we are restoring to. We copy this so that we may mutate it.
        final Map<LockManagerLock,int[]> stateLockCounts =
            new HashMap<>(state.getLockCounts());

        // What we will return
        boolean anyDropped = false;

        // Need to go under the lock for this since we are accessing the
        // collective lock state
        synchronized(this) {
            // The locks which this thread currently holds
            final ColouredList<LockManagerLock> threadLocks =
                myThreadToLocks.get(me);

            // Ensure that we are not trying to acquire locks
            if (threadLocks == null) {
                if (!stateLockCounts.isEmpty()) {
                    throw new IllegalMonitorStateException(
                        "Hold no locks but restoring state: " + state
                    );
                }
                else {
                    // Nothing to do
                    return false;
                }
            }

            // Walk through and drop the locks held
            final Iterator<LockManagerLock> lockItr = threadLocks.iterator();
            while (lockItr.hasNext()) {
                // Move on
                final LockManagerLock lock = lockItr.next();

                // Desired counts. It's possible for us to be holding a lock now
                // which we were not before and so there may be no counts for it
                // in the state.
                final int[] pair = stateLockCounts.remove(lock);
                final int wantNumExcl = (pair == null) ? 0 : pair[0];
                final int wantNumShrd = (pair == null) ? 0 : pair[1];

                // Actual counts
                final int curNumExcl = lock.getHoldCountForCurrentThread(true);
                final int curNumShrd = lock.getHoldCountForCurrentThread(false);

                // Checks
                if (curNumExcl < wantNumExcl) {
                    throw new IllegalMonitorStateException(
                        "Restoring " + wantNumExcl + " exclusive lock(s) " +
                        "but currently hold " + curNumExcl + " in " + state
                    );
                }
                if (curNumShrd < wantNumShrd) {
                    throw new IllegalMonitorStateException(
                        "Restoring " + wantNumShrd + " shared lock(s) " +
                        "but currently hold " + curNumShrd + " in " + state
                    );
                }

                // Now we can drop
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer(
                        lock.getName() + " " +
                        "EXCL: " + curNumExcl + " -> " + wantNumExcl + "; " +
                        "SHRD: " + curNumShrd + " -> " + wantNumShrd
                    );
                }
                for (int i = curNumExcl; i > wantNumExcl; i--) {
                    lock.unlock(true);
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Dropped exclusive lock on " + lock);
                    }
                }
                for (int i = curNumShrd; i > wantNumShrd; i--) {
                    lock.unlock(false);
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Dropped shared lock on " + lock);
                    }
                }

                // If we dropped all the locks then ensure that the lock does
                // not know this thread as a locker still
                if (wantNumExcl <= 0 && curNumExcl > 0 &&
                    !lock.removeLocker(me, true))
                {
                    throw new IllegalStateException(
                        "Failed to remove exclusive holding thread " +
                        "'" + me.getName() + "' for lock: " + lock
                    );
                }
                if (wantNumShrd <= 0 && curNumShrd > 0 &&
                    !lock.removeLocker(me, false))
                {
                    throw new IllegalStateException(
                        "Failed to remove shared holding thread " +
                        "'" + me.getName() + "' for lock: " + lock
                    );
                }

                // Is the thread still holding this lock? If not we remove the
                // entry from the thread's list.
                if (wantNumExcl == 0 && wantNumShrd == 0) {
                    lockItr.remove();
                }

                // Remember if we dropped any
                anyDropped |= ((wantNumExcl < curNumExcl) ||
                               (wantNumShrd < curNumShrd));
            }

            // Any locks still held?
            if (threadLocks.isEmpty()) {
                // No, so remove the mapping
                myThreadToLocks.remove(me);
            }
        }

        // Finally, we should have handled all the state
        if (!stateLockCounts.isEmpty()) {
            throw new IllegalArgumentException(
                "Locks not currently held when restoring " + state + ": " +
                stateLockCounts
            );
        }

        // Say if we dropped any at all
        return anyDropped;
    }

    // ------------------------------------------------------------------ //

    /**
     * Get the logger for this instance.
     *
     * @return the logger.
     */
    protected Logger getLogger()
    {
        return LOG;
    }

    /**
     * Create a named LockManagerLock instance. May be overridden by subclasses
     * which want to provide different implementations.
     *
     * @param name  The name for the lock.
     *
     * @return the newly-created lock.
     */
    protected LockManagerLock newNamedLock(final CharSequence name)
    {
        return new NamedReentrantLock(name);
    }

    /**
     * Returns the current thread which calls this method.
     *
     * <p>Note that, in some cases where the semantics of the LockManagerLock
     * implementations are special, this might not be what you'd get back from
     * {@code Thread.currentThread()}. In which case subclasses should override
     * this method.
     *
     * @return the current thread, per the class's semantics.
     */
    protected Thread currentThread()
    {
        return Thread.currentThread();
    }

    // ------------------------------------------------------------------ //

    /**
     * Get a named lock. This allows external clients to get a lock which they
     * may use to coordinate multiple instances of themselves.
     *
     * @param name The handle which we'll use for lookups.
     *
     * @return The named LockManagerLock instance, never null.
     */
    private LockManagerLock getNamedLock(CharSequence name)
    {
        final long start = ourGetNamedLockInstr.start();
        try {
            if (name == null) {
                throw new NullPointerException("Given a null lock name");
            }

            // Save a bit of time and garbage
            if (!(name instanceof StringUtil.HashableSubSequence)) {
                name = name.toString();
            }

            // Look for it
            LockManagerLock lock = myNamedLocks.get(name);
            if (lock == null) {
                // It doesn't yet exist so create it, handling the race
                // condition case
                lock = newNamedLock(name);
                final LockManagerLock previous =
                    myNamedLocks.putIfAbsent(name.toString(), lock);
                if (previous == null) {
                    // We were the first, say so
                    getLogger().finer("Created named lock \"" + name + "\"");
                }
                else {
                    // We hit the race condition and someone else created it
                    // before us, use theirs
                    lock = previous;
                }
            }

            // Give back our final result
            return lock;
        }
        finally {
            ourGetNamedLockInstr.end(start);
        }
    }

    /**
     * Attempt to acquire a named lock if no-one else is holding it, creating
     * the lock if needbe.
     *
     * @param lock         The lock to acquire.
     * @param isExclusive  Whether the lock should be acquired exclusively.
     *
     * @return whether the lock was successfully acquired.
     */
    private boolean tryLock(final LockManagerLock lock, final boolean isExclusive)
    {
        final String how = isExclusive ? "exclusive" : "shared";
        final long start = ourTryLockInstr.start();
        try {
            // By whom
            final Thread me = currentThread();

            // Acquire under the global lock
            synchronized(this) {
                // For the devlopers out there
                if (getLogger().isLoggable(Level.FINEST)) {
                    getLogger().log(
                        Level.FINEST,
                        "Looking to acquire " + how + " " + lock,
                        new Throwable("Call tree")
                    );
                }
                else if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Looking to acquire " + how + " " + lock);
                }

                // If we're already holding the lock then it's trivial. It's
                // _important_ that we make this check since we mustn't add this
                // lock-and-thread pair to the state more than once.
                if (lock.isHeldByCurrentThread(isExclusive)) {
                    // Just lock it again and we're done
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Already " + how + "-holding " + lock);
                    }
                    lock.lock(isExclusive);
                    return true;
                }

                // Attempt to acquire it, this can never result in deadlock if
                // no-one else is holding it
                if (!lock.tryLock(isExclusive)) {
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Failed to acquire " + how + " " + lock);
                    }
                    return false;
                }
                else if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Acquired " + how + " " + lock);
                }

                // Since we acquired it we need to update our lock state
                ColouredList<LockManagerLock> locks = myThreadToLocks.get(me);
                if (locks == null) {
                    locks = new ColouredList<>(4);
                    myThreadToLocks.put(me, locks);
                }
                locks.add(lock);
                lock .addLocker(me, true, isExclusive);
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer(
                        "Thread '" + me.getName() + "' now holds " +
                        locks.size() + " lock(s):"
                    );
                    for (LockManagerLock l : locks) {
                        getLogger().finer("  " + l);
                    }
                }

                // Log information if the lock wants us to
                if (LOG.isLoggable(lock.getLogLevel())) {
                    LOG.log(lock.getLogLevel(),
                            "Thread '" + me.getName() + "' acquired " +
                            how + " " + lock,
                            new Throwable());
                }

                // Log each time we seem to have a "lot" of locks
                if (getLogger().isLoggable(Level.FINE) && (locks.size() % 25) == 0) {
                    getLogger().log(
                        Level.FINE,
                        "Thread '" + me.getName() + "' now holds " +
                        locks.size() + " lock(s): ",
                        new Throwable()
                    );
                }
            }

            // Got it
            return true;
        }
        finally {
            ourTryLockInstr.end(start);
        }
    }

    /**
     * Attempt to acquire a named lock if no-one else is holding it, creating
     * the lock if needbe and timing out after a given period.
     *
     * @param lock         The lock to acquire.
     * @param isExclusive  Whether the lock should be acquired exclusively.
     * @param time         How long to wait before timing out.
     * @param unit         The units of {@code time}.
     *
     * @return whether the lock was successfully acquired.
     *
     * @throws InterruptedException if the thread was interrupted while waiting
     *                              to acquire the lock.
     */
    private boolean tryLock(final LockManagerLock lock,
                            final boolean isExclusive,
                            final long time, final TimeUnit unit)
        throws InterruptedException
    {
        final String how = isExclusive ? "exclusive" : "shared";
        final long start = ourTryLockTUInstr.start();

        try {
            // Do a speculative try first, before we fall into the retrying
            // logic below
            if (tryLock(lock, isExclusive)) {
                return true;
            }

            // If we didn't get it above and our timeout is never going to be
            // hit then we're done, and we failed
            final long timeNs = unit.toNanos(time);
            if (timeNs <= 0) {
                return false;
            }

            // We want to sleep between attempts to retry the lock acquision
            // below, so as not to thrash the lock manager. This is a bit of a
            // heuristic which we base off the timeout. We'll assume that if
            // folks are willing to wait for a while then sleeping for longer is
            // okay. 999999 is the maximum allowed number for the nanos argument
            // and is almost a millisecond, which seems like a good maximum.
            //
            // If this turns out not to be responsive enough for people then we
            // can add a queue-set of "triers" to the LockManagerLock instance
            // and push this thread onto it while its waiting. Any threads which
            // unlock() can wake it up and, if it's successful, it remove itself
            // from the "triers". We can use LockSupport's park() and unpark()
            // for the sleeping and waking.
            final int sleepNs =
                (int)Math.max(Math.min(timeNs, 100),
                              Math.min(999999, timeNs / 10));

            // Since we failed to speculatively get the lock above we need to
            // keep retrying. We do this by going around and around since we
            // need to hold the lock manager's lock while attempting to acquire
            // the lock's lock. If we block in this we may deadlock the
            // application.
            final long untilNs = System.nanoTime() + timeNs;
            do {
                // Wait for a while and then try again. Note that this means we
                // could, in theory, acquire the lock after going past out
                // "time" limit. However, we assume that users care more about
                // actually getting the lock than they do about being strict
                // about the time limit. (The idea being that the time limit is
                // mostly saying, "Don't wait forever".)
                Thread.sleep(0, sleepNs);
                if (tryLock(lock, isExclusive)) {
                    return true;
                }
            }
            while (System.nanoTime() < untilNs);

            // If we fell out the bottom of the while loop then we didn't get
            // the lock
            if (getLogger().isLoggable(Level.FINER)) {
                getLogger().finer(
                    "Failed to acquire " + how + " " +
                    lock + " after " + time + " " +
                    unit + (time == 1 ? "" : "s")
                );
            }
            return false;
        }
        finally {
            ourTryLockTUInstr.end(start);
        }
    }

    /**
     * Attempt to acquire a named lock, creating if needbe.
     *
     * @param lock         The lock to acquire.
     * @param isExclusive  Whether the lock should be acquired exclusively.
     *
     * @throws DeadlockException if acquiring the lock would result in deadlock.
     */
    private void lock(final LockManagerLock lock, final boolean isExclusive)
        throws DeadlockException
    {
        final String how = isExclusive ? "exclusive" : "shared";
        final long start = ourLockInstr.start();
        try {
            // By whom
            final Thread me = currentThread();

            // We do all this under the global lock for simplicity. This avoids
            // race conditions where we the lock might be released by another
            // user of the same virtual-thread (which is trying to tear itself
            // down). This can happen in PJRmi where you have workers servicing
            // requests and a worked thread might still be associated with the
            // virtual thread even though it's returned control to the Python
            // side. (Then the Python side makes another request using the same
            // virtual thread and we trigger the race condition.)
            synchronized(this) {
                // Hello developers
                if (getLogger().isLoggable(Level.FINEST)) {
                    getLogger().log(
                        Level.FINEST,
                        "Looking to acquire " + how + " " + lock,
                        new Throwable("Call tree")
                    );
                }
                else if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Looking to acquire " + how + " " + lock);
                }

                // If we're already holding the lock then it's trivial. It's
                // _important_ that we make this check since lockWalksTo() relies
                // on being called only when we are not already holding the lock in
                // question. Also we mustn't add this lock-and-thread pair to the
                // state more than once.
                if (lock.isHeldByCurrentThread(isExclusive)) {
                    // Just lock it again and we're done
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Already holding " + how + " " + lock);
                    }
                    lock.lock(isExclusive);
                    return;
                }

                // First, attempt to acquire the lock speculatively, if we can
                // do this then we can avoid doing the deadlock detection. We
                // must do this in the synchronized block since, if we are
                // successful then it's very important that the state reflects
                // that we are now holding the lock.
                final boolean gotLock = lock.tryLock(isExclusive);

                // If we didn't get the lock then see if acquiring it would
                // cause a deadlock
                if (!gotLock) {
                    // Attempting to upgrade a shared lock to an exclusive one
                    // will always fail, and doing so effectively deadlocks the
                    // thread with itself
                    if (isExclusive && lock.isHeldByCurrentThread(false)) {
                        throw new DeadlockException(
                            "Attempt to upgrade lock from shared to exclusive: " +
                            lock
                        );
                    }

                    // Okay, no upgrade so just do a regular deadlock check
                    final long lwtStart = ourLockWalksToInstr.start();
                    try {
                        final StringBuilder sb =
                            (ourLockWalksToSb == null) ? null
                                                       : ourLockWalksToSb.get();
                        if (lockWalksTo(lock, me, ColouredList.getNextColour(), sb)) {
                            // Add the locks which we also hold, to the error
                            // message. This can help the caller figure out what
                            // is going on.
                            final ColouredList<LockManagerLock> locks =
                                myThreadToLocks.get(me);

                            // Dump the current state into the logs. We don't
                            // expect to see deadlocks happen that often and so,
                            // when we do get them; it can be handy to know
                            // what's going on, in addition to the information in
                            // the exception.
                            getLogger().warning(
                                "Deadlock detected in thread " + me + " " +
                                "when acquiring " +
                                (isExclusive ? "exclusive " : "shared ") +
                                "lock " + lock + " and holding " +
                                ((locks == null) ? "[]" : locks.toString()) +
                                "; " + ((sb == null) ? "" : sb + "; ") +
                                "myThreadToLocks = " + myThreadToLocks
                            );

                            // Now we can throw the exception
                            throw new DeadlockException(
                                "Deadlock detected in thread " + me + " " +
                                "when acquiring " +
                                (isExclusive ? "exclusive " : "shared ") +
                                "lock " + lock + " and holding " +
                                ((locks == null) ? "[]" : locks.toString()) +
                                ((sb == null) ? "" : ": " + sb)

                            );
                        }
                    }
                    finally {
                        ourLockWalksToInstr.end(lwtStart);
                    }
                }

                // Okay, we've either got the lock or we're going to acquire it
                // below so update our state accordingly
                ColouredList<LockManagerLock> locks = myThreadToLocks.get(me);
                if (locks == null) {
                    locks = new ColouredList<>(4);
                    myThreadToLocks.put(me, locks);
                }
                locks.add(lock);
                lock .addLocker(me, gotLock, isExclusive);
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer(
                        "Thread '" + me.getName() + "' now holds " +
                        locks.size() + " lock(s):"
                    );
                    for (LockManagerLock l : locks) {
                        getLogger().finer("  " + l);
                    }
                }

                // Log information if the lock wants us to
                if (LOG.isLoggable(lock.getLogLevel())) {
                    LOG.log(lock.getLogLevel(),
                            "Thread '" + me.getName() + "' acquired " +
                            how + " " + lock,
                            new Throwable());
                }

                // Log each time we seem to have a "lot" of locks
                if (getLogger().isLoggable(Level.FINE) && (locks.size() % 25) == 0) {
                    getLogger().log(
                        Level.FINE,
                        "Thread '" + me.getName() + "' now holds " +
                        locks.size() + " lock(s): ",
                        new Throwable()
                    );
                }

                // If we managed to get the lock above then we can bail at this
                // point since the state is now updated.
                if (gotLock) {
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer("Speculatively acquired " + lock);
                    }
                    return;
                }
            }

            // Okay, we don't have it already and it's safe to acquire. We must
            // do this _outside_ the synchronized block above since we might
            // deadlock otherwise (oh, the irony). This is fine however, since
            // we have updated our state to reflect that we are holding this
            // lock now.
            if (getLogger().isLoggable(Level.FINER)) {
                synchronized (this) {
                    getLogger().finer("Locking " + how + " " + lock);
                }
            }
            lock.lock(isExclusive);
            synchronized (this) {
                // Mark this lock as actually held now. We had set "holding" to
                // false above, since we had not acquired the lock at that
                // point. Now we have it.
                //
                // Note that actually getting the lock and setting this value is
                // not an atomic operation. This is okay though since nothing
                // relies on this being the case (and it should not).
                lock.setLockerHolding(me, isExclusive);
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Locked " + how + " " + lock);
                }
            }
        }
        finally {
            ourLockInstr.end(start);
        }
    }

    /**
     * Release a lock.
     *
     * @param lock         The lock to release.
     * @param isExclusive  Whether the lock should be released exclusively.
     *
     * @throws IllegalMonitorStateException If the current thread does not hold
     *                                      this lock.
     */
    private void unlock(final LockManagerLock lock,
                        final boolean isExclusive)
        throws IllegalMonitorStateException
    {
        final String how = isExclusive ? "exclusive" : "shared";
        final long start = ourUnlockInstr.start();
        try {
            // By whom
            final Thread me = currentThread();

            // Go under the global state lock here since releasing the lock and
            // updating the state need to be done atomically in order for the
            // lock() code to correctly work.
            synchronized(this) {
                // Release the lock
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Releasing " + how + " " + lock);
                }
                lock.unlock(isExclusive);
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("Released " + how + " " + lock);
                }
                if (LOG.isLoggable(lock.getLogLevel())) {
                    LOG.log(lock.getLogLevel(),
                            "Thread '" + me.getName() + "' released " +
                            how + " " + lock,
                            new Throwable());
                }

                // And update the sets, if we are no longer holding the lock
                if (!lock.isHeldByCurrentThread(isExclusive)) {
                    // These should never be null if we were able to lock it in the
                    // first place but...
                    final ColouredList<LockManagerLock> locks = myThreadToLocks.get(me);
                    if (locks != null) {
                        locks.remove(lock);
                    }
                    if (!lock.removeLocker(me, isExclusive)) {
                        throw new IllegalMonitorStateException(
                            "Failed to remove holding thread " +
                            "'" + me.getName() + "' for lock: " + lock
                        );
                    }
                    if (getLogger().isLoggable(Level.FINER)) {
                        getLogger().finer(
                            "Thread '" + me.getName() + "' now holds " +
                            locks.size() + " lock(s):"
                        );
                        for (LockManagerLock l : locks) {
                            getLogger().finer("  " + l);
                        }
                    }

                    // To avoid the maps growing without bound, we should consider
                    // adding garbage collection here to remove threads/locks with
                    // no corresponding sets, if:
                    //  1. The myFooToBar maps are getting "large"
                    //  2. We've not done one in the last N calls to unlock()
                }
            }
        }
        finally {
            ourUnlockInstr.end(start);
        }
    }

    /**
     * See if the given lock walks to the given thread. That is to say, can we
     * navigate the graph comprised of locks-to-threads and thread-to-locks
     * edges to get from a lock to a thread. We only traverse edges where the
     * thread is actually blocked on acquiring a lock, since that is the only
     * route by which we can encounter deadlock.
     *
     * <p>It's important that this method is called before we update the state with
     * the thread and lock being passed in.
     *
     * @param lock   The lock we are walking from.
     * @param thread The thread which we are trying to walk to.
     * @param colour The colour we're using to mark lists which we've seen.
     * @param sb     The buffer to use for returning a helpful message in.
     *
     * @return whether a loop was detected.
     */
    private boolean lockWalksTo(final LockManagerLock lock,
                                final Thread          thread,
                                final int             colour,
                                final StringBuilder   sb)
    {
        // We are effectively edge-chasing in a directed dependency graph here.
        // There exists a bidirectional thread-lock edge in the graph if a
        // thread holds a lock; if a thread is blocked on acquiring a lock then
        // there is a unidirectional edge from thread-to-lock.
        //
        // We will start at the given lock and look at all the threads which
        // currently are blocked on acquiring that lock. If we find the thread
        // we're looking for in this lot then we're done. Otherwise, for each of
        // those threads we see what locks it is trying to acquire, and recurse.
        //
        // An example, imagine that T1 holds L1 and is blocked trying to acquire
        // L2, and that T2 holds L2, and we enter this method at the moment that
        // T2 is trying to acquire L1. Here we are looking to find a path to T2
        // from L1, via blocked threads by calling lockWalksTo(L1, T2, colour):
        //
        //                              [wants]
        //                       [L2] < = = = = = [T1]
        //                        ^                 ^
        //                        |                 |
        //                [holds] |                 | [holds]
        //                        |                 |
        //                        v                 v
        //                       [T2] = = = = = > [L1]
        //                              [wants]
        //
        // So, in the above we walk from L1 to T1, since T1 holds L1 (and so
        // there is a bidirectional edge between them). We then walk from, T1 to
        // L2 (since there is a unidirectional edge from T1 to L2, as T1 is
        // blocked on acquiring L2). Now we also have a bidirectional edge from
        // L2 to T2, and so we walk from L2 to T2. At this point we have found
        // T2, which was the thread we were looking for.
        //
        // It's similar for when we have three threads and three locks.
        //
        // Finally, note that we only care about whether a thread is blocked on
        // getting a lock, not what the type of lock is. This is because, if a
        // thread holds a lock which another is blocked on, it doesn't matter on
        // how it's being held, it's still a blocker.

        // Look at the threads which are holding this lock
        final ColouredList<LockManagerLock.Locker> lockers = lock.getLockers();

        // If we resolve to nothing then we're done
        if (lockers.isEmpty()) {
            if (getLogger().isLoggable(Level.FINEST)) {
                getLogger().finest(lock + " is not held by any threads");
            }
            return false;
        }

        // Say that we've seen this set of threads now. If we discover that we
        // have looked here already then we can bail at this point.
        if (!lockers.setColour(colour)) {
            if (getLogger().isLoggable(Level.FINEST)) {
                getLogger().finest(lock + "'s threads have already been checked");
            }
            return false;
        }

        // First we see if the thread we want is in this list; this is quicker
        // than recursing first and finding it later on.
        for (int i=0; i < lockers.size(); i++) {
            if (lockers.get(i).getThread().equals(thread)) {
                if (getLogger().isLoggable(Level.FINEST)) {
                    getLogger().finest(
                        lock + " walked to thread '" + thread.getName() + "'"
                    );
                }
                return true;
            }
        }

        // Okay, time to do the recursive walk
        for (int i=0; i < lockers.size(); i++) {
            // Examine all the locks upon which each candidate thread is blocked
            // acquiring
            final LockManagerLock.Locker locker = lockers.get(i);
            final ColouredList<LockManagerLock> locks =
                myThreadToLocks.get(locker.getThread());

            // If there's anything to inspect then say we've seen these now,
            // checking to see if we already have at the same time.
            if (locks != null && !locks.isEmpty() && locks.setColour(colour)) {
                // We have some locks which we haven't checked before so we need
                // to inspect them all
                if (getLogger().isLoggable(Level.FINEST)) {
                    getLogger().finest(
                        "Walking locks of thread " +
                        "'" + locker.getThread().getName() + "': " + locks
                    );
                }

                // Recurse on blocked threads
                for (int j=0; j < locks.size(); j++) {
                    final LockManagerLock lml = locks.get(j);
                    if (lml.isThreadAwaiting(locker.getThread())) {
                        if (getLogger().isLoggable(Level.FINER)) {
                            getLogger().finer(
                                "Thread '" + locker.getThread().getName() + "' " +
                                "is blocked acquiring " + lml
                            );
                        }

                        // Update the StringBuilder with the details
                        int sbLen = 0;
                        if (sb != null) {
                            sbLen = sb.length();
                            if (sbLen > 0) {
                                sb.append("; ");
                            }
                            sb.append(locker.getThread().getName())
                              .append(" is blocked acquiring ")
                              .append(lml.getName());
                        }

                        // And see if we want to the lock from here
                        if (lockWalksTo(lml, thread, colour, sb)) {
                            return true;
                        }
                        else if (sb != null) {
                            // We didn't, so the sb can forget this blocking
                            // information
                            sb.setLength(sbLen);
                        }
                    }
                    else if (getLogger().isLoggable(Level.FINEST)) {
                        getLogger().finest(
                            "Thread '" + locker.getThread().getName() + "' " +
                            "is not blocked acquiring " + lml + " " +
                            "so not walking to it"
                        );
                    }
                }
            }
        }

        // If we got here, then no deadlock was detected
        if (getLogger().isLoggable(Level.FINEST)) {
            getLogger().finest(
                "Thread '" + thread.getName() + "' " +
                "was not found from lock " + lock
            );
        }
        return false;
    }
}
