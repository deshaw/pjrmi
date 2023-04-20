package com.deshaw.util.concurrent;

import com.deshaw.util.concurrent.LockManager;
import com.deshaw.util.concurrent.LockManager.SafeLock;
import com.deshaw.util.concurrent.LockManager.SafeLock.AcquireFailedException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test LockManager for correctness.
 */
public class LockManagerTest
{
    /**
     * A thread which watches a queue and tries to lock the Strings it finds
     * there.
     */
    private static class Locker
        extends Thread
    {
        private final ArrayBlockingQueue<String> myNames = new ArrayBlockingQueue<>(100);
        private final AtomicBoolean              myDone  = new AtomicBoolean(false);
        private final LockManager                myMgr;
        private final boolean                    myExclusive;

        public Locker(String name, LockManager mgr, boolean exclusive)
        {
            super(name);
            setDaemon(true);
            myMgr = mgr;
            myExclusive = exclusive;
        }

        /**
         * Terminate this thread after a given time; this is to try to prevent
         * the thing hanging forever in some weird case.
         */
        public void doneAfter(final long millis)
        {
            new Thread() {
                public void run() {
                    sleepMs(millis);
                    myDone.set(true);
                }
            }.start();
        }

        /**
         * Add a named lock to the queue for acquisition.
         */
        public void lock(String name)
        {
            myNames.add(name);
        }

        /**
         * See if all the named locks have been pulled from the queue for
         * acquisition.
         */
        public boolean allLocksPolled()
        {
            return myNames.isEmpty();
        }

        /**
         * Look to acquire the next lock in the queue, if any. We call this
         * recursively.
         */
        @Override
        public void run()
        {
            String name = null;
            while (!myDone.get() && (name = myNames.poll()) == null) {
                sleepMs(1);
            }

            if (name != null) {
                try (SafeLock l =
                         (myExclusive
                             ? myMgr.getExclusiveLockFor(name)
                             : myMgr.getSharedLockFor   (name)).acquire())
                {
                    run(); // recurse
                }
            }
        }
    }

    // ---------------------------------------------------------------------- //

    /**
     * We should get a deadlock in this case since we acquire the two locks in a
     * bad order.
     */
    @Test
    public void testDeadlockTwoLocks()
    {
        final LockManager mgr = new LockManager();
        final SafeLock  exclA = mgr.getExclusiveLockFor("A");
        final SafeLock  exclB = mgr.getExclusiveLockFor("B");

        Locker locker = new Locker("testDeadlockTwoLocks", mgr, true);
        locker.start();
        locker.doneAfter(2000);

        exclA.lock();
        locker.lock("B");
        locker.lock("A");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(100);

        try {
            exclB.lock();
            exclB.unlock();
            fail("Expected DeadlockException was not thrown");
        }
        catch (LockManager.DeadlockException e) {
            // Good, we expect to get the exception
        }
        exclA.unlock();
    }

    /**
     * We should get a deadlock in this case since we acquire three locks in a
     * bad order.
     */
    @Test
    public void testDeadlockThreeLocks()
    {
        final LockManager mgr = new LockManager();
        final SafeLock  exclA = mgr.getExclusiveLockFor("A");
        final SafeLock  exclB = mgr.getExclusiveLockFor("B");

        Locker locker = new Locker("testDeadlockThreeLocks", mgr, true);
        locker.start();
        locker.doneAfter(2000);

        exclA.lock();
        locker.lock("C");
        locker.lock("B");
        locker.lock("A");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(100);

        try {
            exclB.lock();
            exclB.unlock();
            fail("Expected DeadlockException was not thrown");
        }
        catch (LockManager.DeadlockException e) {
            // Good
        }
        exclA.unlock();
    }

    /**
     * We should not get a deadlock in this case since we acquire the locks in
     * the same order.
     */
    @Test
    public void testNoDeadlockTwoThreads()
    {
        final LockManager mgr = new LockManager();
        final SafeLock  exclA = mgr.getExclusiveLockFor("A");
        final SafeLock  exclB = mgr.getExclusiveLockFor("B");

        Locker locker = new Locker("testNoDeadlockTwoThreads", mgr, true);
        locker.start();
        locker.doneAfter(1000);

        locker.lock("A");
        locker.lock("B");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(100);

        exclA.lock(); // this will block until locker exits
        exclB.lock();
        exclB.unlock();
        exclA.unlock();
    }

    /**
     * We should not get a deadlock in this case since we're using a single
     * thread, even though we acquire locks in a "bad" order.
     */
    @Test
    public void testNoDeadlockOneThread()
    {
        final LockManager mgr = new LockManager();
        final SafeLock  exclA = mgr.getExclusiveLockFor("A");
        final SafeLock  exclB = mgr.getExclusiveLockFor("B");

        exclA.lock();
        exclB.lock();
        exclA.lock();
        exclA.unlock();
        exclB.unlock();
        exclA.unlock();
    }

    // ----------------------------------------------------------------------

    /**
     * We should not get a deadlock in this case since we're using a single
     * thread; also since these are shared locks.
     */
    @Test
    public void testNoDeadlockSharedOneThread()
    {
        final LockManager  mgr = new LockManager();
        final SafeLock sharedA = mgr.getSharedLockFor("A");
        final SafeLock sharedB = mgr.getSharedLockFor("B");

        sharedA.lock();
        sharedB.lock();
        sharedA.lock();
        sharedA.unlock();
        sharedB.unlock();
        sharedA.unlock();
    }

    /**
     * We should not get a deadlock in this case since we acquire the locks in
     * the same order.
     */
    @Test
    public void testNoDeadlockSharedTwoThreads()
    {
        final LockManager  mgr = new LockManager();
        final SafeLock sharedA = mgr.getSharedLockFor("A");
        final SafeLock sharedB = mgr.getSharedLockFor("B");

        // This is the case where we don't expect deadlock anyhow
        Locker locker = new Locker("testNoDeadlockTwoThreads", mgr, false);
        locker.start();
        locker.doneAfter(1000);

        locker.lock("A");
        locker.lock("B");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(10);

        sharedA.lock();
        sharedB.lock();
        sharedB.unlock();
        sharedA.unlock();

        while (true) {
            try {
                locker.join();
                break;
            }
            catch (InterruptedException e) {
                // Nothing
            }
        }

        // This is the case where we would expect deadlock in an exclusive
        // situation, but not for shared locks
        locker = new Locker("testDeadlockTwoLocks", mgr, false);
        locker.start();
        locker.doneAfter(2000);

        sharedA.lock();
        locker.lock("B");
        locker.lock("A");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(10);

        // This should be fine
        try {
            sharedB.lock();
            sharedB.unlock();
        }
        catch (LockManager.DeadlockException e) {
            throw new RuntimeException(
                "unexpected DeadlockException was thrown", e
            );
        }
        sharedA.unlock();
    }

    /**
     * We should not see a deadlock if we are just waiting to acquire an
     * exclusive lock and others hold shared ones.
     */
    @Test
    public void testNoDeadlockSharedExclusiveTwoThreads()
    {
        final LockManager  mgr = new LockManager();
        final SafeLock sharedA = mgr.getSharedLockFor   ("A");
        final SafeLock   exclB = mgr.getExclusiveLockFor("B");

        Locker locker = new Locker("testNoDeadlockTwoThreads", mgr, false);
        locker.start();
        locker.doneAfter(1000);

        locker.lock("A");
        locker.lock("B");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(10);

        sharedA.lock();

        exclB.lock(); // this will block until locker exits
        exclB.unlock();
    }

    /**
     * We should see a deadlock if we try a exclusive locks over the top of
     * shared ones, just like regular ones.
     */
    @Test
    public void testDeadlockSharedExclusiveTwoThreads()
    {
        final LockManager  mgr = new LockManager();
        final SafeLock   exclA = mgr.getExclusiveLockFor("A");
        final SafeLock sharedB = mgr.getSharedLockFor   ("B");

        Locker locker = new Locker("testNoDeadlockTwoThreads", mgr, true);
        locker.start();
        locker.doneAfter(1000);

        sharedB.lock();

        locker.lock("A");
        locker.lock("B"); // This will block
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(10);

        try {
            exclA.lock(); // This should raise a deadlock exception
            exclA.unlock();
            fail("Expected DeadlockException was not thrown");
        }
        catch (LockManager.DeadlockException e) {
            // Good
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Make sure that try-with-resources works.
     */
    @Test
    public void testTryWithResources()
    {
        final LockManager mgr       = new LockManager();
        final SafeLock    lockExcl = mgr.getExclusiveLockFor("LOCK");
        final SafeLock    lockShrd = mgr.getSharedLockFor   ("LOCK");

        // Acquire the lock in the try clause
        try (LockManager.SafeLock l = lockExcl.acquire()) {
            // We should have it here
            assertTrue(lockExcl.isHeldByCurrentThread(), "Lock should be held");
        }
        // It should have been dropped by here
        assertFalse(lockExcl.isHeldByCurrentThread(), "Lock should not be held");

        // Acquire the lock in the try clause
        try (LockManager.SafeLock l = lockShrd.acquire()) {
            // We should have it here
            assertTrue(lockShrd.isHeldByCurrentThread(), "Lock should be held");
        }
        // It should have been dropped by here
        assertFalse(lockShrd.isHeldByCurrentThread(), "Lock should not be held");
    }

    // ----------------------------------------------------------------------

    /**
     * Make sure that try-with-resources works for the tryAcquire() case.
     */
    @Test
    public void testAcquireWithResources()
    {
        final LockManager mgr  = new LockManager();
        final SafeLock    lock = mgr.getExclusiveLockFor("LOCK");

        // Acquire the lock in the try clause
        try (LockManager.SafeLock l = lock.tryAcquire()) {
            // We should have it here
            assertTrue(lock.isHeldByCurrentThread(), "Lock should be held");
        }
        catch (AcquireFailedException e) {
            fail("Unexpected AcquireFailedException thrown: " + e);
        }
        // It should have been dropped by here
        assertFalse(lock.isHeldByCurrentThread(), "Lock should not be held");

        // Now get a locker to lock it so that tryAcquire() will fail
        final Locker locker = new Locker("testAcquireWithResources", mgr, true);
        locker.start();
        locker.doneAfter(1000);
        locker.lock("LOCK");
        while (!locker.allLocksPolled()) sleepMs(1);
        sleepMs(100);

        // Acquire the lock in the try clause
        try (LockManager.SafeLock l = lock.tryAcquire()) {
            fail("Expected AcquireFailedException not thrown");
        }
        catch (AcquireFailedException e) {
            // Good
        }
        // Check that the lock isn't mysteriously held
        assertFalse(lock.isHeldByCurrentThread(), "Lock should not be held");
    }

    // ----------------------------------------------------------------------

    /**
     * Make sure that state restoral works.
     */
    @Test
    public void testStateRestoral()
    {
        // The lock manager and the locks
        final LockManager mgr   = new LockManager();
        final SafeLock    lock1 = mgr.getExclusiveLockFor("LOCK1");
        final SafeLock    lock2 = mgr.getExclusiveLockFor("LOCK2");
        final SafeLock    lock3 = mgr.getExclusiveLockFor("LOCK3");

        // Acquire Lock1 and Lock2
        lock1.lock();
        lock2.lock();
        assertTrue (lock1.isHeldByCurrentThread(), "LOCK1 should be held"    );
        assertTrue (lock2.isHeldByCurrentThread(), "LOCK2 should be held"    );
        assertFalse(lock3.isHeldByCurrentThread(), "LOCK3 should not be held");

        // At this point we have 1 lock on each of Lock1 and Lock2
        try (LockManager.ThreadLockState state = mgr.saveLockState()) {
            // Get more locks
            lock2.lock();
            lock3.lock();

            // We now have 1 lock on Lock1 and Lock3 and 2 locks on Lock2
            assertTrue(lock1.isHeldByCurrentThread(), "LOCK1 should be held");
            assertTrue(lock2.isHeldByCurrentThread(), "LOCK2 should be held");
            assertTrue(lock3.isHeldByCurrentThread(), "LOCK3 should be held");
        }

        // Upon exiting the try block we now are back to 1 lock on Lock1 and Lock2
        assertTrue (lock1.isHeldByCurrentThread(), "LOCK1 should be held"    );
        assertTrue (lock2.isHeldByCurrentThread(), "LOCK2 should be held"    );
        assertFalse(lock3.isHeldByCurrentThread(), "LOCK3 should not be held");

        // Check that we have the right amount of locking here
        final SafeLock[] locks = {lock1, lock2};
        for (SafeLock lock : locks) {
            assertTrue (lock.isHeldByCurrentThread(), lock + " should be held"    );
            lock.unlock();
            assertFalse(lock.isHeldByCurrentThread(), lock + " should not be held");
        }
    }

    /**
     * Make sure that dropping all locks works
     */
    @Test
    public void testDropAllThreadLocks()
    {
        // The lock manager and the locks
        final LockManager mgr   = new LockManager();
        final SafeLock    lock1 = mgr.getExclusiveLockFor("LOCK1");
        final SafeLock    lock2 = mgr.getExclusiveLockFor("LOCK2");
        final SafeLock    lock3 = mgr.getExclusiveLockFor("LOCK3");

        // Nothing yet
        assertFalse(lock1.isHeldByCurrentThread(), "LOCK1 should not be held");
        assertFalse(lock2.isHeldByCurrentThread(), "LOCK2 should not be held");
        assertFalse(lock3.isHeldByCurrentThread(), "LOCK3 should not be held");

        // Acquire lockN N times
        lock1.lock();
        lock2.lock(); lock2.lock();
        lock3.lock(); lock3.lock(); lock3.lock();

        // We now have 1 lock on Lock1 and Lock3 and 2 locks on Lock2
        assertTrue(lock1.isHeldByCurrentThread(), "LOCK1 should be held");
        assertTrue(lock2.isHeldByCurrentThread(), "LOCK2 should be held");
        assertTrue(lock3.isHeldByCurrentThread(), "LOCK3 should be held");

        // Drop everything
        mgr.dropAllThreadLocks();

        // Nothing now
        assertFalse(lock1.isHeldByCurrentThread(), "LOCK1 should be released");
        assertFalse(lock2.isHeldByCurrentThread(), "LOCK2 should be released");
        assertFalse(lock3.isHeldByCurrentThread(), "LOCK3 should be released");
    }

    // ----------------------------------------------------------------------

     /**
     * Wait for at least a set period of time.
     */
    private static void sleepMs(long millis)
    {
        final long until = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < until) {
            try {
                // Wait for one ms and then see if we've waited long enough
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                // Don't care, loop around and continue to wait
            }
        }
    }
}
