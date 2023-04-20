package com.deshaw.util.concurrent;

import com.deshaw.util.concurrent.VirtualThreadLock;
import com.deshaw.util.concurrent.VirtualThreadLock.VirtualThread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the VirtualThreadLock class for correctness.
 */
public class VirtualThreadLockTest
{
    // What we'll use in the tests
    private static final VirtualThread THREAD1 = new VirtualThread("Thread1");
    private static final VirtualThread THREAD2 = new VirtualThread("Thread2");

    /**
     * Ensure that get and set works as expected.
     */
    @Test
    public void testGetSet()
    {
        // Ensure it's clear; should not throw
        VirtualThreadLock.setThread(null);

        // This should throw
        try {
            VirtualThreadLock.getThread();
            fail("getThread() should have thrown");
        }
        catch (IllegalArgumentException e) { /* Nothing */ }

        // Associate, should not throw
        VirtualThreadLock.setThread(THREAD1);

        // This shoudl now not throw
        VirtualThreadLock.getThread();

        // This should now throw
        try {
            VirtualThreadLock.setThread(THREAD2);
            fail("setThread() should have thrown");
        }
        catch (IllegalArgumentException e) { /* Nothing */ }

        // Tidy up; should not throw
        VirtualThreadLock.setThread(null);
    }

    /**
     * Ensure that acquiring works as expected for the same VirtualThread.
     */
    @Test
    public void testMultipleSameLockers()
    {
        final VirtualThreadLock lock = new VirtualThreadLock("SameLock");

        List<String> exceptions = Collections.synchronizedList(new ArrayList<>());

        final AtomicBoolean result1 = new AtomicBoolean();
        final AtomicBoolean result2 = new AtomicBoolean();
        final Thread locker1 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD1);
                        final boolean locked = lock.tryLock();
                        sleep(200);
                        if (locked) {
                            assertTrue(lock.isHeldByCurrentThread());
                            lock.unlock();
                        }
                        result1.set(locked);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };
        final Thread locker2 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD1);
                        sleep(100);
                        final boolean locked = lock.tryLock();
                        sleep(100);
                        if (locked) {
                            assertTrue(lock.isHeldByCurrentThread());
                            lock.unlock();
                        }
                        result2.set(locked);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };

        // Daemonise, we don't want to hang on exit
        locker1.setDaemon(true);
        locker2.setDaemon(true);

        // Start them
        final long start = System.nanoTime();
        locker1.start();
        locker2.start();

        // Wait for them to finish, or timeout
        while ((locker1.isAlive() || locker2.isAlive()) &&
               (System.nanoTime() - start < 10_000_000_000L))
        {
            sleep(1);
        }

        assertTrue(exceptions.isEmpty(),
                   "Problems in spawned thread: " + exceptions);

        // Threads should have exited
        assertFalse(locker1.isAlive(), "Locker1 didn't exit"   );
        assertFalse(locker2.isAlive(), "Locker2 didn't exit"   );
        assertTrue (result1.get(),     "Locker1 failed to lock");
        assertTrue (result2.get(),     "Locker2 failed to lock");
    }

    /**
     * Ensure that acquiring works as expected for different VirtualThreads.
     */
    @Test
    public void testMultipleDifferentTryLockers()
    {
        final VirtualThreadLock lock = new VirtualThreadLock("DifferentLock");

        List<String> exceptions = Collections.synchronizedList(new ArrayList<>());

        final AtomicBoolean result1 = new AtomicBoolean();
        final AtomicBoolean result2 = new AtomicBoolean();
        final Thread locker1 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD1);
                        final boolean locked = lock.tryLock();
                        sleep(200);
                        if (locked) {
                            assertTrue(lock.isHeldByCurrentThread());
                            lock.unlock();
                        }
                        result1.set(locked);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };
        final Thread locker2 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD2);
                        sleep(100);
                        assertTrue(lock.isLocked(), "Lock is not locked");
                        final boolean locked = lock.tryLock();
                        sleep(100);
                        if (locked) {
                            assertTrue(lock.isHeldByCurrentThread());
                            lock.unlock();
                        }
                        result2.set(locked);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };

        // Daemonise, we don't want to hang on exit
        locker1.setDaemon(true);
        locker2.setDaemon(true);

        // Start them
        final long start = System.nanoTime();
        locker1.start();
        locker2.start();

        // Wait for them to finish, or timeout
        while ((locker1.isAlive() || locker2.isAlive()) &&
               (System.nanoTime() - start < 10_000_000_000L))
        {
            sleep(1);
        }

        assertTrue(exceptions.isEmpty(),
                   "Problems in spawned thread: " + exceptions);

        // Threads should have exited
        assertFalse(locker1.isAlive(),"Locker1 didn't exit"          );
        assertFalse(locker2.isAlive(),"Locker2 didn't exit"          );
        assertTrue (result1.get(),    "Locker1 failed to lock"       );
        assertFalse(result2.get(),    "Locker2 shouldn't have locked");
    }

    /**
     * Ensure that acquiring works as expected for different VirtualThreads.
     */
    @Test
    public void testMultipleDifferentBlockingLockers()
    {
        final VirtualThreadLock lock = new VirtualThreadLock("DifferentLock");

        List<String> exceptions = Collections.synchronizedList(new ArrayList<>());

        final AtomicBoolean result1 = new AtomicBoolean();
        final AtomicBoolean result2 = new AtomicBoolean();
        final Thread locker1 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD1);
                        lock.lock();
                        sleep(200);
                        assertTrue(lock.isHeldByCurrentThread());
                        lock.unlock();
                        result1.set(true);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };
        final Thread locker2 =
            new Thread()
            {
                @Override public void run()
                {
                    try {
                        VirtualThreadLock.setThread(THREAD2);
                        sleep(100);
                        assertTrue(lock.isLocked(), "Lock is not locked");
                        lock.lock();
                        sleep(100);
                        assertTrue(lock.isHeldByCurrentThread());
                        lock.unlock();
                        result2.set(true);
                    }
                    catch (Throwable t) {
                        exceptions.add(this + ": " + t);
                    }
                }
            };

        // Daemonise, we don't want to hang on exit
        locker1.setDaemon(true);
        locker2.setDaemon(true);

        // Start them
        final long start = System.nanoTime();
        locker1.start();
        locker2.start();

        // Wait for them to finish, or timeout
        while ((locker1.isAlive() || locker2.isAlive()) &&
               (System.nanoTime() - start < 10_000_000_000L))
        {
            sleep(1);
        }

        assertTrue(exceptions.isEmpty(),
                   "Problems in spawned thread: " + exceptions);

        // Threads should have exited
        assertFalse(locker1.isAlive(), "Locker1 didn't exit"   );
        assertFalse(locker2.isAlive(), "Locker2 didn't exit"   );
        assertTrue (result1.get(),     "Locker1 failed to lock");
        assertTrue (result2.get(),     "Locker2 failed to lock");
    }

    /**
     * Sleep without worrying about interruptions.
     */
    private static void sleep(long millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            // Nothing
        }
    }
}
