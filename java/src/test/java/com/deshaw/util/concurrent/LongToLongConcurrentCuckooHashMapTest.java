package com.deshaw.util.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.LongConsumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class to test LongToLongConcurrentCuckooHashMap.
 */
public class LongToLongConcurrentCuckooHashMapTest
{
    /**
     * Use to test the FooCallbacks.
     */
    private static class Callback
        implements LongConsumer
    {
        public long total = 0;

        @Override public void accept(long v) { total += v; }
    }

    private static final long NULL = Long.MAX_VALUE;

    /**
     * Compare two hashs and dump if they differ.
     */
    public static boolean compare(Map<Long,Long> jdkMap,
                                  LongToLongConcurrentCuckooHashMap l2lMap)
    {
        if (!doCompare(jdkMap, l2lMap)) {
            System.err.println(jdkMap.toString());
            System.err.println(l2lMap.toString());
            return false;
        }
        return true;
    }

    /**
     * Compare two maps.
     */
    public static boolean doCompare(Map<Long,Long> jdkMap,
                                    LongToLongConcurrentCuckooHashMap l2lMap)
    {
        // Check containment one way
        final Iterator<Map.Entry<Long,Long>> jdkItr = jdkMap.entrySet().iterator();
        while (jdkItr.hasNext()) {
            final Map.Entry<Long,Long> jdkCur = jdkItr.next();
            if (l2lMap.get(jdkCur.getKey(), NULL) != jdkCur.getValue()) {
                System.err.println("Get mismatch: " + jdkCur.getKey() + " " +
                                   l2lMap.get(jdkCur.getKey(), NULL) + " " +
                                   jdkCur.getValue());
                return false;
            }
        }

        // And the other
        final LongToLongConcurrentCuckooHashMap.Iterator desItr = l2lMap.iterator();
        while (desItr.next()) {
            if (jdkMap.get(desItr.currentKey()) != desItr.currentValue()) {
                System.err.println("Get mismatch: " + desItr.currentKey() + " " +
                                   jdkMap.get(desItr.currentKey()) + " " +
                                   desItr.currentValue());
                return false;
            }
        }

        return true;
    }

    /**
     * Run some common operations on the given map.
     */
    @Test
    public void testMap()
    {
        Map<Long,Long> jdkMap = new HashMap<Long,Long>();
        LongToLongConcurrentCuckooHashMap  l2lMap = new LongToLongConcurrentCuckooHashMap();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // Add some stuff; do this in reverse order to attempt to avoid false
        // positives with things which put into order etc.
        final int elements = 100;
        long keyTotal   = 0;
        long valueTotal = 0;
        for (long i = elements-1; i >= 0; i--) {
            jdkMap.put(i, i + 1);
            l2lMap.put(i, i + 1);
            assertTrue(compare(jdkMap, l2lMap));
            keyTotal   += i;
            valueTotal += i + 1;
        }

        // Test counting up the keys and values
        Callback callback = new Callback();
        callback.total = 0; l2lMap.onKeys  (callback);
        assertEquals(keyTotal,   callback.total);
        callback.total = 0; l2lMap.onValues(callback);
        assertEquals(valueTotal, callback.total);

        // Now remove them all, some by remove() and the rest by clear()
        for (long i = elements-1; i >= elements / 10; i--) {
            jdkMap.remove(i);
            l2lMap.remove(i);
            assertTrue(compare(jdkMap, l2lMap));
        }
        jdkMap.clear();
        l2lMap.clear();
        assertTrue(compare(jdkMap, l2lMap));

        // Now try removing and adding in slices
        for (long slice = 1; slice < 10; slice++) {
            for (long i = elements-1-slice; i >= 0; i -= slice) {
                jdkMap.put(i, i + 1);
                l2lMap.put(i, i + 1);
                assertTrue(compare(jdkMap, l2lMap));
            }
            for (long i = elements-slice; i >= 0; i -= slice) {
                jdkMap.put(i, i + 1);
                l2lMap.put(i, i + 1);
                assertTrue(compare(jdkMap, l2lMap));
            }
            for (long i = elements-1-slice; i >= 0; i -= slice) {
                jdkMap.remove(i);
                l2lMap.remove(i);
                assertTrue(compare(jdkMap, l2lMap));
            }
            for (long i = elements-1-slice; i >= 0; i -= slice) {
                jdkMap.remove(i);
                l2lMap.remove(i);
                assertTrue(compare(jdkMap, l2lMap));
            }
        }
    }

    /**
     * Test removal with an iterator.
     */
    @Test
    public void testIteratorRemoval()
    {
        // Make our map, and an iterator
        LongToLongConcurrentCuckooHashMap map          = new LongToLongConcurrentCuckooHashMap();
        LongToLongConcurrentCuckooHashMap.Iterator itr = map.iterator();
        map.put(0L, 0L);
        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 3L);
        map.put(4L, 4L);
        itr.reset();

        int mask = 0;

        // Remove the last two elements
        int i = 0;
        while (itr.next()) {
            if (i++ >= 3) {
                // Remember this and remove it
                mask |= (1 << itr.currentKey());
                itr.remove();
            }
        }

        // And those elements should not be in the mask
        itr.reset();
        while (itr.next()) {
            // Make sure that we don't have anything which we removed
            assertTrue(((1 << itr.currentKey()) & mask) == 0);
        }

        // Remove the rest
        itr.reset();
        while (itr.next()) {
            itr.remove();
        }
        itr.reset();
        assertFalse(itr.hasNext());
    }
}
