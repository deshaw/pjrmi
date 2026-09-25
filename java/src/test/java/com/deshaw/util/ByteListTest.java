package com.deshaw.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A unit test suite for testing the {@link com.deshaw.util.ByteList} class.
 *
 * <p>Note: {@code testSubArrayBoundary} and
 * {@code testAddAllAcrossSubArrayBoundary} below each span two sub-arrays, and
 * so need a gigabyte and a half at their peak. That fits within the heap the
 * test task is given, one test at a time.
 * The cases which need several gigabytes live in {@code ByteListStressMain}
 * instead, which the build runs only on a machine with the memory for it.
 */
public class ByteListTest
{
    /**
     * Indices which no list can hold because they are negative. Sign aside,
     * these are chosen so that each would address a valid element if the range
     * check were done after the narrowing cast to a sub-array number rather
     * than before it.
     */
    private static final long[] NEGATIVE_INDICES = new long[] {
        Long.MIN_VALUE,
        -(1L << 62),
        -(1L << 62) + 1,
        -(1L << 31),
        -2L,
        -1L
    };

    /**
     * Test that a list built with the no-args constructor starts out empty.
     */
    @Test
    public void testDefaultConstructor()
    {
        final ByteList list = new ByteList();
        assertEquals(0L, list.size());
        assertEquals(0L, list.capacity());
    }

    /**
     * Test the constructor which takes an initial capacity.
     */
    @Test
    public void testCapacityConstructor()
    {
        final ByteList list = new ByteList(100);
        assertEquals(  0L, list.size());
        assertEquals(100L, list.capacity());
    }

    /**
     * Test that a zero initial capacity allocates nothing.
     */
    @Test
    public void testZeroCapacityConstructor()
    {
        final ByteList list = new ByteList(0);
        assertEquals(0L, list.size());
        assertEquals(0L, list.capacity());
    }

    /**
     * Test that a negative initial capacity is rejected.
     */
    @Test
    public void testNegativeCapacityConstructor()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            new ByteList(-1);
        });
    }

    /**
     * Test that an initial capacity beyond what the list can ever hold is
     * rejected, rather than being allowed to overflow into something small.
     */
    @Test
    public void testHugeCapacityConstructor()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            new ByteList(Long.MAX_VALUE);
        });
    }

    /**
     * Test that the first allocation is the documented default, and that
     * growth thereafter keeps the capacity at or above the size.
     */
    @Test
    public void testGrowth()
    {
        final ByteList list = new ByteList();

        list.add((byte)1);
        assertEquals(1L, list.size());
        assertEquals((long)ByteList.DEFAULT_INITIAL_CAPACITY, list.capacity());

        long capacity = list.capacity();
        for (int i=0; i < 10000; i++) {
            list.add((byte)i);
            assertTrue(list.capacity() >= list.size(),
                       "Capacity " + list.capacity() + " below size " +
                       list.size());
            assertTrue(list.capacity() >= capacity,
                       "Capacity shrank from " + capacity + " to " +
                       list.capacity());
            capacity = list.capacity();
        }
        assertEquals(10001L, list.size());
    }

    /**
     * Test that a large ensureCapacity() lands on exactly what was asked for.
     * The zero-copy path in toArray() relies on this.
     */
    @Test
    public void testEnsureCapacityIsExact()
    {
        final ByteList list = new ByteList(1024);
        list.ensureCapacity(1024 * 1024);
        assertEquals(1024L * 1024L, list.capacity());
    }

    /**
     * Test that ensureCapacity() never reduces the capacity.
     */
    @Test
    public void testEnsureCapacityNeverShrinks()
    {
        final ByteList list = new ByteList(1000);
        list.ensureCapacity(10);
        assertEquals(1000L, list.capacity());
    }

    /**
     * Test that a negative capacity is rejected by ensureCapacity().
     */
    @Test
    public void testEnsureCapacityNegative()
    {
        final ByteList list = new ByteList();
        assertThrows(IllegalArgumentException.class, () -> {
            list.ensureCapacity(-1);
        });
    }

    /**
     * Test that a capacity beyond what the list can hold is rejected by
     * ensureCapacity().
     */
    @Test
    public void testEnsureCapacityTooLarge()
    {
        final ByteList list = new ByteList();
        assertThrows(IllegalArgumentException.class, () -> {
            list.ensureCapacity(Long.MAX_VALUE);
        });
    }

    /**
     * Test appending single elements and reading them back.
     */
    @Test
    public void testAddAndGet()
    {
        final ByteList list = new ByteList();
        for (int i=0; i < 256; i++) {
            assertTrue(list.add((byte)i));
        }

        assertEquals(256L, list.size());
        for (int i=0; i < 256; i++) {
            assertEquals((byte)i, list.get(i));
            assertEquals((byte)i, list.getByte(i));
        }
    }

    /**
     * Test overwriting elements with set().
     */
    @Test
    public void testSet()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        list.set(0, (byte)10);
        list.set(3, (byte)40);

        assertArrayEquals(new byte[] { (byte)10, (byte)2, (byte)3, (byte)40 },
                          list.toArray());
        assertEquals(4L, list.size());
    }

    /**
     * Test that set() drops the cached toString() value, like the other
     * mutators do.
     */
    @Test
    public void testSetInvalidatesToString()
    {
        final ByteList list = new ByteList();
        list.append("abc".getBytes());
        assertEquals("abc", list.toString());

        list.set(1, (byte)'X');
        assertEquals("aXc", list.toString());
    }

    /**
     * Test that set() refuses an index which is out of range.
     */
    @Test
    public void testSetOutOfBounds()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2 });

        final long beyond = list.capacity();
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.set(beyond, (byte)9));
    }

    /**
     * Test that set() refuses a negative index.
     *
     * <p>This matters more than it looks. The sub-array number comes from the
     * middle bits of the index, so an index like {@code Long.MIN_VALUE} has
     * both a sub-array number and an offset of zero once the sign bits have
     * been shifted away. Without an explicit check these would quietly
     * overwrite element zero rather than being rejected.
     */
    @Test
    public void testSetNegativeIndex()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        for (final long index : NEGATIVE_INDICES) {
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.set(index, (byte)99),
                         "set(" + index + ") should have been rejected");
        }

        // And nothing should have been touched by any of them
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 },
                          list.toArray());
    }

    /**
     * Test the boolean accessor.
     */
    @Test
    public void testGetBoolean()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)0, (byte)1, (byte)-1, (byte)127 });

        assertFalse(list.getBoolean(0));
        assertTrue (list.getBoolean(1));
        assertTrue (list.getBoolean(2));
        assertTrue (list.getBoolean(3));
    }

    /**
     * Test the multi-byte accessors against a known big-endian pattern.
     */
    @Test
    public void testMultiByteAccessors()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)0x01, (byte)0x23, (byte)0x45, (byte)0x67,
                                 (byte)0x89, (byte)0xab, (byte)0xcd, (byte)0xef });

        assertEquals((short)0x0123,         list.getShort(0));
        assertEquals((short)0x2345,         list.getShort(1));
        assertEquals(0x01234567,            list.getInt(0));
        assertEquals(0x456789ab,            list.getInt(2));
        assertEquals(0x0123456789abcdefL,   list.getLong(0));
        assertEquals(Float.intBitsToFloat(0x01234567),
                     list.getFloat(0));
        assertEquals(Double.longBitsToDouble(0x0123456789abcdefL),
                     list.getDouble(0));
    }

    /**
     * Test that reading beyond the end of the list is rejected.
     */
    @Test
    public void testGetOutOfBounds()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        // Beyond the capacity, which growth may have put past the size
        final long beyond = list.capacity();
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getShort(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getInt(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getLong(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getFloat(beyond));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getDouble(beyond));
    }

    /**
     * Test that a multi-byte read which starts inside the list but runs off
     * the end of it is rejected.
     */
    @Test
    public void testGetRunOffTheEnd()
    {
        final ByteList list = new ByteList(4);
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });
        assertEquals(4L, list.capacity());

        assertThrows(IndexOutOfBoundsException.class, () -> list.getLong(0));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getInt(1));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getShort(3));
    }

    /**
     * Test that a read of the space between the size and the capacity is
     * rejected.
     *
     * <p>That space is backed by a sub-array, so the size is the only thing
     * standing between the caller and whatever happens to be sitting there.
     */
    @Test
    public void testGetBeyondSizeIsRejected()
    {
        final ByteList list = new ByteList(100);
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });
        assertEquals(  4L, list.size());
        assertEquals(100L, list.capacity());

        final long beyond = list.size();
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.get       (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getBoolean(beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getByte   (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getShort  (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getInt    (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getLong   (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getFloat  (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.getDouble (beyond));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.charAt((int)beyond));
    }

    /**
     * Test that a write to the space between the size and the capacity is
     * rejected, and that it leaves the list alone.
     */
    @Test
    public void testSetBeyondSizeIsRejected()
    {
        final ByteList list = new ByteList(100);
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        for (final long index : new long[] { list.size(),
                                             list.size() + 1,
                                             list.capacity() - 1 }) {
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.set(index, (byte)99),
                         "set(" + index + ") should have been rejected");
        }

        assertEquals(4L, list.size());
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 },
                          list.toArray());
    }

    /**
     * Test that a multi-byte read which starts inside the list but reaches
     * into the space between the size and the capacity is rejected.
     *
     * <p>The whole run has to be within the list, not just its first byte.
     */
    @Test
    public void testGetRunBeyondSizeIsRejected()
    {
        final ByteList list = new ByteList(100);
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        assertThrows(IndexOutOfBoundsException.class, () -> list.getShort(3));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getInt  (1));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getLong (0));

        // The runs which do fit still work
        assertEquals((short)0x0304, list.getShort(2));
        assertEquals(0x01020304,    list.getInt  (0));
    }

    /**
     * Test that a list gives back nothing of what it held before it was
     * emptied or shrunk.
     *
     * <p>The sub-arrays are kept and reused, so the old elements are still
     * there to be read back; a list which is refilled for one payload after
     * another must not hand out the tail of the last one.
     */
    @Test
    public void testReadsNeverSeeTheOldContents()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)0x01, (byte)0x02, (byte)0x03,
                                 (byte)0x04, (byte)0x05, (byte)0x06,
                                 (byte)0x07, (byte)0x08 });

        // Emptied and refilled with less than was there before
        list.clear();
        list.append(new byte[] { (byte)0x11, (byte)0x12 });
        assertEquals(2L, list.size());
        assertTrue(list.capacity() >= 8,
                   "Capacity " + list.capacity() + " was not kept");

        assertEquals((byte)0x11, list.get(0));
        assertEquals((byte)0x12, list.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(2));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getInt(0));
        assertArrayEquals(new byte[] { (byte)0x11, (byte)0x12 },
                          list.toArray());

        // And the same for a list which was shrunk rather than emptied
        list.append(new byte[] { (byte)0x13, (byte)0x14 });
        list.setSize(3);
        assertEquals((byte)0x13, list.get(2));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(3));
        assertThrows(IndexOutOfBoundsException.class, () -> list.getInt(0));
    }

    /**
     * Test that negative indices are rejected.
     *
     * <p>The interesting cases are the ones whose sub-array number and offset
     * both come out as zero once the sign bits have been shifted away; without
     * an explicit check they would quietly address element zero.
     */
    @Test
    public void testGetNegativeIndex()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        for (final long index : NEGATIVE_INDICES) {
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.get(index),
                         "get(" + index + ") should have been rejected");
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.getInt(index),
                         "getInt(" + index + ") should have been rejected");
        }
    }

    /**
     * Test that indices which are too large are rejected.
     *
     * <p>The mirror of the negative-index case. The sub-array number is taken
     * from the middle bits of the index and needs more bits than an
     * {@code int} has, so an index like 2^62 is positive, yet truncates to
     * sub-array zero with an offset of zero. Without the range check being
     * done before that truncation these would quietly address element zero.
     */
    @Test
    public void testHugeIndex()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 });

        for (final long index : HUGE_INDICES) {
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.get(index),
                         "get(" + index + ") should have been rejected");
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.getInt(index),
                         "getInt(" + index + ") should have been rejected");
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.set(index, (byte)99),
                         "set(" + index + ") should have been rejected");
        }

        // And none of them should have touched anything
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 },
                          list.toArray());
    }

    /**
     * Indices which are too large for any list to hold. These all truncate to
     * a low sub-array number if the range check is done after the narrowing
     * cast rather than before it.
     */
    private static final long[] HUGE_INDICES = new long[] {
        (1L << 61),
        (1L << 62),
        (1L << 62) + 1,
        (1L << 62) + 3,
        Long.MAX_VALUE
    };

    /**
     * Test appending a whole array.
     */
    @Test
    public void testAppendArray()
    {
        final byte[] data = new byte[] { (byte)10, (byte)20, (byte)30 };

        final ByteList list = new ByteList();
        assertSame(list, list.append(data));
        assertSame(list, list.append(data));

        assertEquals(6L, list.size());
        assertArrayEquals(new byte[] { (byte)10, (byte)20, (byte)30,
                                       (byte)10, (byte)20, (byte)30 },
                          list.toArray());
    }

    /**
     * Test appending part of an array.
     */
    @Test
    public void testAppendSubArray()
    {
        final byte[] data = new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 };

        final ByteList list = new ByteList();
        list.append(data, 1, 2);

        assertEquals(2L, list.size());
        assertArrayEquals(new byte[] { (byte)2, (byte)3 }, list.toArray());
    }

    /**
     * Test that appending nothing does nothing.
     */
    @Test
    public void testAppendEmpty()
    {
        final ByteList list = new ByteList();
        list.append(new byte[0]);
        list.append(new byte[] { (byte)1, (byte)2 }, 1, 0);
        assertEquals(0L, list.size());
    }

    /**
     * Test that a bad offset or length is rejected, including the case where
     * their sum overflows an int.
     */
    @Test
    public void testAppendBadBounds()
    {
        final ByteList list = new ByteList();
        final byte[]   data = new byte[] { (byte)1, (byte)2, (byte)3, (byte)4 };

        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.append(data, -1, 1));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.append(data, 0, -1));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.append(data, 2, 3));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.append(data, Integer.MAX_VALUE - 1, 10));

        // Nothing should have been appended by any of those
        assertEquals(0L, list.size());
    }

    /**
     * Test appending one list to another.
     */
    @Test
    public void testAddAll()
    {
        final ByteList src = new ByteList();
        src.append(new byte[] { (byte)4, (byte)5 });

        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });

        assertTrue(list.addAll(src));
        assertEquals(5L, list.size());
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3,
                                       (byte)4, (byte)5 },
                          list.toArray());

        // The source should be untouched
        assertEquals(2L, src.size());
    }

    /**
     * Test that appending an empty list is a no-op which says so.
     */
    @Test
    public void testAddAllEmpty()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1 });

        assertFalse(list.addAll(new ByteList()));
        assertEquals(1L, list.size());
    }

    /**
     * Test that appending a list to itself doubles it, rather than hanging or
     * producing something else.
     */
    @Test
    public void testAddAllSelf()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });

        assertTrue(list.addAll(list));
        assertEquals(6L, list.size());
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3,
                                       (byte)1, (byte)2, (byte)3 },
                          list.toArray());
    }

    /**
     * Test that appending an empty list to itself is a no-op.
     */
    @Test
    public void testAddAllSelfEmpty()
    {
        final ByteList list = new ByteList();
        assertFalse(list.addAll(list));
        assertEquals(0L, list.size());
    }

    /**
     * Test that toArray() always hands back an array of exactly the list's
     * size, whether or not it has to copy to do so.
     */
    @Test
    public void testToArrayLength()
    {
        // Empty
        final ByteList empty = new ByteList();
        assertEquals(0, empty.toArray().length);

        // Exactly filled, so handed back without a copy
        final ByteList exact = new ByteList(3);
        exact.append(new byte[] { (byte)1, (byte)2, (byte)3 });
        assertEquals(3L, exact.size());
        assertEquals(3L, exact.capacity());
        assertEquals(3, exact.toArray().length);
        assertSame(exact.toArray(), exact.toArray());

        // Partly filled, so copied down to size
        final ByteList slack = new ByteList(100);
        slack.append(new byte[] { (byte)1, (byte)2, (byte)3 });
        assertEquals(  3L, slack.size());
        assertEquals(100L, slack.capacity());
        assertEquals(3, slack.toArray().length);
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3 },
                          slack.toArray());
    }

    /**
     * Test writeTo() for an empty list, a filled one, and one which has been
     * shrunk so that its backing store holds more than its size.
     */
    @Test
    public void testWriteTo()
        throws IOException
    {
        final ByteList list = new ByteList();

        // Empty
        final ByteArrayOutputStream empty = new ByteArrayOutputStream();
        list.writeTo(empty);
        assertEquals(0, empty.size());

        // Filled
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });
        final ByteArrayOutputStream filled = new ByteArrayOutputStream();
        list.writeTo(filled);
        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3 },
                          filled.toByteArray());

        // Shrunk, so that the capacity is beyond the size; only the live
        // elements should be written
        list.setSize(2);
        final ByteArrayOutputStream shrunk = new ByteArrayOutputStream();
        list.writeTo(shrunk);
        assertArrayEquals(new byte[] { (byte)1, (byte)2 },
                          shrunk.toByteArray());

        // And cleared
        list.clear();
        final ByteArrayOutputStream cleared = new ByteArrayOutputStream();
        list.writeTo(cleared);
        assertEquals(0, cleared.size());
    }

    /**
     * Test that writeTo() gives back exactly what toArray() does, for a list
     * whose backing store is larger than its contents.
     */
    @Test
    public void testWriteToMatchesToArray()
        throws IOException
    {
        final ByteList list = new ByteList(1000);
        for (int i=0; i < 300; i++) {
            list.add((byte)i);
        }
        assertEquals( 300L, list.size());
        assertEquals(1000L, list.capacity());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        list.writeTo(out);
        assertArrayEquals(list.toArray(), out.toByteArray());
    }

    /**
     * Test that clearing empties the list, and that the list is usable
     * afterwards.
     */
    @Test
    public void testClear()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });
        assertEquals("\1\2\3", list.toString());

        list.clear();
        assertEquals(0L, list.size());
        assertEquals(0,  list.toArray().length);
        assertEquals("", list.toString());

        list.append(new byte[] { (byte)4 });
        assertEquals(1L, list.size());
        assertArrayEquals(new byte[] { (byte)4 }, list.toArray());
    }

    /**
     * Test growing and shrinking with setSize().
     */
    @Test
    public void testSetSize()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });

        list.setSize(2);
        assertEquals(2L, list.size());
        assertArrayEquals(new byte[] { (byte)1, (byte)2 }, list.toArray());

        list.setSize(5);
        assertEquals(5L, list.size());
        assertTrue(list.capacity() >= 5);

        // The contents beyond the old size are whatever was in the capacity
        // already, so all we can say about them is how many there are
        assertEquals(5, list.toArray().length);
    }

    /**
     * Test that a negative size is rejected by setSize().
     */
    @Test
    public void testSetSizeNegative()
    {
        final ByteList list = new ByteList();
        assertThrows(IllegalArgumentException.class, () -> list.setSize(-1));
    }

    /**
     * Test the CharSequence methods.
     */
    @Test
    public void testCharSequence()
    {
        final ByteList list = new ByteList();
        list.append("hello".getBytes());

        assertEquals(5,     list.length());
        assertEquals('h',   list.charAt(0));
        assertEquals('o',   list.charAt(4));
        assertEquals("hello", list.toString());
        assertEquals("ell", list.subSequence(1, 4).toString());
    }

    /**
     * Test that the toString() cache is dropped whenever the list changes.
     */
    @Test
    public void testToStringIsInvalidated()
    {
        final ByteList list = new ByteList();
        list.append("ab".getBytes());
        assertEquals("ab", list.toString());

        list.add((byte)'c');
        assertEquals("abc", list.toString());

        list.append("de".getBytes());
        assertEquals("abcde", list.toString());

        list.addAll(list);
        assertEquals("abcdeabcde", list.toString());

        list.setSize(3);
        assertEquals("abc", list.toString());

        list.clear();
        assertEquals("", list.toString());
    }

    /**
     * Test both flavours of subSequence().
     */
    @Test
    public void testSubSequence()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3, (byte)4, (byte)5 });

        assertArrayEquals(new byte[] { (byte)2, (byte)3 },
                          ((ByteList)list.subSequence(1, 3)).toArray());
        assertArrayEquals(new byte[] { (byte)2, (byte)3 },
                          list.subSequence(1L, 3L).toArray());

        // The whole thing, and nothing at all
        assertEquals(5L, list.subSequence(0L, 5L).size());
        assertEquals(0L, list.subSequence(2L, 2L).size());

        // The result must be independent of the list it came from
        final ByteList sub = list.subSequence(0L, 5L);
        list.clear();
        assertEquals(5L, sub.size());
    }

    /**
     * Test that a bad range is rejected by subSequence().
     */
    @Test
    public void testSubSequenceBadBounds()
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)1, (byte)2, (byte)3 });

        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.subSequence(-1L, 2L));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.subSequence(0L, -1L));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.subSequence(2L, 1L));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> list.subSequence(0L, 4L));
    }
    /**
     * Everything which walks the sub-arrays, exercised across the seam between
     * the first and the second.
     *
     * <p>This holds one list of a little over a gigabyte, and only one at a
     * time. Growing the first sub-array doubles it, so the last step has the
     * old half-gigabyte live alongside the new gigabyte and the transient peak
     * is nearer one and a half; the test heap is 4Gb, which that fits and a
     * second list of the same size would not. The destination side of the same
     * split is in {@code testAddAllAcrossSubArrayBoundary}, which costs the
     * same again but not at the same time. Cases which need more than one list
     * live in {@code ByteListStressMain} rather than here.
     *
     * <p>It is worth the heap: the split-copy loops are the part of this class
     * which a unit test can otherwise never reach, and the sibling straddle
     * bug fixed in the hypercube code (see
     * {@code HypercubeTest.testArrayHypercubeSubArrayBoundary}) was exactly
     * this shape -- two halves of a split handed each other's lengths, which
     * comes out right only when the run happens to split evenly.
     * The split point is therefore walked across the whole run, rather than
     * being sampled at one offset.
     */
    @Test
    public void testSubArrayBoundary()
        throws IOException
    {
        // A run which straddles the seam, with the data either side of it
        final int  span = 64;
        final long base = ByteList.SUB_ARRAY_SIZE - span / 2;

        final byte[] block = new byte[span];
        for (int i=0; i < block.length; i++) {
            block[i] = (byte)(i + 1);
        }

        final ByteList list = new ByteList();
        list.setSize(base);
        list.append(block);
        assertEquals(base + span, list.size());

        // Every element of the run reads back, on both sides of the seam
        for (int i=0; i < span; i++) {
            assertEquals(block[i], list.get(base + i),
                         "element " + i + " across the seam");
        }

        // The multi-byte getters have a fast path for a run inside one
        // sub-array and a slow one for a run which straddles two. Walking the
        // start position across the seam takes each of them through both.
        for (int i=0; i + Long.BYTES <= span; i++) {
            final long at = base + i;
            long expected = 0;
            for (int j=0; j < Long.BYTES; j++) {
                expected = (expected << 8) | (block[i + j] & 0xffL);
            }
            assertEquals(expected, list.getLong(at),
                         "getLong at seam offset " + i);
            assertEquals((int)(expected >>> 32), list.getInt(at),
                         "getInt at seam offset " + i);
            assertEquals((short)(expected >>> 48), list.getShort(at),
                         "getShort at seam offset " + i);
        }

        // subSequence() goes through the two-sided copy loop. Walk where the
        // range starts so that the split lands at a different place each time.
        for (int i=0; i < span; i++) {
            final ByteList sub = list.subSequence(base + i, base + span);
            assertEquals(span - i, sub.size(), "subSequence size at " + i);
            for (int j=0; j < span - i; j++) {
                assertEquals(block[i + j], sub.get(j),
                             "subSequence " + i + " element " + j);
            }
        }

        // copyTo() is the other multi-sub-array walk. Same treatment.
        for (int i=0; i < span; i++) {
            final byte[] dst = new byte[span - i];
            list.copyTo(base + i, dst, 0, dst.length);
            for (int j=0; j < dst.length; j++) {
                assertEquals(block[i + j], dst[j],
                             "copyTo " + i + " element " + j);
            }
        }

        // writeTo() hands over each sub-array in turn, so the whole list has to
        // come out in order. Count what arrives and check the tail, rather than
        // keeping a second copy of a gigabyte.
        /*scope*/ {
            final long[] seen = new long[] { 0 };
            final byte[] tail = new byte[span];
            list.writeTo(
                new java.io.OutputStream() {
                    @Override
                    public void write(final int b)
                    {
                        write(new byte[] { (byte)b }, 0, 1);
                    }

                    @Override
                    public void write(final byte[] b, final int off,
                                      final int len)
                    {
                        for (int i=0; i < len; i++) {
                            final long pos = seen[0] + i;
                            if (pos >= base) {
                                tail[(int)(pos - base)] = b[off + i];
                            }
                        }
                        seen[0] += len;
                    }
                }
            );
            assertEquals(base + span, seen[0], "writeTo byte count");
            assertArrayEquals(block, tail, "writeTo tail");
        }
    }

    /**
     * The copy in {@code addAll()} walked across the seam on its destination
     * side.
     *
     * <p>This is the half which {@code testSubArrayBoundary} cannot reach.
     * Everything there straddles the seam in the list being read from and
     * copies into a small destination, so the source is always what limits how
     * much moves in one step. Here it is the other way about: the source is a
     * few bytes living in the first sub-array, and the destination is the list
     * which straddles. The two sides are computed by separate expressions, and
     * a split which takes its length from the wrong one comes out right
     * whenever the two happen to agree -- which is every case the other test
     * covers.
     *
     * <p>The heap cost is the same shape as {@code testSubArrayBoundary}: one
     * list a little over a gigabyte, and a transient peak while the first
     * sub-array grows into it. {@code addAll()} reserves the whole thing in one
     * go before it copies, exactly as {@code append()} does, so appending to a
     * list already at the seam costs nothing beyond the list itself.
     *
     * <p>The append is done in several small chunks rather than one large one
     * so that the destination column differs each time, with one chunk landing
     * astride the seam and the rest either side of it.
     */
    @Test
    public void testAddAllAcrossSubArrayBoundary()
    {
        final int chunk = 16;
        final int count = 8;

        // Offset by half a chunk so that a chunk straddles the seam, rather
        // than one ending exactly on it and the next starting there
        final long base =
            ByteList.SUB_ARRAY_SIZE - (long)chunk * count / 2 - chunk / 2;

        // The source is small, so it stays wholly inside its first sub-array
        final ByteList src = new ByteList();
        for (int i=0; i < chunk; i++) {
            src.add((byte)(i + 1));
        }

        final ByteList list = new ByteList();
        list.setSize(base);

        for (int c=0; c < count; c++) {
            assertTrue(list.addAll(src), "addAll " + c);
            assertEquals(base + (long)chunk * (c + 1), list.size(),
                         "size after addAll " + c);
        }

        // Every byte of every chunk reads back, the straddling one included.
        // A destination-side split which moved the wrong number of bytes shows
        // up here as a zero left behind in the gap it skipped.
        for (int c=0; c < count; c++) {
            for (int i=0; i < chunk; i++) {
                assertEquals((byte)(i + 1),
                             list.get(base + (long)chunk * c + i),
                             "chunk " + c + " element " + i);
            }
        }

        // Nothing before the run was disturbed
        assertEquals(0, list.get(base - 1), "byte before the run");
    }

    /**
     * Test that a list's storage holds everything the list says it holds,
     * whichever way it arrived at that size.
     *
     * <p>The bounds checks stop at the size, and an index becomes a sub-array
     * and an offset by arithmetic alone, so an element which the size admits
     * has to be one which the sub-arrays really have. Growth is what settles
     * that, and it has several ways in.
     */
    @Test
    public void testCapacityAlwaysBacksTheSize()
    {
        // Whatever the constructor was asked for, including the sizes which
        // are below the default first allocation
        for (final long initial : new long[] { 0, 1, 2, 10, 1000 }) {
            final ByteList list = new ByteList(initial);
            assertBacked(list, 0);

            list.append(new byte[] { (byte)1, (byte)2, (byte)3 });
            assertBacked(list, 3);
        }

        // And then every way of changing the size, in turn
        final ByteList list = new ByteList();
        assertBacked(list, 0);

        list.ensureCapacity(5000);
        assertBacked(list, 0);

        list.append(new byte[100]);
        assertBacked(list, 100);

        // Grown into the room which is already there, and shrunk again
        list.setSize(4000);
        assertBacked(list, 4000);
        list.setSize(7);
        assertBacked(list, 7);

        list.addAll(list);
        assertBacked(list, 14);

        list.add((byte)1);
        assertBacked(list, 15);

        list.clear();
        assertBacked(list, 0);

        // And the growths which have to reallocate, by each of the two routes
        list.append(new byte[9000]);
        assertBacked(list, 9000);

        list.setSize(20000);
        assertBacked(list, 20000);
    }

    /**
     * Assert that a list is the given size and that its storage holds that
     * many elements.
     *
     * @param list  The list to check.
     * @param size  The size which it should be.
     */
    private static void assertBacked(final ByteList list, final long size)
    {
        assertEquals(size, list.size());
        assertTrue(list.capacity() >= size,
                   "Capacity " + list.capacity() + " below size " + size);

        // Reading every element back walks the sub-arrays for the whole of the
        // size, which is what says the storage is really there. Comparing the
        // two numbers only says that the list thinks it is.
        assertEquals(size, (long)list.toArray().length);
    }
}
