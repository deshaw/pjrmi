package com.deshaw.io;

import com.deshaw.util.ByteList;

import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A unit test suite for testing the
 * {@link com.deshaw.io.ByteListOutputStream} class.
 */
public class ByteListOutputStreamTest
{
    /**
     * Test that a null list is refused, rather than blowing up on first use.
     */
    @Test
    public void testNullList()
    {
        assertThrows(NullPointerException.class, () -> {
            new ByteListOutputStream(null);
        });
    }

    /**
     * Test that the list which was handed in is the one which is written to,
     * rather than a copy of it, and that the caller can watch it fill up.
     */
    @Test
    public void testWritesGoToTheGivenList()
        throws IOException
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);

        out.write(0x12);
        assertEquals(1L,           list.size());
        assertEquals((byte)0x12,   list.get(0));
    }

    /**
     * Test writing single bytes.
     */
    @Test
    public void testWriteByte()
        throws IOException
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);

        for (int i=0; i < 256; i++) {
            out.write(i);
        }

        assertEquals(256L, list.size());
        for (int i=0; i < 256; i++) {
            assertEquals((byte)i, list.get(i));
        }
    }

    /**
     * Test that only the low eight bits of a written value are kept, per the
     * OutputStream contract.
     */
    @Test
    public void testWriteByteTruncates()
        throws IOException
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);

        out.write(0x1234);
        out.write(-1);

        assertEquals(2L,          list.size());
        assertEquals((byte)0x34,  list.get(0));
        assertEquals((byte)0xff,  list.get(1));
    }

    /**
     * Test writing whole arrays and subarrays.
     */
    @Test
    public void testWriteArray()
        throws IOException
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);

        out.write(new byte[] { (byte)1, (byte)2, (byte)3 });
        out.write(new byte[] { (byte)4, (byte)5, (byte)6, (byte)7 }, 1, 2);
        out.write(new byte[0]);

        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3,
                                       (byte)5, (byte)6 },
                          list.toArray());
    }

    /**
     * Test that a bad offset or length is refused.
     */
    @Test
    public void testWriteArrayBadBounds()
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);
        final byte[]               data = new byte[] { (byte)1, (byte)2 };

        assertThrows(IndexOutOfBoundsException.class,
                     () -> out.write(data, -1, 1));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> out.write(data, 0, -1));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> out.write(data, 1, 2));
        assertEquals(0L, list.size());
    }

    /**
     * Test that writes append to whatever the list already held.
     */
    @Test
    public void testAppendsToExistingContents()
        throws IOException
    {
        final ByteList list = new ByteList();
        list.append(new byte[] { (byte)9 });

        final ByteListOutputStream out = new ByteListOutputStream(list);
        out.write(new byte[] { (byte)8 });

        assertArrayEquals(new byte[] { (byte)9, (byte)8 }, list.toArray());
    }

    /**
     * Test that flushing and closing are no-ops which leave the stream usable.
     */
    @Test
    public void testFlushAndCloseAreNoOps()
        throws IOException
    {
        final ByteList             list = new ByteList();
        final ByteListOutputStream out  = new ByteListOutputStream(list);

        out.write(1);
        out.flush();
        out.write(2);
        out.close();
        out.write(3);

        assertArrayEquals(new byte[] { (byte)1, (byte)2, (byte)3 },
                          list.toArray());
    }

    /**
     * Test the case this class exists for: driving one from a
     * DataOutputStream, which is how a message payload gets built.
     */
    @Test
    public void testViaDataOutputStream()
        throws IOException
    {
        final ByteList         list = new ByteList();
        final DataOutputStream out  =
            new DataOutputStream(new ByteListOutputStream(list));

        out.writeByte(0x01);
        out.writeInt (0x02030405);
        out.writeLong(0x060708090a0b0c0dL);
        out.write(new byte[] { (byte)0x0e, (byte)0x0f });
        out.flush();

        assertEquals(1L + Integer.BYTES + Long.BYTES + 2, list.size());

        // Read it back the way the wire code does
        assertEquals((byte)0x01,               list.get(0));
        assertEquals(0x02030405,               list.getInt(1));
        assertEquals(0x060708090a0b0c0dL,      list.getLong(5));
        assertEquals((byte)0x0e,               list.get(13));
        assertEquals((byte)0x0f,               list.get(14));
    }
}
