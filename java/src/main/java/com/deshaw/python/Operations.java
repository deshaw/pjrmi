package com.deshaw.python;

/**
 * Pickle protocol opcodes. See the publicly availble Python pickle code for
 * their definitions.
 */
public enum Operations
{
    /** See Python. */ MARK           ((byte) '('),
    /** See Python. */ STOP           ((byte) '.'),
    /** See Python. */ POP            ((byte) '0'),
    /** See Python. */ POP_MARK       ((byte) '1'),
    /** See Python. */ DUP            ((byte) '2'),
    /** See Python. */ FLOAT          ((byte) 'F'),
    /** See Python. */ INT            ((byte) 'I'),
    /** See Python. */ BININT         ((byte) 'J'),
    /** See Python. */ BININT1        ((byte) 'K'),
    /** See Python. */ LONG           ((byte) 'L'),
    /** See Python. */ BININT2        ((byte) 'M'),
    /** See Python. */ NONE           ((byte) 'N'),
    /** See Python. */ PERSID         ((byte) 'P'),
    /** See Python. */ BINPERSID      ((byte) 'Q'),
    /** See Python. */ REDUCE         ((byte) 'R'),
    /** See Python. */ STRING         ((byte) 'S'),
    /** See Python. */ BINSTRING      ((byte) 'T'),
    /** See Python. */ SHORT_BINSTRING((byte) 'U'),
    /** See Python. */ UNICODE        ((byte) 'V'),
    /** See Python. */ BINUNICODE     ((byte) 'X'),
    /** See Python. */ APPEND         ((byte) 'a'),
    /** See Python. */ BUILD          ((byte) 'b'),
    /** See Python. */ GLOBAL         ((byte) 'c'),
    /** See Python. */ DICT           ((byte) 'd'),
    /** See Python. */ EMPTY_DICT     ((byte) '}'),
    /** See Python. */ APPENDS        ((byte) 'e'),
    /** See Python. */ GET            ((byte) 'g'),
    /** See Python. */ BINGET         ((byte) 'h'),
    /** See Python. */ INST           ((byte) 'i'),
    /** See Python. */ LONG_BINGET    ((byte) 'j'),
    /** See Python. */ LIST           ((byte) 'l'),
    /** See Python. */ EMPTY_LIST     ((byte) ']'),
    /** See Python. */ OBJ            ((byte) 'o'),
    /** See Python. */ PUT            ((byte) 'p'),
    /** See Python. */ BINPUT         ((byte) 'q'),
    /** See Python. */ LONG_BINPUT    ((byte) 'r'),
    /** See Python. */ SETITEM        ((byte) 's'),
    /** See Python. */ TUPLE          ((byte) 't'),
    /** See Python. */ EMPTY_TUPLE    ((byte) ')'),
    /** See Python. */ SETITEMS       ((byte) 'u'),
    /** See Python. */ BINFLOAT       ((byte) 'G'),

    // Protocol 2
    /** See Python. */ PROTO          ((byte) 0x80),
    /** See Python. */ NEWOBJ         ((byte) 0x81),
    /** See Python. */ EXT1           ((byte) 0x82),
    /** See Python. */ EXT2           ((byte) 0x83),
    /** See Python. */ EXT4           ((byte) 0x84),
    /** See Python. */ TUPLE1         ((byte) 0x85),
    /** See Python. */ TUPLE2         ((byte) 0x86),
    /** See Python. */ TUPLE3         ((byte) 0x87),
    /** See Python. */ NEWTRUE        ((byte) 0x88),
    /** See Python. */ NEWFALSE       ((byte) 0x89),
    /** See Python. */ LONG1          ((byte) 0x8a),
    /** See Python. */ LONG4          ((byte) 0x8b);

    private static final Operations[] BY_CODE = new Operations[256];
    static {
        for (Operations op : Operations.values()) {
            Operations.BY_CODE[((int)op.code) & 0xff] = op;
        }
    }

    /**
     * The identifier code for the operation.
     */
    public final byte code;

    /**
     * Get the {@link Operations} value for the given byte.\
     *
     * @param c  The operation's byte code.
     *
     * @return the enum value, or {@code null} if none was found.
     */
    public static Operations valueOf(byte c)
    {
        return BY_CODE[((int)c) & 0xff];
    }

    /**
     * Constructor.
     *
     * @param c  The operation's byte code.
     */
    private Operations(final byte c)
    {
        code = c;
    }
}
