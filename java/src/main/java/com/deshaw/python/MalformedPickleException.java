package com.deshaw.python;

/**
 * Exception thrown when a problem is encountered with a Python pickle.
 */
public class MalformedPickleException
    extends Exception
{
    private static final long serialVersionUID = -8373745679492469567L;

    /**
     * Constructor.
     *
     * @param message  The exception message.
     */
    public MalformedPickleException(String message)
    {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  The exception message.
     * @param cause    The original exception.
     */
    public MalformedPickleException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
