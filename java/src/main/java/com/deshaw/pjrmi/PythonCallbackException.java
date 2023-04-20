package com.deshaw.pjrmi;

/**
 * An exception thrown by a Python callback.
 */
public class PythonCallbackException
    extends Exception
{
    /**
     * Constructor with a message and a cause.
     *
     * @param message  The exception message.
     * @param cause    The reason.
     */
    public PythonCallbackException(final String message,
                                   final Throwable cause)
    {
        super(message, cause);
    }
}
