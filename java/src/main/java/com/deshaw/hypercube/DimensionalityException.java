package com.deshaw.hypercube;

/**
 * An exception which is thrown when a dimension is utilized in an
 * incorrect manner.
 */
public class DimensionalityException
    extends RuntimeException
{
    /**
     * Constructor.
     */
    public DimensionalityException(final String message)
    {
        super(message);
    }

    /**
     * Constructor.
     */
    public DimensionalityException(final String    message,
                                   final Throwable cause)
    {
        super(message, cause);
    }
}
