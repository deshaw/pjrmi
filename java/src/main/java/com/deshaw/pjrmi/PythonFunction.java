package com.deshaw.pjrmi;

/**
 * A Python function which may be invoked with regular (non-kwarg) arguments
 * only.
 */
@FunctionalInterface
public interface PythonFunction<T>
{
    /**
     * Invoke a method on the object.
     *
     * @param args  The method's arguments, if any (may be null).
     *
     * @return the result of the call.
     *
     * @throws IllegalArgumentException if the arguments were incorrect.
     * @throws NoSuchMethodException    if there was no such method in the
     *                                  Python object.
     */
    public T invoke(final Object... args)
        throws Throwable;
}
