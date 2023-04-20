package com.deshaw.pjrmi;

import java.util.Map;

/**
 * A Python function which may be invoked with arguments and key-word
 * arguments.
 */
@FunctionalInterface
public interface PythonKwargsFunction<T>
    extends PythonFunction<T>
{
    /**
     * {@inheritDoc}
     */
    @Override
    public default T invoke(final Object... args)
        throws Throwable
    {
        return invoke(null, args);
    }

    /**
     * Invoke a method on the object.
     *
     * @param kwargs  The method's keyword arguments, if any (may be null).
     * @param args    The method's arguments, if any (may be null).
     *
     * @return the result of the call.
     *
     * @throws IllegalArgumentException if the arguments were incorrect.
     * @throws NoSuchMethodException    if there was no such method in the
     *                                  Python object.
     */
    public T invoke(final Map<String,Object> kwargs,
                    final Object...          args)
        throws Throwable;
}
