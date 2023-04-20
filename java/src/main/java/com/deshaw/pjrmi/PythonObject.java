package com.deshaw.pjrmi;

import java.io.IOException;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A wrapper around an object sitting in the Python interpreter.
 *
 * <p>A word about runtime errors. Since The Python objects may be mutated in
 * the background after a PythonObject wrapper is instantiated all errors are
 * thrown when you attempt interact with the object. That is to say, you won't
 * see a NoSuchMethodException until you attempt to call {@code invoke()}.
 */
public abstract class PythonObject
{
    /**
     * Constructor, forces local implementation since we only really want
     * this to be implemented by PythonObjectImpl.
     */
    protected PythonObject()
    {
        // Nothing
    }

    /**
     * Invoke a method on the object.
     *
     * @param methodName  The name of the method to invoke.
     * @param args        The method's arguments, if any (may be null).
     *
     * @return the resultant object.
     *
     * @throws Throwable if there was a problem.
     */
    public PythonObject invoke(final String    methodName,
                               final Object... args)
        throws Throwable
    {
        return invoke(PythonObject.class, methodName, null, args);
    }

    /**
     * Invoke a method on the object.
     *
     * @param methodName  The name of the method to invoke.
     * @param kwargs      The method's keyword arguments, if any (may be null).
     * @param args        The method's arguments, if any (may be null).
     *
     * @return the resultant object.
     *
     * @throws Throwable if there was a problem.
     */
    public PythonObject invoke(final String             methodName,
                               final Map<String,Object> kwargs,
                               final Object...          args)
        throws Throwable
    {
        return invoke(PythonObject.class, methodName, kwargs, args);
    }

    /**
     * Invoke a method on the object.
     *
     * @param <T>         The type of the result.
     * @param returnType  The return type expected of the method (may be null).
     * @param methodName  The name of the method to invoke.
     * @param args        The method's arguments, if any (may be null).
     *
     * @return the resultant object.
     *
     * @throws Throwable if there was a problem.
     */
    public <T> T invoke(final Class<T>  returnType,
                        final String    methodName,
                        final Object... args)
        throws Throwable
    {
        return invoke(returnType, methodName, null, args);
    }

    /**
     * Invoke a method on the object.
     *
     * @param <T>         The type of the result.
     * @param returnType  The return type expected of the method (may be null).
     * @param methodName  The name of the method to invoke.
     * @param kwargs      The method's keyword arguments, if any (may be null).
     * @param args        The method's arguments, if any (may be null).
     *
     * @return the resultant object.
     *
     * @throws Throwable if there was a problem.
     */
    public abstract <T> T invoke(final Class<T>           returnType,
                                 final String             methodName,
                                 final Map<String,Object> kwargs,
                                 final Object...          args)
        throws Throwable;

    /**
     * Get a field from the object.
     *
     * @param fieldName  The name of the field in the object.
     *
     * @return the field.
     *
     * @throws Throwable if there was a problem.
     */
    public PythonObject getattr(final String fieldName)
        throws Throwable
    {
        return getattr(PythonObject.class, fieldName);
    }

    /**
     * Get a field from the object.
     *
     * @param <T>        The type of the field.
     * @param fieldType  The field's type.
     * @param fieldName  The name of the field in the object.
     *
     * @return the field.
     *
     * @throws Throwable if there was a problem.
     */
    public abstract <T> T getattr(final Class<T> fieldType,
                                  final String   fieldName)
        throws Throwable;

    /**
     * Get a wrapper for this object which attempts to treat it as the given
     * Java interface.
     *
     * @param <T>    The new type.
     * @param klass  The new type.
     *
     * @return the field.
     *
     * @throws IOException if there was a problem.
     */
    public abstract <T> T asProxy(final Class<T> klass)
        throws IOException;

    // -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

    // Functionals sugar

    /**
     * Get a function by name bound to this instance as a PythonFunction. If
     * the function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The function return type.
     * @param returnType  The type to cast the result to.
     * @param methodName  The name of the method.
     *
     * @return the function.
     */
    public <T> PythonFunction<T> getMethod(final Class<T> returnType,
                                           final String   methodName)
    {
        return
            new PythonFunction<T>()
            {
                @Override
                public T invoke(final Object... args)
                    throws Throwable
                {
                    return PythonObject.this.invoke(returnType,
                                                    methodName,
                                                    args);
                }

                @Override
                public String toString()
                {
                    return returnType.getSimpleName() + " " +
                           methodName + "(*args)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a PythonKwargsFunction.
     * If the function does not exist then you will get an exception when
     * you attempt to invoke it.
     *
     * @param <T>         The function return type.
     * @param returnType  The type to cast the result to.
     * @param methodName  The name of the method.
     *
     * @return the function.
     */
    public <T> PythonKwargsFunction<T> getKwargsMethod(final Class<T> returnType,
                                                       final String   methodName)
    {
        return
            new PythonKwargsFunction<T>()
            {
                @Override
                public T invoke(final Map<String,Object> kwargs,
                                final Object...          args)
                    throws Throwable
                {
                    return PythonObject.this.invoke(returnType,
                                                    methodName,
                                                    kwargs,
                                                    args);
                }

                @Override
                public String toString()
                {
                    return returnType.getSimpleName() + " " +
                           methodName + "(*args, **kwargs)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a Function. If the
     * function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The function argument type.
     * @param <R>         The function return type.
     * @param returnType  The type to cast the result to.
     * @param methodName  The name of the method.
     *
     * @return the function.
     */
    public <T,R> Function<T,R> getFunction(final Class<R> returnType,
                                           final String   methodName)
    {
        return
            new Function<T,R>()
            {
                @Override
                public R apply(final T arg)
                {
                    try {
                        return PythonObject.this.invoke(returnType,
                                                        methodName,
                                                        null,
                                                        arg);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return returnType.getSimpleName() + " " + methodName + "(?)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a BiFunction. If the
     * function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The function's first argument type.
     * @param <U>         The function's second argument type.
     * @param <R>         The function return type.
     * @param returnType  The type to cast the result to.
     * @param methodName  The name of the method.
     *
     * @return the function.
     */
    public <T,U,R> BiFunction<T,U,R> getBiFunction(final Class<R> returnType,
                                                   final String   methodName)
    {
        return
            new BiFunction<T,U,R>()
            {
                @Override
                public R apply(final T arg1, final U arg2)
                {
                    try {
                        return PythonObject.this.invoke(returnType,
                                                        methodName,
                                                        null,
                                                        arg1, arg2);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return returnType.getSimpleName() + " " +
                           methodName + "(?, ?)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a Consumer. If the
     * function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The consumer's argument type.
     * @param methodName  The name of the method.
     *
     * @return the consumer.
     */
    public <T> Consumer<T> getConsumer(final String methodName)
    {
        return
            new Consumer<T>()
            {
                @Override
                public void accept(final T arg)
                {
                    try {
                        PythonObject.this.invoke(PythonObject.class,
                                                 methodName,
                                                 null,
                                                 arg);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return "void " + methodName + "(?)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a BiConsumer. If the
     * function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The consumer's first argument type.
     * @param <U>         The consumer's second argument type.
     * @param methodName  The name of the method.
     *
     * @return the consumer.
     */
    public <T,U> BiConsumer<T,U> getBiConsumer(final String methodName)
    {
        return
            new BiConsumer<T,U>()
            {
                @Override
                public void accept(final T arg1, final U arg2)
                {
                    try {
                        PythonObject.this.invoke(PythonObject.class,
                                                 methodName,
                                                 null,
                                                 arg1, arg2);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return "void " + methodName + "(?, ?)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a Predicate. If the
     * function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The predicate's argument type.
     * @param methodName  The name of the method.
     *
     * @return the predicate.
     */
    public <T> Predicate<T> getPredicate(final String methodName)
    {
        return
            new Predicate<T>()
            {
                @Override
                public boolean test (final T arg)
                {
                    try {
                        return PythonObject.this.invoke(Boolean.class,
                                                        methodName,
                                                        null,
                                                        arg);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return "boolean " + methodName + "(?)";
                }
            };
    }

    /**
     * Get a function by name bound to this instance as a BiPredicate. If
     * the function does not exist then you will get an exception when you
     * attempt to invoke it.
     *
     * @param <T>         The predicate's first argument type.
     * @param <U>         The predicate's second argument type.
     * @param methodName  The name of the method.
     *
     * @return the predicate.
     */
    public <T,U> BiPredicate<T,U> getBiPredicate(final String methodName)
    {
        return
            new BiPredicate<T,U>()
            {
                @Override
                public boolean test(final T arg1, final U arg2)
                {
                    try {
                        return PythonObject.this.invoke(Boolean.class,
                                                        methodName,
                                                        null,
                                                        arg1, arg2);
                    }
                    catch (Throwable t) {
                        throw new UndeclaredThrowableException(t);
                    }
                }

                @Override
                public String toString()
                {
                    return "boolean " + methodName + "(?, ?)";
                }
            };
    }
}
