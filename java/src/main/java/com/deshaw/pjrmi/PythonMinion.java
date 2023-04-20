package com.deshaw.pjrmi;

import com.deshaw.python.PythonPickle;

import java.io.IOException;

/**
 * How one may call into Python.
 */
public interface PythonMinion
{
    /**
     * A special "annotation" class which allows users to specify that
     * arguments should be passed by value instead of by reference.
     *
     * <p>In order to send values by value they must be supported by the
     * {@link PythonPickle} class.
     */
    public static final class ByValue
    {
        /**
         * What we're going to take the value of.
         */
        private final Object myReference;

        /**
         * Constructor.
         *
         * @param reference the reference to use.
         */
        protected ByValue(final Object reference)
        {
            myReference = reference;
        }

        /**
         * Get the wrapped object.
         *
         * @return the wrappe object.
         */
        protected Object get()
        {
            return myReference;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "ByValue{" + myReference + "}";
        }
    }

    /**
     * Method to "annotate" a reference and denote that it should be sent by
     * value in invoke() calls.
     *
     * <p>In order to send values by reference they must be supported by the
     * {@link PythonPickle} class.
     *
     * @param reference  The reference to use.
     *
     * @return the value.
     */
    public static ByValue byValue(final Object reference)
    {
        return new ByValue(reference);
    }

    /**
     * Perform a Python exec() call in the global context.
     *
     * <p>The Python exec call performs dynamic execution of Python code in the
     * global context. This is what you want to use should you desire to do
     * things like {@code from foo.bar import baz}.
     *
     * @param string  The code to exec.
     *
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public void exec(final String string)
        throws IOException,
               PythonCallbackException;

    /**
     * Perform a python eval() call on an expression.
     *
     * @param string  The code to eval.
     *
     * @return the result of the eval.
     *
     * @throws ClassCastException      if there was a problem returning the result.
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public default Object eval(final String string)
        throws ClassCastException,
               IOException,
               PythonCallbackException
    {
        return eval(string, Object.class);
    }

    /**
     * Perform a python eval() call on an expression. The returnType tells
     * Python what we expect to get back.
     *
     * @param <T>         The type of the return value.
     * @param string      The code to eval.
     * @param returnType  The type to return the value as.
     *
     * @return the result of the eval.
     *
     * @throws ClassCastException      if there was a problem returning the result.
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public <T> T eval(final String string, final Class<T> returnType)
        throws ClassCastException,
               IOException,
               PythonCallbackException;

    /**
     * Set a variable to point to a Java value in the Python globals.
     *
     * @param name   The name of the value to set.
     * @param value  The value to set with.
     *
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public void setGlobalVariable(final String name, final Object value)
        throws IOException,
               PythonCallbackException;

    /**
     * Call a python function call with the given args.
     *
     * <p>If an argument is wrapped by {@code byValue()} then the
     * {@link PythonPickle} class will be used to send it over the wire,
     * otherwise arguments are passed by reference.
     *
     * @param functionName  The name of the function to invoke.
     * @param args          The function arguments.
     *
     * @return the result.
     *
     * @throws ClassCastException      if there was a problem returning the result.
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public default Object invoke(final String functionName,
                                 final Object... args)
        throws ClassCastException,
               IOException,
               PythonCallbackException
    {
        return invoke(functionName, Object.class, args);
    }

    /**
     * Call a python function call with the given args. The returnType tells
     * Python what we expect to get back.
     * <p>
     * If an argument is wrapped by {@code byValue()} then the {@link
     * PythonPickle} class will be used to send it over the wire, otherwise
     * arguments are passed by reference.
     *
     * @param <T>           The type of the return value.
     * @param functionName  The name of the function to invoke.
     * @param returnType    The type to return the value as.
     * @param args          The function arguments.
     *
     * @return the result.
     *
     * @throws ClassCastException      if there was a problem returning the result.
     * @throws IOException             if there was a problem.
     * @throws PythonCallbackException if calling Python resulted in an exception.
     */
    public <T> T invoke(final String    functionName,
                        final Class<T>  returnType,
                        final Object... args)
        throws ClassCastException,
               IOException,
               PythonCallbackException;

    /**
     * Get a wrapper around a Python object.
     *
     * @param string the object to get.
     *
     * @return the wrapper.
     *
     * @throws Throwable if there was a problem.
     */
    public default PythonObject getObject(final String string)
        throws Throwable
    {
        return getObject(string, null);
    }

    /**
     * Get a wrapper around a Python object.
     *
     * @param string The string to representing the object.
     * @param name   What to name the local instance, if anything.
     *
     * @return the result of the lookup, if any.
     *
     * @throws Throwable if there was a problem
     */
    public PythonObject getObject(final String string,
                                  final String name)
        throws Throwable;

    /**
     * Invokes the given function name with the provided arguments and
     * returns a wrapper around the resulting Python object.
     *
     * @param functionName The function to invoke.
     * @param args         The arguments to provide to the function.
     *
     * @return the result of the call, if any.
     *
     * @throws Throwable if there was a problem
     */
    public PythonObject invokeAndGetObject(final String functionName,
                                           final Object... args)
        throws Throwable;

    /**
     * Close the connection.
     */
    public void close();
}
