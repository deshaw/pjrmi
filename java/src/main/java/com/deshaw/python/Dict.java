package com.deshaw.python;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A more Pythonic version of the map: throws exceptions when trying to access
 * missing keys.
 */
public class Dict
    extends HashMap<Object,Object>
{
    /**
     * Exception thrown when the key is missing from the dictionary.
     */
    public static class MissingKeyException
        extends RuntimeException
    {
        private static final long serialVersionUID = -963449382298809244L;

        /**
         * Constructor.
         *
         * @param message  The exception message.
         */
        public MissingKeyException(String message)
        {
            super(message);
        }
    }

    // ----------------------------------------------------------------------

    private static final long serialVersionUID = 3896936112974357004L;

    /**
     * Return the value to which the specified key is mapped.
     *
     * @throws MissingKeyException if the key is not in the dict.
     */
    @Override
    public Object get(final Object key)
        throws MissingKeyException
    {
        final Object v = super.get(key);
        if (v != null) {
            return v;
        }
        if (containsKey(key)) {
            return null;
        }
        throw new MissingKeyException("Missing key '" + key + "'");
    }

    /**
     * Return the value to which the specified key is mapped or the specified
     * default value if the key is not in the dict.
     *
     * @param <T>   The type of the object to get.
     * @param key   The key to use for the lookup.
     * @param dflt  The value to return if no match was found for the key.
     *
     * @return the element for the given key.
     *
     * @throws ClassCastException if any associated value was not the same time
     *                            as the given default.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final Object key, final T dflt)
    {
        final Object v = super.get(key);
        if (v != null) {
            return (T) v;
        }
        if (containsKey(key)) {
            return null;
        }
        return dflt;
    }

    /**
     * Get the {@code int} value corresponding to the given key.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public int getInt(Object key)
    {
        return ((Number) get(key)).intValue();
    }

    /**
     * Get the {@code int} value corresponding to the given key, or the default
     * value if it doesn't exist.
     *
     * @param key   The key to use for the lookup.
     * @param dflt  The value to return if no match was found for the key.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public int getInt(Object key, int dflt)
    {
        return containsKey(key) ? getInt(key) : dflt;
    }

    /**
     * Get the {@code long} value corresponding to the given key.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public long getLong(Object key)
    {
        return ((Number) get(key)).longValue();
    }

    /**
     * Get the {@code long} value corresponding to the given key, or the default
     * value if it doesn't exist.
     *
     * @param key   The key to use for the lookup.
     * @param dflt  The value to return if no match was found for the key.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public long getLong(Object key, long dflt)
    {
        return containsKey(key) ? getLong(key) : dflt;
    }

    /**
     * Get the {@code double} value corresponding to the given key.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public double getDouble(Object key)
    {
        return ((Number) get(key)).doubleValue();
    }

    /**
     * Get the {@code double} value corresponding to the given key, or the default
     * value if it doesn't exist.
     *
     * @param key   The key to use for the lookup.
     * @param dflt  The value to return if no match was found for the key.
     *
     * @return the value associated with the given key.
     *
     * @throws ClassCastException if the associated value was not numeric.
     */
    public double getDouble(Object key, double dflt)
    {
        return containsKey(key) ? getDouble(key) : dflt;
    }

    /**
     * Get the {@link List} value corresponding to the given key.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key, if any.
     *
     * @throws ClassCastException if the associated value was not a {@link List}.
     */
    public List<?> getList(Object key)
    {
        return (List<?>) get(key);
    }

    /**
     * Return the value to which the key is mapped as a list of
     * strings. Non-null entries are converted to strings by calling
     * {@link String#valueOf(Object)}, and null entries are left
     * unchanged.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key, if any.
     *
     * @throws ClassCastException if the associated value was not a {@link List}.
     */
    @SuppressWarnings("unchecked")
    public List<String> getStringList(Object key)
    {
        final List<?> rawList = getList(key);
        if (rawList == null || rawList.isEmpty()) {
            return (List<String>)rawList;
        }

        final List<String> stringList = new ArrayList<>();
        for (Object raw : rawList) {
            stringList.add((raw != null) ? String.valueOf(raw) : null);
        }
        return stringList;
    }

    /**
     * Return the {@link NumpyArray} corresponding to the given key.
     *
     * @param key   The key to use for the lookup.
     *
     * @return the value associated with the given key, if any.
     *
     * @throws ClassCastException if the associated value was not a
     *                            {@link NumpyArray}.
     */
    public NumpyArray getArray(Object key)
    {
        return (NumpyArray) get(key);
    }

    /**
     * Return the {@link NumpyArray} corresponding to the given key and validate
     * its dimensions.
     *
     * <p>See also {@link NumpyArray#validateShape(String, int...)}.
     *
     * @param key            The key to use for the lookup.
     * @param expectedShape  The expected shape of the return value.
     *
     * @return the value associated with the given key, if any.
     *
     * @throws ClassCastException if the associated value was not a
     *                            {@link NumpyArray}.
     */
    public NumpyArray getArray(Object key, int... expectedShape)
    {
        NumpyArray array = getArray(key);
        array.validateShape(String.valueOf(key), expectedShape);
        return array;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Object rawEntry: entrySet()) {
            Map.Entry e = (Map.Entry) rawEntry;
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(' ');
            Object k = e.getKey();
            Object v = e.getValue();

            sb.append(stringify(k)).append(": ").append(stringify(v));
        }
        if (!first) {
            sb.append(' ');
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Wrap strings in quotes, but defer to {@link String#valueOf} for
     * other types of objects.
     *
     * @param o  The object to turn into a string.
     */
    private String stringify(Object o)
    {
        return (o instanceof CharSequence) ? "'" + o + "'"
                                           : String.valueOf(o);
    }
}
