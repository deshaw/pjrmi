package com.deshaw.util;

/**
 * A pool of objects which are available for reuse by various threads.
 */
public abstract class Pool<T>
{
    /**
     * Get an instance of the element from the pool.
     *
     * @return the new element, or the result of newInstance() if none existed in
     *         the pool.
     */
    public abstract T getInstance();

    /**
     * Give an element back to the pool.
     *
     * @param t  The instance to release.
     *
     * @return whether the element was accepted back into the pool.
     */
    public abstract boolean releaseInstance(T t);

    /**
     * Create a new instance of a {@code T}.
     *
     * @return the new instance.
     */
    protected abstract T newInstance();

    /**
     * Prepare a {@code T} instance for reuse, making it like new.
     *
     * @param t  The instance to reuse.
     */
    protected abstract void prepareForReuse(T t);
}
