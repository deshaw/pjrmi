package com.deshaw.util;

/**
 * A pool of objects which are available for reuse by various threads. (This
 * class is threadsafe.) The idea is that you take and get objects from the
 * pool instead of new()'ing them.
 *
 * <p>We also allow ourselves to move pools between threads as they fill or
 * empty. This has the advantage of not needing many locks but still allows us
 * to pool objects between threads. In the case of no threading this is still
 * very fast.
 */
public abstract class SwappingPool<T>
    extends Pool<T>
{
    /**
     * A simple stack.
     */
    private static class Stack<T>
    {
        /**
         * The stack'd elements.
         */
        private final T myElements[];

        /**
         * The next free element's index. Zero means this stack is empty; if
         * it equals myElements.length then it's full.
         */
        private volatile int myNextElement;

        /**
         * Constructor.
         */
        @SuppressWarnings("unchecked")
        public Stack(final int size)
        {
            myElements = (T[]) new Object[size];
        }

        /**
         * Get an element, if any exists.
         */
        public T pop()
        {
            if (myNextElement > 0) {
                T element = myElements[--myNextElement];
                myElements[myNextElement] = null;
                return element;
            }
            else {
                return null;
            }
        }

        /**
         * Put an element on the stack; possibly failing it if the stack is
         * full.
         *
         * @return success
         */
        public boolean push(T t)
        {
            if (myNextElement < myElements.length) {
                myElements[myNextElement++] = t;
                return true;
            }
            else {
                return false;
            }
        }

        /**
         * Is this stack empty?
         */
        public boolean isEmpty()
        {
            return myNextElement == 0;
        }

        /**
         * Is this stack full?
         */
        public boolean isFull()
        {
            return myNextElement == myElements.length;
        }

        /**
         * How much room.
         */
        public int capacity()
        {
            return myElements.length;
        }

        /**
         * How many elements.
         */
        public int size()
        {
            return myNextElement;
        }
    }

    /**
     * The shared pool of elements.
     */
    private volatile Stack<T> mySharedStack;

    /**
     * The local pool of elements.
     */
    private final ThreadLocal<Stack<T>> myStack;

    /**
     * Constructor.
     */
    public SwappingPool(final int size)
    {
        this(size, 0);
    }

    /**
     * Constructor.
     */
    public SwappingPool(final int size, final int numPreallocate)
    {
        if (size < 1) {
            throw new IllegalArgumentException(
                "Pool size must be greater than zero, not " + size
            );
        }

        // Use a capped value
        final long prealloc = Math.min(size, numPreallocate);

        // Build
        mySharedStack = new Stack<>(size);
        for (int i=0; i < prealloc; i++) {
            mySharedStack.push(newInstance());
        }

        myStack =
            new ThreadLocal<Stack<T>>()
            {
                @Override
                protected Stack<T> initialValue()
                {
                    final Stack<T> stack = new Stack<>(size);
                    for (int i=0; i < prealloc; i++) {
                        stack.push(newInstance());
                    }
                    return stack;
                }
            };
    }

    /**
     * The max size of the pool.  Really, this is the capacity of the
     * shared stack plus the capacity of our thread-local pool, ignoring
     * all other thread-local pools.
     */
    public int capacity()
    {
        // One could argue that we should use
        //   mySharedStack.capacity() + myStack.get().capacity()
        // but, since all stacks have the same capacity, this should still
        // return the same value, but a little bit faster. Be careful to handle
        // overflow here (unlikely but...).
        final int capacity = (mySharedStack.capacity() << 1);
        return (capacity < 0) ? Integer.MAX_VALUE : capacity;
    }

    /**
     * The current number of elements in the pool.
     *
     * <p>Really, this returns the number of shared elements plus the number of
     * elements in our thread-local pool, ignoring all other thread-local pools.
     */
    public int size()
    {
        // Watch out for overflow (unlikely but...)
        final int size = mySharedStack.size() + myStack.get().size();
        return (size < 0) ? Integer.MAX_VALUE : size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getInstance()
    {
        // See if the local pool has any elements, if not then grab a load
        // from the shared pool
        Stack<T> local = myStack.get();
        T result = local.pop();
        if (result == null && !mySharedStack.isEmpty()) {
            // Swap this pool with the shared one in the hope that it still
            // has enough elements to satisfy us (note that the call to
            // isEmpty() is optimistically done outside the lock)
            synchronized (this) {
                Stack<T> tmp = mySharedStack;
                mySharedStack = local;
                local = tmp;
                myStack.set(local);
            }

            // Try again
            result = local.pop();
        }

        // Attempt to grab an element from the local pool, if it has anything
        // in it
        if (result == null) {
            // We failed to get an element, so we construct a new one and give
            // it back
            result = newInstance();
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean releaseInstance(T t)
    {
        // Ignore NOPs
        if (t == null) {
            return false;
        }

        // Tell the object to clean itself up now, so that if it's
        // holding any references to objects that can be garbage-collected,
        // the garbage collector gets a chance reclaim those objects
        // before they get moved to the old generation.
        prepareForReuse(t);

        // Attempt to return this element to the local pool
        Stack<T> local = myStack.get();
        if (local.push(t)) {
            return true;
        }
        else if (!mySharedStack.isFull()) {
            // Swap the stacks around in the hope that the shared one still
            // has room (note that the call to isFull() is optimistically done
            // outside the lock)
            synchronized (this) {
                Stack<T> tmp = mySharedStack;
                mySharedStack = local;
                local = tmp;
                myStack.set(local);
            }

            // Try again
            if (local.push(t)) {
                return true;
            }
        }

        // Must have failed to return it
        return false;
    }
}
