package com.deshaw.util.concurrent;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * A long-to-long map which is thread-safe and does not employ locks.
 * 
 * <p>We use a variant on <a href="http://en.wikipedia.org/wiki/Cuckoo_hashing">
 * cuckoo hashing</a> to achieve this.
 * 
 * <p>Size operations are potentially noisy and expensive on this map so we
 * choose not to support them.
 * 
 * <p>Users should be aware that rehashing the table is a garbagy and time
 * consuming operation. As such, it is advised that a good initial size is
 * chosen if rehashing is to be avoided. A good rule of thumb is seems to be
 * around 1.5x the likely number of unique keys. The capacity will be rounded up
 * to a prime strictly larger than the next power of 2 after capacity.
 */
public class LongToLongConcurrentCuckooHashMap
{
    // The way cuckoo hashing works is that each key may occupy one of N
    // buckets. (For this implementation, N=2.) If a put operation fails for a
    // key then the entry in one of those buckets will bumped to its alternative
    // location; if that bucket is full then its contents are bumped; and so
    // forth.

    // We store the data in a bucket consisting of 3 things:
    //   KEY      -- The key
    //   VALUE    -- The value
    //   REVISION -- The bucket's revision
    //
    // We have a revision number which will change when a bucket's VALUE
    // changes. When doing a read of a bucket one should read the revision
    // number, then the bucket's key (to see if it matches your key) then the
    // value (which is what you want) then the revision number again (to see if
    // nothing has changed). If the revision number has changed then the read
    // was invalid and should be retried.
    //
    // Should a reader read a bucket in an inconsistent state (see below) or see
    // that the revision number changes then they should start again from
    // scratch.

    // We have certain invariants which hold for a bucket:
    //
    //   KEY  | VALUE | Meaning
    //  ------+-------+---------------
    //   NULL |  NULL | Bucket is free
    //  !NULL |  NULL | Bucket claimed but not okay to read
    //  !NULL | !NULL | Bucket full and okay to read
    //   NULL | !NULL | INVALIDATE STATE!
    //
    // Where NULL is a special value which acts as a semaphore.
    //
    // And some rules which must be obeyed overall:
    //  1. To claim an empty bucket a thread must CAS the KEY to non-NULL. This
    //     effectively locks the bucket.
    //  2. A VALUE in a bucket may only be set to non-NULL by the thread that
    //     claimed it.
    //  3. To claim a full bucket a thread must CAS the VALUE to non-NULL. This
    //     effectively locks the bucket.
    //  4. A KEY in a bucket may only be set to NULL by the thread that claimed
    //     it (either full or empty).
    //  5. Whenever a VALUE transitions from NULL to non-NULL or vice versa then
    //     the associated REVISION value will change.
    //  6. The REVISION value will only change while the VALUE is NULL.
    //  7. A KEY must never have a valid value in both of its locations at the
    //     same time.

    // Here and some examples of various operations ("N" is NULL):
    //
    //   Put:       KEY | VAL | REV
    //              ----+-----+----
    //               N  |  N  |  0
    //               1  |  N  |  0
    //               1  |  N  |  1
    //               1  |  9  |  1
    //
    //   Remove:    KEY | VAL | REV
    //              ----+-----+----
    //               1  |  9  |  2
    //               1  |  N  |  3
    //               N  |  N  |  3
    //
    //   Bump:      KEY | VAL | REV
    //              ----+-----+----
    //      Bkt1     1  |  9  |  2
    //      Bkt2     N  |  N  |  0
    //                  |     |
    //      Bkt1     1  |  9  |  2
    //      Bkt2     1  |  N  |  0
    //                  |     |
    //       .       1  |  9  |  2
    //       .       1  |  N  |  1
    //                  |     |
    //       .       1  |  N  |  2
    //       .       1  |  N  |  1
    //                  |     |
    //       .       1  |  N  |  3
    //       .       1  |  N  |  1
    //                  |     |
    //       .       1  |  N  |  3
    //       .       1  |  9  |  1
    //                  |     |
    //      Bkt1     N  |  N  |  3
    //      Bkt2     1  |  9  |  1
    //
    // The real things are a little more complex since they may involve touching
    // both buckets at once, but you get the general idea. See the actual
    // methods for deeper discussion.
    //
    // The main idea of the revision is for a reader to be able to tell if they
    // have managed to read the KEY and VALUE without one of them having changed
    // under their feet during the read. (It kinda fakes an atomic read.)
    // Semantically, a KEY cannot change without its VALUE changing too, even if
    // it eventually changes back to the same value as before; it will still
    // transition through being NULL.

    // =========================================================================

    /**
     * Provide a means to iterate through the contents of the map.
     * The idiom should be:
     *
     * <pre>
     *     try (Iterator itr = map.iterator()) {
     *         while (itr.next()) {
     *             System.out.println("key = "   + itr.currentKey());
     *             System.out.println("value = " + itr.currentValue());
     *         }
     *     }
     * </pre>
     *
     * The results of calling release() or close() more than once (or, by
     * implication, after the try-with-resources block) are undefined.
     * <p>
     * Note that this iterator is a little fuzzy in that it may return the same
     * element more than once or may miss it completely, if it happens to move
     * around in the map. Sorry!
     */
    public static class Iterator
        implements AutoCloseable
    {
        /**
         * The map we're working on.
         */
        private LongToLongConcurrentCuckooHashMap myMap = null;

        /**
         * The buckets which we are iterating over.
         */
        private AtomicLongArray myBuckets = null;

        /**
         * Our index into myBuckets.
         */
        private int myIndex = 0;

        /**
         * The current key.
         */
        private long myKey = NULL;

        /**
         * The current value.
         */
        private long myValue = NULL;

        /**
         * The next key.
         */
        private long myNextKey = NULL;

        /**
         * The next value.
         */
        private long myNextValue = NULL;

        /**
         * Constructor.
         */
        protected void init(final LongToLongConcurrentCuckooHashMap map,
                            final AtomicLongArray buckets)
        {
            myMap = map;
            myBuckets = buckets;
            myIndex = 0;
            findNext();
        }

        /**
         * Resets the iterator. After the reset, the iterator is in the same
         * state as a new iterator returned by the underlying collection's
         * {@code iterator()} method would be. In particular, iterating using
         * it would result in exactly the same sequence of elements to be
         * returned.
         */
        public void reset()
        {
            myIndex = 0;
            findNext();
        }

        /**
         * Returns true if it's safe to advance the iterator (that is,
         * if there are items remaining to be visited).
         */
        public boolean hasNext()
        {
            return (myNextKey != NULL);
        }

        /**
         * Advance the iterator to the next entry, if any.
         *
         * @return whether it was advanced (i.e. there was an entry).
         */
        public boolean next()
        {
            if (myNextKey == NULL) {
                return false;
            }

            // Copy over the next values
            myKey   = myNextKey;
            myValue = myNextValue;

            // And populate the new ones
            findNext();

            return true;
        }

        /**
         * Return the key of the current entry.
         */
        public long currentKey()
        {
            if (myKey == NULL) {
                throw new NoSuchElementException();
            }
            else {
                return myKey;
            }
        }

        /**
         * Return the value of the current entry.
         */
        public long currentValue()
        {
            if (myValue == NULL) {
                throw new NoSuchElementException();
            }
            else {
                return myValue;
            }
        }

        /**
         * Remove the current entry from the map.
         *
         * @throws NoSuchElementException if there was no current element to
         *                                remove.
         */
        public void remove()
        {
            if (myKey == NULL) {
                throw new NoSuchElementException();
            }
            else {
                myMap.remove(myKey);
                myKey = NULL; // we've removed this, current one is invalid now
            }
        }

        /**
         * Release the iterator back to its associated map. The pointer should
         * be considered invalidated after this call; one should call iterator()
         * on the map in order to get a valid iterator once more. The results of
         * calling this method (and/or release()) multiple times are undefined.
         */
        @Override
        public void close()
        {
            ourIterators.set(this);
        }

        /**
         * Find the next element.
         */
        private void findNext()
        {
            // Set key and value NULL
            myNextKey = myNextValue = NULL;

            // Attempt to read the current bucket, moving on to the next if it's
            // empty and so on
            for (/* nothing */;
                 myIndex < myBuckets.length() && myNextKey == NULL;
                 myIndex += 3)
            {
                // Make the read
                for (;;) {
                    final int kidx = myIndex;
                    final int vidx = kidx + 1;
                    final int ridx = kidx + 2;

                    final long rev = myBuckets.get(ridx);
                    final long key = myBuckets.get(kidx);
                    final long val = myBuckets.get(vidx);

                    // No entry?
                    if (key == NULL) {
                        break;
                    }

                    // Being touched?
                    if (val == NULL) {
                        continue;
                    }

                    // Good read?
                    if (myBuckets.get(ridx) != rev) {
                        continue;
                    }

                    // Okay!
                    myNextKey   = key;
                    myNextValue = val;
                    break;
                }
            }
        }
    }

    /**
     * How we pass and return various values to methods. This is a fudge since
     * we sometimes want to return multiple things and don't want to create
     * garbage or throw exceptions to do it.
     */
    private static class ResultData
    {
        public long result;

        /**
         * CTOR.
         */
        public ResultData()
        {
            clear();
        }

        /**
         * Make like new.
         */
        public void clear()
        {
            result = NULL;
        }
    }

    /**
     * The result of an operation. These are not binary.
     */
    private static enum Result { SUCCESS, FAILURE, RETRY }

    // -------------------------------------------------------------------------

    /**
     * The value which we use for NULL. You may not use this as a key or a
     * value. Any attempt to do so will yield an IllegalArgumentException.
     */
    public static final long NULL = Long.MIN_VALUE;

    /**
     * An array of prime numbers, each one roughly double the previous one.
     * These are strictly bigger than their corresponding power of 2. This goes
     * all the way up to around 2^31 / 3; the largest capacity which we can
     * have.
     */
    private static final int[] PRIMES = {
        3, 7, 11, 17, 37, 67, 131, 257, 521, 1031, 2053, 4099, 8209, 16411,
        32771, 65537, 131101, 262147, 524309, 1048583, 2097169, 4194319,
        8388617, 16777259, 33554467, 67108879, 134217757, 268435459, 536870923,
        715827881
    };

    /**
     * Our iterators.
     */
    private static final ThreadLocal<Iterator> ourIterators =
        ThreadLocal.withInitial(Iterator::new);

    /**
     * The calling values which we use (per thread).
     */
    private static final ThreadLocal<ResultData> ourResultData =
        new ThreadLocal<ResultData>()
        {
            @Override public ResultData get()
            {
                ResultData data = super.get();
                data.clear();
                return data;
            }

            @Override protected ResultData initialValue()
            {
                return new ResultData();
            }
        };

    // ----------------------------------------------------------------------

    /**
     * The array of buckets and so forth.
     *
     * As an implementation detail we hold all the values together in the array.
     * These are laid out as three longs thusly:
     *   [ KEY, VALUE, REVISION ]
     *
     * The pointer contained within this reference may be null if someone is
     * performing a rehash. Since this can change various methods should grab a
     * handle on it and work using that handle. They should then check to see if
     * the pointer changed under their feet when they're "done".
     */
    private final AtomicReference<AtomicLongArray> myBuckets =
        new AtomicReference<>();

    /**
     * The number of rehashes performed.
     */
    private volatile int myRehashCount = 0;

    // ----------------------------------------------------------------------

    /**
     * Our hashing method.
     */
    private static final int hash(final long key, final AtomicLongArray buckets)
    {
        // It's possible for multiple keys to generate the same hash. If this
        // occurs for 3 or more keys then they will all map to the same set of
        // buckets and we will get pathological failure. In order to avoid this
        // we salt the key with the table size when computing the hash, since
        // that will change when we rehash. This is, unfortunately, no guarantee
        // though.
        return Long.hashCode(key ^ buckets.length());
    }

    /**
     * Our hash-hashing method. Designed to shuffle the bits in a hash nicely
     * for us. Gained through a bit of googling and experimentation.
     */
    private static final int hashHash(int h)
    {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h << 4) ^ (h >>> 1);
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor with the default size.
     */
    public LongToLongConcurrentCuckooHashMap()
    {
        // Choose a starting capacity which is likely to have good properties
        // for bucketIndex()
        this(2 * 17 + 1);
    }

    /**
     * Constructor with a suggested starting capacity.
     */
    public LongToLongConcurrentCuckooHashMap(int capacity)
    {
        // Create the buckets and populate with that values which we require for
        // the invariants to hold etc. We can't create an array of more than
        // MAX_INT in size so cap at that.
        myBuckets.set(create(Math.min(Integer.MAX_VALUE / 3, capacity)));
    }

    /**
     * Get a value for a given key, or the NULL if it was not present.
     */
    public long get(final long key)
    {
        return get(key, NULL);
    }

    /**
     * Returns the value to which the specified key is mapped in this map.
     *
     * <p>Note that we purposely do _not_ throw an exception when we have a
     * missing value since exceptions are really expensive on 64bit x86
     * architectures and this stuff is meant to be very quick. Returning a
     * magic number sucks but there you go, if you want something cleaner then
     * use a real Map...</p>
     *
     * @param   key        a key in the map.
     * @param   nullValue  a value to return if key doesn't map to anything
     * @return  the value to which the key is mapped in this map;
     *          nullValue if the key is not mapped to any value in
     *          this map.
     */
    public long get(final long key, final long nullValue)
    {
        // Sanity
        if (key == NULL) {
            throw new IllegalArgumentException(
                "Cannot use NULL (" + NULL + ") as a key"
            );
        }

        // Keep trying until we succeed or fail, provided that the buckets still match
        for (;;) {
            // The buckets pointer might change from call to call, or may be
            // null, if someone is rehashing
            final AtomicLongArray buckets = myBuckets.get();
            if (buckets != null) {
                final int hash = hash(key, buckets);

                // Try both buckets (a bucket clash just means a mild waste of
                // effort)
                long result;
                if ((result = get(buckets, key, hash, true )) == NULL &&
                    (result = get(buckets, key, hash, false)) == NULL)
                {
                    result = nullValue;
                }

                // We won't check the myBuckets pointer here since we don't mind
                // too much if we happen to be looking at "outdated" data.
                // (Someone could have rehashed and then someone else could have
                // mutated the new buckets.) Since we're living in an threaded
                // world this is the sort of thing we might expect anyhow. (And
                // not checking the pointer means less contention on it.) Also,
                // rehashes should be rare in a perfect world.
                return result;
            }
        }
    }

    /**
     * <p>Maps the specified {@code key} to the specified {@code value} in this
     * map. The key cannot be {@code null}.
     *
     * <p>This function returns the old value if a mapping previously existed,
     * else it will return the given value.
     *
     * @param key     the map key.
     * @param value   the value.
     */
    public void put(final long key, final long value)
    {
        retryingPut(key, value, value, false, null);
    }

    /**
     * <p>Maps the specified {@code key} to the specified {@code value} in this
     * map. The key cannot be {@code null}.
     *
     * <p>This function returns the old value if a mapping previously existed,
     * else it will return the given {@code nullValue}.
     *
     * @param key     the map key.
     * @param value   the value.
     */
    public long put(final long key, final long value, final long nullValue)
    {
        return retryingPut(key, value, nullValue, false, null);
    }

    /**
     * <p>Removes any mapping from the given key its corresponding value, if it
     * was present.
     *
     * <p>This method does nothing if no mapping for the key exists in the map.
     *
     * @param key  the key to be removed.
     */
    public void remove(final long key)
    {
        remove(key, NULL);
    }

    /**
     * <p>Removes any mapping from the given key its corresponding value, if it
     * was present.
     *
     * <p>This method does nothing if no mapping for the key exists in the map.
     *
     * @param key        the key to be removed.
     * @param nullValue  a value to return if key doesn't map to anything.
     *
     * @return  the value to which the key had been mapped in this map,
     *          or {@code nullValue} if the key did not have a mapping.
     */
    public long remove(final long key, final long nullValue)
    {
        // Sanity
        if (key == NULL) {
            throw new IllegalArgumentException(
                "Cannot use NULL (" + NULL + ") as a key"
            );
        }

        // Keep trying until we succeed or fail
        for (;;) {
            // The buckets pointer might change from call to call, or may be
            // null, if someone is rehashing
            final AtomicLongArray buckets = myBuckets.get();
            if (buckets == null) {
                // Rehash in progress, loop around and try again
                continue;
            }

            // In order to perform the remove we must ensure that we
            // successfully remove (or not) from both buckets. It is possible
            // that a move() could shift the key's location under our feet while
            // we are checking each of them so we use the revisions to guard
            // against this.

            // Figure out offsets
            final int hash  = hash(key, buckets);
            final int k1idx = bucketIndex(buckets, hash, true);
            final int v1idx = k1idx + 1;
            final int r1idx = k1idx + 2;
            final int k2idx = bucketIndex(buckets, hash, false);
            final int v2idx = k2idx + 1;
            final int r2idx = k2idx + 2;

            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            // Keep trying to remove from both buckets until we're sure that
            // nothing changed while we were doing so
            long result = NULL;
            for (;;) {
                // Pull out the buckets' contents
                final long r1 = buckets.get(r1idx);
                final long k1 = buckets.get(k1idx);
                final long v1 = buckets.get(v1idx);
                final long r2 = buckets.get(r2idx);
                final long k2 = buckets.get(k2idx);
                final long v2 = buckets.get(v2idx);

                // Attempt to remove from bucket 1 by claiming it?
                if (k1 == key) {
                    if (v1 != NULL && buckets.compareAndSet(v1idx, v1, NULL)) {
                        // Still the right key?
                        if (buckets.get(k1idx) == k1) {
                            // Success! Bump up the revision, free the bucket
                            // and give back the value
                            buckets.getAndIncrement(r1idx);
                            buckets.set(k1idx, NULL);
                            result = v1;
                            break;
                        }
                        else {
                            // Restore the value and retry
                            buckets.set(v1idx, v1);
                            continue;
                        }
                    }
                    else {
                        // Bucket was in an invalid state or we failed to claim
                        // it, retry...
                        continue;
                    }
                }

                // Attempt to remove from bucket 2 by claiming it?
                if (k2 == key) {
                    if (v2 != NULL && buckets.compareAndSet(v2idx, v2, NULL)) {
                        // Still the right key?
                        if (buckets.get(k2idx) == k2) {
                            // Success! Bump up the revision, free the bucket
                            // and give back the value
                            buckets.getAndIncrement(r2idx);
                            buckets.set(k2idx, NULL);
                            result = v2;
                            break;
                        }
                        else {
                            // Restore the value and retry
                            buckets.set(v2idx, v2);
                            continue;
                        }
                    }
                    else {
                        // Bucket was in an invalid state or we failed to claim
                        // it, retry...
                        continue;
                    }
                }

                // If we got here then the key was not found; if things changed
                // them it might have been move()'d under our feet. If they
                // didn't then we really didn't find it.
                if (buckets.get(r1idx) == r1 &&
                    buckets.get(r2idx) == r2)
                {
                    break;
                }
            }

            // If the buckets haven't changed then we're all set now
            if (myBuckets.get() == buckets) {
                // See if that actually removed a value or not
                if (result == NULL) {
                    return nullValue;
                }
                else {
                    return result;
                }
            }
        }
    }

    /**
     * Returns the value to which the specified key is mapped in this map. This
     * is a little like {@code putIfAbsent()} except that you get back the
     * created value.
     *
     * @param   key        a key in the map.
     * @param   factory    the factory used to create a missing value.
     *
     * @return  the value to which the key is mapped in this map; creating
     *          if needbe.
     */
    public long computeIfAbsent(long key, LongUnaryOperator factory)
    {
        if (factory == null) {
            throw new NullPointerException("Given a null factory");
        }
        return retryingPut(key, NULL, NULL, true, factory);
    }

    /**
     * Put a value, only if it is not there already.
     *
     * @param key       The key to put with.
     * @param value     The value to put for the given key.
     * @param nullValue What to return if there was no existing value for the
     *                  given key.
     */
    public long putIfAbsent(long key, long value, long nullValue)
    {
        return retryingPut(key, value, nullValue, true, null);
    }

    /**
     * <p>Clears this map so that it contains no keys.
     *
     * <p>Note that this is <b>not atomic</b> and only best-effort if it is
     * called while other threads are mutating the table. We can not even
     * guarantee that elements put() into the table before clear() was called
     * will be cleared!
     *
     * <p>If you truly desire an empty table then you should either ensure that
     * no other threads are mutating it when clear() is called, or throw away
     * the table and create a new instance of the same capacity.
     */
    public void clear()
    {
        for (;;) {
            final AtomicLongArray buckets = myBuckets.get();
            if (buckets == null) {
                // A rehash is going on, retry
                continue;
            }

            // Perform the clear. We must obey the same semantics as other
            // mutation methods.
            for (int kidx = 0, len = buckets.length();
                 kidx < len;
                 kidx += 3)
            {
                // The other indices
                final int vidx = kidx + 1;
                final int ridx = kidx + 2;

                // Keep retrying for this element
                for (;;) {
                    // Pull out the current values
                    final long rev = buckets.get(ridx);
                    final long key = buckets.get(kidx);
                    final long val = buckets.get(vidx);

                    // A clean read?
                    if (buckets.get(ridx) != rev) {
                        // Nope, retry
                        continue;
                    }

                    // Anything there?
                    if (key == NULL) {
                        // Nope, we're done with this entry
                        break;
                    }

                    // Is the bucket claimed by another?
                    if (val == NULL) {
                        // Yes, retry
                        continue;
                    }

                    // Attempt to claim the bucket
                    if (!buckets.compareAndSet(vidx, val, NULL)) {
                        // Failed to claim, retry
                        continue;
                    }

                    // Successfully claimed. Update the revision and release as
                    // empty, and we're done with this element.
                    buckets.getAndIncrement(ridx);
                    buckets.set            (kidx, NULL);
                    break;
                }
            }

            // Make sure that the table didn't change under our feet
            if (myBuckets.get() == buckets) {
                // It's good, we're done!
                return;
            }
        }
    }

    /**
     * The capacity of the table.
     */
    public int capacity()
    {
        for (;;) {
            final AtomicLongArray buckets = myBuckets.get();
            if (buckets != null) {
                return buckets.length() / 3;
            }
        }
    }

    /**
     * <p>Tests if some key maps into the specified value in this map.
     * This operation is more expensive than the {@code containsKey}
     * method.
     *
     * <p>Note that this method is identical in functionality to
     * {@code containsValue}.
     *
     * @param      value   a value to search for.
     * @return     {@code true} if and only if some key maps to the
     *             {@code value} argument in this map as
     *             determined by the {@code equals()} method;
     *             {@code false} otherwise.
     * @see        java.util.Map
     */
    public boolean contains(final long value)
    {
        try (Iterator itr = iterator()) {
            while (itr.hasNext()) {
                if (itr.currentValue() == value) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the given
     * value.
     *
     * @param value the value to look for.
     */
    public boolean containsValue(final long value)
    {
        return contains(value);
    }

    /**
     * Tests if the specified object is a key in this map.
     *
     * @param key  the possible key.
     */
    public boolean containsKey(final long key)
    {
        return get(key, NULL) != NULL;
    }

    /**
     * Get the number of rehashes which this table has performed.
     */
    public int getRehashCount()
    {
        return myRehashCount;
    }

    /**
     * Note that this iterator is a little fuzzy in that it may return the same
     * element more than once or may miss it completely, if that element happens
     * to move around in the map owing to a {@code put()}. Sorry!
     */
    public Iterator iterator()
    {
        // Take ownership of any iterator. Note that we're dealing with a
        // ThreadLocal here so we don't need to do this atomically.
        Iterator itr = ourIterators.get();
        ourIterators.set(null);

        // If we got nothing back (since someone else in this thread is using
        // it) then create a new one
        itr = new Iterator();

        // Set it up and give it back
        for (;;) {
            final AtomicLongArray buckets = myBuckets.get();
            if (buckets != null) {
                itr.init(this, buckets);
                break;
            }
        }
        return itr;
    }

    /**
     * Iterate over all the keys and call this callback on them.
     *
     * <p>See the warning in {@link #iterator()} about how iteration over the map is
     * not entirely perfect.
     */
    public void onKeys(LongConsumer callback)
    {
        try (Iterator itr = iterator()) {
            while (itr.next()) {
                callback.accept(itr.currentKey());
            }
        }
    }

    /**
     * Iterate over all the values and call this callback on them.
     *
     * <p>See the warning in {@link #iterator()} about how iteration over the map is
     * not entirely perfect.
     */
    public void onValues(LongConsumer callback)
    {
        try (Iterator itr = iterator()) {
            while (itr.next()) {
                callback.accept(itr.currentValue());
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>See the warning in {@link #iterator()} about how iteration over the map is
     * not entirely perfect.
     */
    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append('[');

        try (Iterator itr = iterator()) {
            while (itr.next()) {
                if (sb.length() > 1) sb.append(',');
                sb.append(itr.currentKey())
                  .append("=>")
                  .append(itr.currentValue());
            }
        }

        sb.append(']');
        return sb.toString();
    }

    // ----------------------------------------------------------------------

    /**
     * Create a set of buckets with the given capacity.
     */
    private static AtomicLongArray create(final int capacity)
    {
        // We ensure that the capacity is prime here. This is important for the
        // way that bucketIndex works (it should yield fewer collisions).
        int cap = -1;
        for (int prime : PRIMES) {
            if (prime >= capacity) {
                cap = prime;
                break;
            }
        }

        // Did we find one?
        if (cap < 0) {
            throw new IllegalStateException(
                "Unable to allocate a new array of capacity " + capacity
            );
        }

        final long size = cap * 3;
        final AtomicLongArray buckets = new AtomicLongArray((int)size);
        for (int i=0; i < (int)size; i += 3) {
            final int keyIndex      = i;
            final int valueIndex    = keyIndex + 1;
            final int revisionIndex = keyIndex + 2;
            buckets.set(keyIndex,      NULL);
            buckets.set(valueIndex,    NULL);
            buckets.set(revisionIndex,    0);
        }
        return buckets;
    }

    /**
     * Compute the index of the bucket, as an offset into "buckets", for the
     * given hash.
     *
     * @param buckets The buckets which we want the index for.
     * @param hash    The hashcode.
     * @param primary Is this the primary bucket?
     */
    private int bucketIndex(final AtomicLongArray buckets,
                            final int hash,
                            final boolean primary)
    {
        // Ensure that the primary and secondary buckets are distinct
        final int capacity = buckets.length() / 3;
        final int b1 = ((hash & 0x7fffffff) % capacity) * 3;
        if (primary) {
            return b1;
        }
        else {
            // Fall back to the secondary bucket. It's very important that the
            // hashHash() method returns something which is going to produce a
            // nice distribution. (Otherwise our bucket chains will get cycles
            // in them.)
            final int b2 = ((hashHash(hash) & 0x7fffffff) % capacity) * 3;
            if (b1 == b2) {
                final int b3 = b1 + 3;
                return (b3 == buckets.length()) ? 0 : b3;
            }
            else {
                return b2;
            }
        }
    }

    /**
     * Get a value for a given key and its hash.
     *
     * @param buckets  The buckets to get the value from.
     * @param key      The key we're looking up.
     * @param hash     The hash to use for determining the bucket.
     *
     * @return The value, or NULL if not found.
     */
    private long get(final AtomicLongArray buckets,
                     final long            key,
                     final int             hash,
                     final boolean         isPrimary)
    {
        // Keep retrying until we succeed or fail
        for (;;) {
            // Read values
            final int kidx = bucketIndex(buckets, hash, isPrimary);
            final int vidx = kidx + 1;
            final int ridx = kidx + 2;

            final long curRev = buckets.get(ridx);
            final long curKey = buckets.get(kidx);

            // Key match?
            if (curKey != key) {
                return NULL;
            }

            // Read the value
            final long curValue = buckets.get(vidx);
            if (curValue == NULL) {
                // We're waiting for someone to fill in the value, go around and
                // try again
                continue;
            }

            // See if the revision is still good
            if (buckets.get(ridx) != curRev) {
                // Nope, someone changed it before in between us reading the key
                // and reading the value. Try again...
                continue;
            }

            // Okay, we go this far so everything checks out; give back the
            // value to denote success
            return curValue;
        }
    }

    /**
     * How put() is called, with retrying. Handles the value types of put()
     * call.
     *
     * <p>See put() for params' meaning
     */
    public long retryingPut(final long              key,
                            final long              value,
                            final long              nullValue,
                            final boolean           ifAbsent,
                            final LongUnaryOperator factory)
    {
        // Sanity
        if (!ifAbsent && (key == NULL || value == NULL)) {
            throw new IllegalArgumentException(
                "Cannot use NULL (" + NULL + ") for key or value: " +
                key + ", " + value
            );
        }

        // Remember what the buckets were at the start of this call. This allows
        // us to detect a rehash.
        final AtomicLongArray origBuckets = myBuckets.get();

        // Used to call
        final ResultData data = ourResultData.get();

        // Used to remember what we got back as the very first successful call
        // to put(). If we happen to successfully call put() but then don't
        // return but instead retry because a rehash() was in progress, then we
        // might lose the old "value" value. So we remember it.
        boolean hadSuccess = false;
        long    oldValue   = NULL;

        // Keep retrying until we succeed
        for (;;) {
            // This might change between iterations
            final AtomicLongArray buckets = myBuckets.get();

            // See if someone is rehashing
            if (buckets == null) {
                // Yes. Loop around so as busy wait until they are done and we
                // get a valid handle on the buckets.
                continue;
            }

            // Attempt to put into either bucket, in a "random" order
            final int hash = hash(key, buckets);
            final boolean isPrimary = (((hash >> 1) ^ hash) & 0x1) == 0;
            Result result = put(data, buckets, hash,
                                key, value,
                                isPrimary, ifAbsent, factory);
            switch (result) {
            case RETRY:
                continue;

            case FAILURE:
                result = put(data, buckets,
                             hash, key, value,
                             !isPrimary, ifAbsent, factory);
                break;
            }

            // Worked?
            if (result == Result.SUCCESS) {
                // If this was our first successful attempt then we remember the
                // old value, since that's we will eventually want to return
                if (!hadSuccess) {
                    hadSuccess = true;
                    oldValue   = data.result;
                }

                // Success. Make sure that the buckets didn't change under our
                // feet (owing to a rehash) since, if they did, we potentially
                // just put our element into the void.
                if (buckets == myBuckets.get()) {
                    // We now expect data.value to hold the previous value. If
                    // this was non-NULL then it was an update, else it was an
                    // insert.
                    if (oldValue == NULL) {
                        return nullValue;
                    }
                    else {
                        return oldValue;
                    }
                }
            }
            else {
                // Bump a "random" bucket" and try again. We choose a maximum
                // recursion depth which is likely to be sane according to the
                // load factor (num empty buckets scaled down).
                final int index = bucketIndex(buckets, hash, (((hash >> 2) ^ hash) & 0x1) == 0);
                if (bump(buckets, key, index, 0, false) == Result.FAILURE) {
                    // We failed to bump; attempt to rehash instead. This
                    // invalidates "data".
                    rehash(buckets, data, 1);
                }
            }
        }
    }

    /**
     * Attempt to put a key-value pair into a bucket.
     *
     * We expect this method to be called twice, once for each bucket where we
     * want to put the value.
     *
     * @param data      The ResultData with key, value, etc.
     * @param buckets   The buckets to look in.
     * @param hash      The hash for the bucket which we want to populate.
     * @param key       The key to insert for.
     * @param value     The value to insert.
     * @param isPrimary Whether we want the primary bucket or not.
     * @param ifAbsent  Whether we are doing a blahIfAbsent() call.
     * @param factory   The computer for computeIfAbsent() or, null, for
     *                  putIfAbsent().
     *
     * @return Whether we were able to put the value into the desired bucket. If
     *         this is SUCCESS then the "result" in data will contain the
     *         bucket's old value. That will be NULL if the bucket was empty.
     */
    private Result put(final ResultData        data,
                       final AtomicLongArray   buckets,
                       final int               hash,
                       final long              key,
                       final long              value,
                       final boolean           isPrimary,
                       final boolean           ifAbsent,
                       final LongUnaryOperator factory)
    {
        // The cases which we must handle (in order of testing, such that
        // certain conditions will be true for latter tests):
        //   key1 == key  && key2 == key   :: Likely bump in progress; bail
        //                   key2 == key   :: Bucket2 contains the key; bail
        //   key1 != key  && key1 != NULL  :: Someone else has bucket1; bail
        //   key1 == NULL                  :: Attempt to claim empty
        //   key1 == key                   :: Attempt to claim existing

        final int k1idx = bucketIndex(buckets, hash, isPrimary);
        final int v1idx = k1idx + 1;
        final int r1idx = k1idx + 2;
        final long rev1 = buckets.get(r1idx);
        final long key1 = buckets.get(k1idx);

        // A different key in the bucket?
        if (key1 != NULL && key1 != key) {
            return Result.FAILURE;
        }

        // Our key in the other bucket?
        final int  k2idx = bucketIndex(buckets, hash, !isPrimary);
        final long key2  = buckets.get(k2idx);
        if (key2 == key) {
            return Result.FAILURE;
        }

        // Attempt to claim an empty bucket
        if (key1 == NULL && buckets.compareAndSet(k1idx, NULL, key)) {
            // Ensure that bucket2 wasn't claimed at the same time
            if (buckets.get(k2idx) == key) {
                // Race to claim; drop ours and back off
                buckets.set(k1idx, NULL);
                return Result.RETRY;
            }
            else {
                // We successfully claimed the first bucket without another
                // thread also claiming the second. Now we bump up the revision
                // and fill in the value.

                // Different behaviour depending on whether we are doing put(),
                // putIfAbsent() or computeIfAbsent().
                final long putValue, retValue;
                if (ifAbsent && factory != null) {
                    // computeIfAbsent() returns the current value (computed or
                    // existing)
                    putValue = factory.applyAsLong(key);
                    retValue = putValue;
                }
                else {
                    // putIfAbsent() returns NULL if there was no existing
                    // value. For an empty slot this is the same as put().
                    putValue = value;
                    retValue = NULL;
                }

                // Do the actual put
                buckets.getAndIncrement(r1idx);
                buckets.set(v1idx, putValue);
                data.result = retValue;
                return Result.SUCCESS;
            }
        }

        // Is there a different key in this bucket?
        if (key1 != key) {
            return Result.FAILURE;
        }

        // We want to overwrite the existing value. We need to make sure
        // that we don't accidently wipe out the value associated with a
        // different key, should it have changed under our feet. We do this
        // by trying to claim the bucket by unsetting the current value and
        // then putting in our own, checking that the key does change as we
        // do this.

        // See what value is currently associated with this key. If it's valid
        // then attempt to claim the bucket (else someone is busy putting
        // themselves).
        final long curValue = buckets.get(v1idx);
        if (curValue != NULL && buckets.compareAndSet(v1idx, curValue, NULL)) {
            // See if the key is still what we think it should be. Note that
            // it's possible for keys to share the same value which is why this
            // test is required. We also check the revision since the key might
            // have changed to something else, and then back again. During this
            // time we might have read the other key's associated curValue.
            if (buckets.get(k1idx) == key && buckets.get(r1idx) == rev1) {
                // Successfully claimed the bucket in the right state; update
                // the revision and set the value to release it. If we were
                // doing a blahIfAbsent() call then we put it back to it's
                // original value (i.e. we don't overwrite, we discard the
                // setting value).
                buckets.getAndIncrement(r1idx);
                buckets.set(v1idx, ifAbsent ? curValue : value);
                data.result = curValue;
                return Result.SUCCESS;
            }
            else {
                // Someone took the bucket with a different key; put back
                // the value and denote failure (below)
                buckets.set(v1idx, curValue);
            }
        }

        // If we got here then we failed to make the put
        return Result.RETRY;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Bump a bucket's contents from a source index to the key's alternative
     * location. This should render the source index's bucket empty.
     *
     * @param buckets   The array of buckets
     * @param key       What we want to put into the source bucket, once its
     *                  contents have been removed.
     * @param index     The index of the source bucket.
     * @param callDepth How deep we are along the "bump()" call chain.
     * @param isRehash  Whether this bump is happening inside a rehash() call.
     *
     * @return whether we failed to make the source bucket empty, or need to
     *         retry.
     */
    private Result bump(final AtomicLongArray buckets,
                        final long            key,
                        final int             index,
                        final int             callDepth,
                        final boolean         isRehash)
    {
        // See if we've been recursing too far. The max value is what seems to
        // be a vague sweet-spot from empirical testing. It's a trade off
        // between effort spent bumping and the size of the table.
        if (callDepth > 64) {
            return Result.FAILURE;
        }

        // Alias for source ('s') key index
        final int ksidx = index;

        // Keep retrying until we succeed or fail
        for (;;) {
            // If we spot that there's a rehash going on (or has happened) then
            // bail out so as not to interfere with it
            if (!isRehash && myBuckets.get() != buckets) {
                // The caller needs to retry from scratch here, they will need
                // to handle the fact that there's a rehash
                return Result.RETRY;
            }

            // Look at the key which is currently in the bucket which we have
            // been told to get ready for our given key value
            final long ks = buckets.get(ksidx);
            if (ks == NULL || ks == key) {
                // Either the bucket is now empty or it actually contains our
                // key. Either way, we don't need to evict its contents to make
                // way for our key. We denote success.
                return Result.SUCCESS;
            }

            // Figure out the destination ('d') bucket. This will be the index
            // which doesn't equal "index", by definition.
            final int hash = hash(ks, buckets);
            final int primaryIdx = bucketIndex(buckets, hash, true);
            final int kdidx =
                (primaryIdx == index) ? bucketIndex(buckets, hash, false) :
                                        primaryIdx;

            // Okay, we have source and destination indices

            // Now we try to ensure that the destination has a spot for us
            try {
                final long kd = buckets.get(kdidx);
                if (kd != NULL) {
                    switch (bump(buckets, ks, kdidx, callDepth + 1, isRehash)) {
                    case FAILURE:
                        // Destination wasn't free and we failed to recursively
                        // bump in order to render it free
                        return Result.FAILURE;

                    case RETRY:
                        // Have another go
                        continue;
                    }
                }
            }
            catch (StackOverflowError e) {
                // Hopefully we should never see this but we obviously failed to
                // recurse
                return Result.FAILURE;
            }

            // In theory, the destination bucket is free at this point (though
            // someone might claim it before we do right here). We attempt to
            // make the move; if this fails we just go around again...
            if (moveBucket(buckets, ksidx, kdidx, ks, isRehash)) {
                // Success! We're done.
                return Result.SUCCESS;
            }
        }
    }

    /**
     * Move the contents of one index to another. The destination should be
     * empty; if not then this call will return false.
     *
     * @param buckets    The buckets to perform the move in.
     * @param fromIndex  The bucket to move from.
     * @param toIndex    The bucket to move to.
     * @param key        The key which we are trying to move.
     * @param isRehash   Whether move this is happening inside a rehash() call.
     *
     * @return whether the bucket's contents were successfully moved.
     */
    private boolean moveBucket(final AtomicLongArray buckets,
                               final int             fromIndex,
                               final int             toIndex,
                               final long            key,
                               final boolean         isRehash)
    {
        // In the below:
        //   'k' is key, 'v' is value & 'r' is revision;
        //   's' is source & 'd' is dest

        // Attempt to claim the destination bucket. We expect that it should be
        // empty (i.e. that the key will be NULL).
        final int kdidx = toIndex;
        if (!buckets.compareAndSet(kdidx, NULL, key)) {
            // We failed to claim it
            return false;
        }

        // We claimed the destination, now attempt to claim the source by
        // removing the value from it. We need to act like this is a read (i.e.
        // read the revision number first).
        final int  ksidx = fromIndex;
        final int  vsidx = ksidx + 1;
        final int  rsidx = ksidx + 2;
        final long rs    = buckets.get(rsidx); // read revision first
        final long ks    = buckets.get(ksidx); // then key
        final long vs    = buckets.get(vsidx); // and value

        // If the source value is NULL then someone else is messing with this
        // bucket.
        if (vs == NULL) {
            // Release the destination and denote failure
            buckets.set(kdidx, NULL);
            return false;
        }

        // Attempt to remove the value from the old bucket so as to lock it
        if (!buckets.compareAndSet(vsidx, vs, NULL)) {
            // Failed to claim the source bucket, release the destination and
            // denote failure
            buckets.set(kdidx, NULL);
            return false;
        }

        // Check the revision number, if that changed then the read was invalid
        if (buckets.get(rsidx) != rs) {
            // The read was invalid, give back the buckets and return failure
            buckets.set(vsidx, vs);
            buckets.set(kdidx, NULL);
            return false;
        }

        // Make sure that the source still has the right key (i.e. that we are
        // moving the right thing)
        if (ks != key) {
            // It no longer holds our key, give the buckets and return failure
            buckets.set(vsidx, vs);
            buckets.set(kdidx, NULL);
            return false;
        }

        // See if someone is performing a rehash (but only if we're not being
        // called from inside the rehash); we must not interfere with the
        // rehashing. We don't want to move a value since this could cause it to
        // be dropped or duplicated in the rehashed buckets.
        if (!isRehash && myBuckets.get() != buckets) {
            // We need to step away and restore things to their original state
            buckets.set(vsidx, vs);
            buckets.set(kdidx, NULL);
            return false;
        }

        // We will need these handles now
        final int vdidx = kdidx + 1;
        final int rdidx = kdidx + 2;

        // Okay, we managed to claim the destination and we know that the we
        // have the right value copied out. Before we release the buckets we
        // change their revision numbers, so as to keep semantics.
        buckets.getAndIncrement(rsidx);
        buckets.getAndIncrement(rdidx);

        // Now we can copy the value into the new bucket and free up the old one
        buckets.set(vdidx, vs);   // value into the destination
        buckets.set(ksidx, NULL); // free the source

        // Success!
        return true;
    }

    /**
     * A standard sort of rehash operation. Create more buckets and copy the
     * contents of the old one over to it. The old buckets are left as garbage
     * (since other threads might be messing about with them).
     *
     * Note that, it may not be the case that we _need_ to reallocate the
     * buckets when rehashing a cuckoo hash-table; it could simply be that our
     * hash functions are creating pathological loops and making it look like
     * we've run out of space. However, we currently assume that the hashes are
     * fine and that the table is just too overloaded. This makes other logic
     * simpler (since it can assume that the hashing functions have not
     * changed).
     *
     * @param buckets       The buckets to assign from.
     * @param data          The ResultData instance to use for calling helper
     *                      methods.
     * @param extraCapacity How much to initially increase the capacity by
     */
    private void rehash(final AtomicLongArray buckets,
                        final ResultData      data,
                        final int             extraCapacity)
    {
        // Attempt to take ownership of the buckets
        if (!myBuckets.compareAndSet(buckets, null)) {
            // Someone else beat us to it; assume that they did the work
            return;
        }

        // Create a new set of buckets and rehash into it. We keep retrying
        // until this succeeds or we run out of memory. We bump up the
        // "capacity" each time by 1 which will force create() to move on to the
        // next prime.
        final int origCapacity = buckets.length() / 3;
        for (int capacity = origCapacity + extraCapacity;
             /* Never ending */;
             capacity = capacity + 1)
        {
            // When we enter here we immediately increment the rehash count;
            // this needs to go up even if we fail (and have to rehash again).
            // This is safe up to update non-atomically since it is only written
            // when we exclusively hold the buckets.
            myRehashCount++;

            // Check for overflow/failure
            if (capacity < origCapacity) {
                throw new IllegalStateException("Failed to allocate new table");
            }

            // Allocate the new buckets and find the size we actually allocated.
            // This might throw an OOM error so ensure that we don't leave
            // things in a bad state if so.
            final AtomicLongArray newBuckets;
            try {
                newBuckets = create(capacity);
            }
            catch (OutOfMemoryError e) {
                // Don't leave things in an inconsistent state
                myBuckets.set(buckets);

                // And rethrow
                throw e;
            }
            capacity = newBuckets.length() / 3;

            // Copy in the new values
            boolean success = true;
            for (int i=0; success && i < origCapacity; i++) {
                // Someone may still be messing about with the table (if they
                // grabbed a handle on it before we did), so we must be careful
                // about reads
                long key, value;
                for (;;) {
                    // Read out the bucket's contents
                    final int  kidx = i * 3;
                    final int  vidx = kidx + 1;
                    final int  ridx = kidx + 2;
                    final long r = buckets.get(ridx);
                    key          = buckets.get(kidx);
                    value        = buckets.get(vidx);

                    // If the key was non-NULL but the VALUE was NULL then
                    // someone is messing with this bucket; loop around and
                    // re-read
                    if (key != NULL && value == NULL) {
                        continue;
                    }

                    // Ensure that we got a consistent read
                    if (buckets.get(ridx) == r) {
                        // Yes
                        break;
                    }
                }

                // Anything to do?
                if (key == NULL) {
                    // Nope. Move on to the next bucket in the original array.
                    continue;
                }

                // The key and value values are good now
                final int hash = hash(key, newBuckets);
                for (;;) {
                    Result result =
                        put(data, newBuckets, hash, key, value, true, false, null);
                    if (result == Result.FAILURE) {
                        result =
                            put(data, newBuckets, hash, key, value, false, false, null);
                    }

                    if (result == Result.RETRY) {
                        continue;
                    }

                    if (result == Result.FAILURE) {
                        // We failed to claim either bucket. Bump the element in the
                        // first one and try again.
                        final int index = bucketIndex(newBuckets, hash, (((hash >> 1) ^ hash) & 0x1) == 0);
                        switch (bump(newBuckets, key, index, 0, true)) {
                        case RETRY:
                            continue;

                        case FAILURE:
                            // We failed to bump; retry with larger capacity
                            success = false;
                            break;

                        case SUCCESS:
                            if (put(data, newBuckets, hash, key, value, true,  false, null) != Result.SUCCESS &&
                                put(data, newBuckets, hash, key, value, false, false, null) != Result.SUCCESS)
                            {
                                // That should have worked
                                throw new IllegalStateException(
                                    "Bumping should have created a space"
                                );
                            }
                            break;
                        }
                    }

                    // Done with retrying
                    break;
                }
            }

            // Did that work?
            if (success) {
                myBuckets.set(newBuckets);
                return;
            }

            // Else we go around and try again with a larger capacity...
        }
    }

    /**
     * Print the table to stdout. Debugging only...
     */
    public void debugDump(String prefix)
    {
        AtomicLongArray buckets = null;
        for (buckets = myBuckets.get(); buckets == null; buckets = myBuckets.get());
        debugDump(prefix, buckets);
    }

    /**
     * Print the table to stdout. Debugging only...
     */
    public void debugDump(String prefix, AtomicLongArray buckets)
    {
        for (int i=0; i < (buckets.length() / 3); i++) {
            final int k = i * 3;
            System.out.println(
                String.format("%-20s %-20s %10s %30s %30s %30s",
                              Thread.currentThread().toString(),
                              prefix,
                              i,
                              buckets.get(k),
                              buckets.get(k+1),
                              buckets.get(k+2))
            );
        }
        System.out.println(
            "-------------------------------------------------" +
            "-------------------------------------------------" +
            "-------------------------------------------------"
        );
    }

    // =========================================================================

    /**
     * Testing method.
     *
     *   java -classpath `destopdir`/java/classes \
     *       deshaw.common.primitive.concurrent.LongToLongConcurrentCuckooHashMap
     */
    public static void main(String args[])
    {
        final LongToLongConcurrentCuckooHashMap map =
            new LongToLongConcurrentCuckooHashMap(2);

        final int count = 10;
        final int mult  = 509;
        final int mod   = 1023;

        System.out.println("PUT...");
        for (int i=0; i < count; i++) {
            final int k = (i * mult + mult) % mod;
            map.put(k, -k);
            map.debugDump("PUT " + k);
        }
        System.out.println();

        System.out.println("PUT_IF_ABSENT...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.putIfAbsent(k, -k - 1, Integer.MIN_VALUE);
            map.debugDump("PUT_IF_ABSENT " + k);
            if (k != -v) {
                throw new IllegalStateException("PutIfAbsent mismatch");
            }
        }
        System.out.println();

        System.out.println("COMPUTE_IF_ABSENT...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.computeIfAbsent(k, x -> -k - 1);
            map.debugDump("COMPUTE_IF_ABSENT " + k);
            if (k != -v) {
                throw new IllegalStateException("PutIfAbsent mismatch");
            }
        }
        System.out.println();

        System.out.println("GET...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.get(k, NULL);
            System.out.println("GET " + k + " " + v);
            if (v != -k) {
                throw new IllegalStateException("Get mismatch");
            }
        }
        System.out.println();

        System.out.println("REMOVE...");
        for (int i=0; i < count; i++) {
            final int k = (i * mult + mult) % mod;
            map.remove(k);
            map.debugDump("REMOVE " + k);
        }

        System.out.println("PUT_IF_ABSENT_2...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.putIfAbsent(k, -k - 1, Integer.MIN_VALUE);
            map.debugDump("PUT_IF_ABSENT_2 " + k);
            if (v != Integer.MIN_VALUE) {
                throw new IllegalStateException("PutIfAbsent2 mismatch");
            }
        }
        System.out.println();

        System.out.println("GET_2...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.get(k, NULL);
            System.out.println("GET_2 " + k + " " + v);
            if (v != -k - 1) {
                throw new IllegalStateException("Get mismatch");
            }
        }
        System.out.println();

        System.out.println("REMOVE_2...");
        for (int i=0; i < count; i++) {
            final int k = (i * mult + mult) % mod;
            map.remove(k);
            map.debugDump("REMOVE_2 " + k);
        }

        System.out.println("COMPUTE_IF_ABSENT_2...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.computeIfAbsent(k, x -> -k - 1);
            map.debugDump("COMPUTE_IF_ABSENT_2 " + k);
            if (v != -k - 1) {
                throw new IllegalStateException("ComputeIfAbsent2 mismatch");
            }
        }
        System.out.println();

        System.out.println("GET_3...");
        for (int i=0; i < count; i++) {
            final int  k = (i * mult + mult) % mod;
            final long v = map.get(k, NULL);
            System.out.println("GET_3 " + k + " " + v);
            if (v != -k - 1) {
                throw new IllegalStateException("Get3 mismatch");
            }
        }
        System.out.println();

    }
}
