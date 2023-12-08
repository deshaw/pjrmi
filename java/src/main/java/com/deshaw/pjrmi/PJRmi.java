package com.deshaw.pjrmi;

import com.deshaw.hypercube.BooleanHypercube;
import com.deshaw.hypercube.DoubleHypercube;
import com.deshaw.hypercube.FloatHypercube;
import com.deshaw.hypercube.Hypercube;
import com.deshaw.hypercube.IntegerHypercube;
import com.deshaw.hypercube.LongHypercube;
import com.deshaw.io.BlockingPipe;
import com.deshaw.python.DType;
import com.deshaw.python.Operations;
import com.deshaw.python.PythonPickle;
import com.deshaw.util.ByteList;
import com.deshaw.util.Instrumentor;
import com.deshaw.util.StringUtil;
import com.deshaw.util.StringUtil.HashableSubSequence;
import com.deshaw.util.ThreadLocalStringBuilder;
import com.deshaw.util.concurrent.LockManager;
import com.deshaw.util.concurrent.VirtualThreadLock;
import com.deshaw.util.concurrent.VirtualThreadLock.VirtualThread;

import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;

import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.xerial.snappy.Snappy;

import static com.deshaw.util.StringUtil.appendHexByte;
import static com.deshaw.util.StringUtil.stackTraceToString;

/**
 * Python-to-Java RMI.
 *
 * <p>This code uses Java reflection to allow users in an external Python
 * process to call methods on and get values from object instances.
 *
 * <p>If you want thread safety here you have two options:
 * <ul>
 *   <li>Firstly you can program this into the Java code which you're attaching
 *       to at the deepest level. I.e. you just treat the PJRmi thread as
 *       another thread in the code and make your code threadsafe
 *       accordingly.</li>
 *   <li>Secondly, the PJRmi instance has a {@link PJRmiLockManager} that uses a
 *       global lock.  This manager will automatically synchronize incoming
 *       calls from clients if instructed to do so.  It is also available
 *       to the clients for additional manual synchronization amongst
 *       themselves.  If you want to share locking between multiple PJRmi
 *       instances, a constructor is provided that accepts an existing
 *       {@link PJRmiLockManager}.</li>
 * </ul>
 *
 * <p>The binary protocol employed is described in the comments of the inner
 * Connection class.
 */
public abstract class PJRmi
    extends Thread
{
    /**
     * How the PJRmi class manages locks. This is here so that external Python
     * processes can get their hands on various locks and coordinate with
     * one-another. This also manages locking of any global lock.
     *
     * <p>This class manages deadlock detection since it's possible for external
     * threads to acquire locks in ways which would cause them to hang forever.
     * As such it is important that <i>all</i> locking is done via this class's
     * methods.
     */
    public class PJRmiLockManager
        extends LockManager
    {
        /**
         * Our virtual thread lock.
         */
        private class PJRmiVirtualThreadLock
            extends VirtualThreadLock
            implements LockManagerLock
        {
            /**
             * Our list of threads which hold us, or are trying to acquire us.
             */
            private final ColouredList<Locker> myLockers;

            /**
             * The log level of this lock.
             */
            private volatile Level myLogLevel;

            /**
             * CTOR
             */
            public PJRmiVirtualThreadLock(CharSequence name)
            {
                super(name.toString()); // will throw if name is null
                myLockers = new ColouredList<>(4);
                myLogLevel = Level.FINEST;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public ColouredList<Locker> getLockers()
            {
                return myLockers;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void lock(final boolean isExclusive)
            {
                if (isExclusive) {
                    super.lock();
                }
                else {
                    throw new UnsupportedOperationException(
                        "Shared locks not supported"
                    );
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean tryLock(final boolean isExclusive)
            {
                if (isExclusive) {
                    return super.tryLock();
                }
                else {
                    throw new UnsupportedOperationException(
                        "Shared locks not supported"
                    );
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean tryLock(final boolean isExclusive,
                                   final long time,
                                   final TimeUnit unit)
                throws InterruptedException
            {
                if (isExclusive) {
                    return super.tryLock(time, unit);
                }
                else {
                    throw new UnsupportedOperationException(
                        "Shared locks not supported"
                    );
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void unlock(final boolean isExclusive)
            {
                if (isExclusive) {
                    super.unlock();
                }
                else {
                    throw new UnsupportedOperationException(
                        "Shared locks not supported"
                    );
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Condition newCondition(final boolean isExclusive)
            {
                if (isExclusive) {
                    return super.newCondition();
                }
                else {
                    throw new UnsupportedOperationException(
                        "Shared locks not supported"
                    );
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean isHeldByCurrentThread(final boolean isExclusive)
            {
                // We only support exclusive locks and so a shared lock can
                // never be held. We don't throw here since the parent
                // LockManager class calls this method as part of its checks.
                return isExclusive ? super.isHeldByCurrentThread() : false;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getHoldCountForCurrentThread(final boolean isExclusive)
            {
                // We only support exclusive locks and so a shared lock can
                // never be held.
                return isExclusive ? super.getHoldCount() : 0;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Level getLogLevel()
            {
                return myLogLevel;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void setLogLevel(final Level level)
            {
                if (level == null) {
                    throw new NullPointerException("Given a null level");
                }
                myLogLevel = level;
            }
        }

        // --------------------------------------------------------------------

        /**
         * The prefix of the global lock. We append a {@code \0} to the end of
         * it so that it hides the postfix when printing but still contains it.
         */
        private static final String GLOBAL_LOCK_PREFIX =
            "__PJRMI_GLOBAL_LOCK_-_DO_NOT_USE__\0";

        /**
         * Our global lock instance, if any.
         */
        private final LockManager.SafeLock myGlobalLock;

        /**
         * CTOR.
         */
        private PJRmiLockManager(final boolean useGlobalLock)
        {
            if (useGlobalLock) {
                myGlobalLock =
                    getExclusiveLockFor(
                        GLOBAL_LOCK_PREFIX + ourGlobalLockCount.getAndIncrement()
                    );
            }
            else {
                myGlobalLock = null;
            }
        }

        /**
         * Whether this class contains a global lock instance. There may be no
         * global lock if the PJRmi instance was specified not to create one.
         *
         * @return  whether the class contains a lock instance.
         */
        public boolean hasGlobalLock()
        {
            return myGlobalLock != null;
        }

        /**
         * Get the the global lock, if any.
         *
         * @return  the lock.
         */
        public LockManager.SafeLock getGlobalLock()
        {
            return myGlobalLock;
        }

        /**
         * Lock the global lock, if it exists.
         *
         * @return  the acquired lock.
         *
         * @throws DeadlockException if acquiring the lock would result in
         *         deadlock.
         */
        public SafeLock lockGlobal()
            throws DeadlockException
        {
            if (myGlobalLock != null) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.log(Level.FINEST,
                            currentThread() + " acquiring global lock",
                            new Throwable());
                }
                else if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(currentThread() + " acquiring global lock");
                }
                myGlobalLock.lock();
            }
            return myGlobalLock;
        }

        /**
         * Unlock the global lock, if it exists.
         */
        public void unlockGlobal()
        {
            if (myGlobalLock != null) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.log(Level.FINEST,
                            currentThread() + " dropping global lock",
                            new Throwable());
                }
                else if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(currentThread() + " dropping global lock");
                }
                myGlobalLock.unlock();
            }
        }

        /**
         * Whether the global lock, if any, is held by the current thread.
         *
         * @return whether the lock is held.
         */
        public boolean isGlobalHeldByCurrentThread()
        {
            if (myGlobalLock != null) {
                return myGlobalLock.isHeldByCurrentThread();
            }
            else {
                return false;
            }
        }

        /**
         * Invoke a runnable (or a function without arguments which returns
         * nothing) without holding the global lock.
         *
         * @param  runnable  The runnable to invoke.
         *
         * @throws DeadlockException            if there was a deadlock when
         *                                      reacquiring the lock.
         * @throws UndeclaredThrowableException if the given function threw an
         *                                      exception.
         */
        public void invokeWithoutGlobalLock(final Runnable runnable)
            throws DeadlockException,
                   UndeclaredThrowableException
        {
            invokeWithoutGlobalLock(
                new Function<Object,Object>() {
                    @Override public Object apply(Object notused) {
                        runnable.run();
                        return null;
                    }
                },
                null
            );
        }

        /**
         * Invoke an argumentless function without holding the global lock, and
         * return its result.
         *
         * @param  <T>       The type of the result.
         * @param  function  The function to invoke.
         *
         * @return the function's result.
         *
         * @throws DeadlockException            if there was a deadlock when
         *                                      reacquiring the lock.
         * @throws UndeclaredThrowableException if the given function threw an
         *                                      exception.
         */
        public <T> T invokeWithoutGlobalLock(final Supplier<T> function)
            throws DeadlockException,
                   UndeclaredThrowableException
        {
            return invokeWithoutGlobalLock(
                new Function<Object,T>() {
                    @Override public T apply(Object notused) {
                        return function.get();
                    }
                },
                null
            );
        }

        /**
         * Invoke a function with the given argument and return its result, all
         * without holding the global lock.
         *
         * @param  <T>       The type of the argument.
         * @param  <R>       The type of the result.
         * @param  function  The function to invoke.
         * @param  argument  The function's argument.
         *
         * @return the function's result.
         *
         * @throws DeadlockException            if there was a deadlock when
         *                                      reacquiring the lock.
         * @throws UndeclaredThrowableException if the given function threw an
         *                                      exception.
         */
        public <T,R> R invokeWithoutGlobalLock(final Function<T,R> function,
                                               final T             argument)
            throws DeadlockException,
                   UndeclaredThrowableException
        {
            // How many times this thread held the lock (it's reentrant). We
            // remember this so that we ensure that, when we exit this method, the
            // lock is held the same number of times as it was when we entered it.
            int lockCount = 0;

            try {
                // Unwind the lock, remembering how many times we had it held
                while (isGlobalHeldByCurrentThread()) {
                    unlockGlobal();
                    lockCount++;
                }

                // Now we can call the function
                return function.apply(argument);
            }
            catch (Throwable t) {
                throw new UndeclaredThrowableException(t);
            }
            finally {
                // Reacquire the lock again
                while (lockCount > 0) {
                    lockGlobal();
                    lockCount--;
                }
            }
        }

        /**
         * Sleep for a period of time, without holding the global lock.
         *
         * @param millis  How long to sleep for.
         *
         * @throws DeadlockException if there was a deadlock when reacquiring
         *                           the lock.
         */
        public void sleepWithoutGlobalLock(final long millis)
            throws DeadlockException
        {
            try {
                invokeWithoutGlobalLock(
                    () -> {
                        try {
                            Thread.sleep(millis);
                        }
                        catch (InterruptedException e) {
                            // Nothing
                        }
                    }
                );
            }
            catch (UndeclaredThrowableException e) {
                // This should never happen
                throw new RuntimeException(e);
            }
        }

        /**
         * Waits on the given monitor to be either interrupted or notified,
         * without holding the global lock.
         *
         * @param monitor The object to use for synchronizing the wait.
         *
         * @throws DeadlockException If there was a deadlock amongst locks
         *                           managed by LockManagers when re-acquiring
         *                           the lock.
         */
        public void waitWithoutGlobalLock(Object monitor)
            throws DeadlockException
        {
            try {
                invokeWithoutGlobalLock(() -> {
                    try {
                        monitor.wait();
                    }
                    catch (InterruptedException e) {
                        // Do nothing since we are done waiting at this point
                    }
                });
            }
            catch (UndeclaredThrowableException e) {
                // This should probably never happen
                throw (new RuntimeException(e));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SafeLock getSharedLockFor(final CharSequence lockName)
        {
            // We only support exclusive locks right now
            throw new UnsupportedOperationException(
                "Shared locks not supported"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected LockManagerLock newNamedLock(final CharSequence name)
        {
            return useWorkers() ? new PJRmiVirtualThreadLock(name) :
                                  super.newNamedLock(name);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Thread currentThread()
        {
            return useWorkers() ? VirtualThreadLock.getThread() :
                                  super.currentThread();
        }
    }
    private static final AtomicLong ourGlobalLockCount = new AtomicLong();
    // Used to distinguish global locks between instances. PJRmiLockManager only.

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * An annotation used to denote that a method returning an {@link Object}
     * should downcast that value to its actual type when being sent to the
     * Python side. This allows methods to mimic the return type semantics used
     * by generic classes.
     *
     * <p>PJRmi understands to do such downcasting for methods in generic
     * classes (e.g. {@code V Map<K,V>::get(K key)}) but there are non-generic
     * classes which might want to have similar semantics. Adding this
     * annotation allows the user to tell the Java side where this should
     * happen. E.g:
     <pre>
          // A class which contains various things
          class OpaqueBagOfStuff {
              // Get the first item to hand from the bag
              &#64;GenericReturnType
              public Object remove() {
                ...
              }

              // Put something into the bag
              public void add(Object something) {
                ...
              }
         }
      </pre>
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(value={ElementType.METHOD})
    public @interface GenericReturnType
    {
        // Nothing else
    }

    /**
     * An annotation used to denote that a method should only be called via
     * explicit binding, not dynamic. This means it is skipped if the binding is
     * ambiguous.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(value={ElementType.CONSTRUCTOR,ElementType.METHOD})
    public @interface ExplicitBinding
    {
        // Nothing else
    }

    /**
     * An annotation used to denote that a method takes a {@code kwargs} {@link
     * Map} as its last argument.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(value={ElementType.CONSTRUCTOR,ElementType.METHOD})
    public @interface Kwargs
    {
        /**
         * The list of accepted {@code kwarg} keys, if they are to be a limited
         * set. If not defined then any key is accepted.
         */
        public String value() default "";
    }

    /**
     * Flags which describe the system.
     */
    private enum Flags
    {
        USE_WORKERS((byte)(1 << 0));

        public final byte value;

        private Flags(final byte value)
        {
            this.value = value;
        }
    }

    /**
     * Our message types.
     */
    private enum MessageType
    {
        NONE                 ((byte) '\0',false), // Effectively "null"
        INSTANCE_REQUEST     ((byte) 'A', false), // Client to server
        ADD_REFERENCE        ((byte) 'B', false), // Client to server & Server to client
        DROP_REFERENCES      ((byte) 'C', false), // Client to server & Server to client
        TYPE_REQUEST         ((byte) 'D', false), // Client to server
        METHOD_CALL          ((byte) 'E', true ), // Client to server
        TO_STRING            ((byte) 'F', true ), // Client to server
        GET_FIELD            ((byte) 'G', true ), // Client to server
        SET_FIELD            ((byte) 'H', true ), // Client to server
        GET_ARRAY_LENGTH     ((byte) 'I', false), // Client to server
        NEW_ARRAY_INSTANCE   ((byte) 'J', false), // Client to server
        OBJECT_CAST          ((byte) 'K', false), // Client to server
        LOCK                 ((byte) 'L', false), // Client to server
        UNLOCK               ((byte) 'M', false), // Client to server
        INJECT_CLASS         ((byte) 'N', false), // Client to server
        GET_VALUE_OF         ((byte) 'O', true ), // Client to server
        GET_CALLBACK_HANDLE  ((byte) 'P', false), // Client to server
        CALLBACK_RESPONSE    ((byte) 'Q', false), // Client to server
        GET_PROXY            ((byte) 'R', false), // Client to server
        INVOKE_AND_GET_OBJECT((byte) 'S', false), // Client to server
        INJECT_SOURCE        ((byte) 'T', false), // Client to server
        OBJECT_REFERENCE     ((byte) 'a', false), // Server to client
        TYPE_DESCRIPTION     ((byte) 'b', false), // Server to client
        ARBITRARY_ITEM       ((byte) 'c', false), // Server to client
        EXCEPTION            ((byte) 'd', false), // Server to client
        ASCII_VALUE          ((byte) 'e', false), // Server to client
        EMPTY_ACK            ((byte) 'f', false), // Server to client
        ARRAY_LENGTH         ((byte) 'g', false), // Server to client
        UTF16_VALUE          ((byte) 'h', false), // Server to client
        PICKLE_BYTES         ((byte) 'i', false), // Server to client
        CALLBACK             ((byte) 'j', false), // Server to client
        PYTHON_EVAL_OR_EXEC  ((byte) 'k', false), // Server to client
        PYTHON_INVOKE        ((byte) 'l', false), // Server to client
        OBJECT_CALLBACK      ((byte) 'm', false), // Server to client
        GET_OBJECT           ((byte) 'n', false), // Server to client
        PYTHON_REFERENCE     ((byte) 'o', false), // Server to client
        GETATTR              ((byte) 'p', false), // Server to client
        SET_GLOBAL_VARIABLE  ((byte) 'q', false), // Server to client
        SHMDATA_BYTES        ((byte) 'r', false), // Server to client
        ;

        /**
         * All the possible types, keyed by ID.
         */
        private static final MessageType[] VALUES = new MessageType[0xff];
        static {
            for (MessageType mt : MessageType.values()) {
                VALUES[Byte.toUnsignedInt(mt.id)] = mt;
            }
        }

        /**
         * The unique identifier of this message type.
         */
        public final byte id;

        /**
         * Whether the global lock should be held when receiving with these
         * messages.
         *
         * <p>It is  important that  we get  this right  since holding  the lock
         * might result  in deadlock (for LOCK  and UNLOCK) and not  holding the
         * lock for operations which read or write from the non-PJRmi Java state
         * might   result  un   undefined/unexpected   client  behaviour   (e.g.
         * METHOD_CALL, GET_FIELD, SET_FIELD, etc.).
         */
        public final boolean shouldLockFor;

        /**
         * Get the MessageType instance for a given ID.
         */
        public static MessageType byId(final byte id)
        {
            MessageType result = VALUES[Byte.toUnsignedInt(id)];
            if (result == null) {
                throw new IllegalArgumentException(
                    "Bad MessageType value: '" + (char)id + "' " + (int)id
                );
            }
            return result;
        }

        /**
         * CTOR.
         */
        private MessageType(byte id, final boolean shouldLockFor)
        {
            this.id            = id;
            this.shouldLockFor = shouldLockFor;
        }
    }

    /**
     * Our extended version of the {@link PythonPickle} class, which
     * understands PJRmi-specific classes (like Hypercubes).
     */
    private static class PJRmiPythonPickle
        extends PythonPickle
    {
        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        protected void saveObject(final Object obj)
            throws UnsupportedOperationException
        {
            // Primitive hypercubes can be rendered directly as ndarrays
            if (obj instanceof BooleanHypercube ||
                obj instanceof DoubleHypercube  ||
                obj instanceof FloatHypercube   ||
                obj instanceof IntegerHypercube ||
                obj instanceof LongHypercube)
            {
                // We want to render a primitive hypercube as an ndarray but
                // of the right shape. We do this by sending the flattened
                // version and then reshaping it.
                final Hypercube<?> cube = (Hypercube<?>)obj;

                // We are going to call the reshape() function with a flattened
                // version of the cube and its shape
                saveGlobal("numpy", "reshape");

                // Get the flattened version of the cube, as an array, and
                // push it onto the stack
                if (obj instanceof BooleanHypercube) {
                    saveNumpyBooleanArray(((BooleanHypercube)obj).toArray());
                }
                else if (obj instanceof DoubleHypercube) {
                    saveNumpyDoubleArray(((DoubleHypercube)obj).toArray());
                }
                else if (obj instanceof FloatHypercube) {
                    saveNumpyFloatArray(((FloatHypercube)obj).toArray());
                }
                else if (obj instanceof IntegerHypercube) {
                    saveNumpyIntArray(((IntegerHypercube)obj).toArray());
                }
                else if (obj instanceof LongHypercube) {
                    saveNumpyLongArray(((LongHypercube)obj).toArray());
                }
                else {
                    throw new IllegalStateException(
                        "Got an unexpected cube type " + obj.getClass()
                    );
                }

                // Now push the shape onto the stack as the next argument. We
                // save this as a tuple, not a list, though reshape() will
                // accept both. Using a tuple is more "conventional".
                saveCollection(cube.getBoxedShape());

                // Turn the array and the shape into (a tuple of) arguments and
                // call the function
                write(Operations.TUPLE2);
                write(Operations.REDUCE);
            }
            else {
                super.saveObject(obj);
            }
        }
    }

    /**
     * The wire format to use when sending objects to python.
     */
    private enum PythonValueFormat
    {
        REFERENCE               ((byte) 'A'),
        RAW_PICKLE              ((byte) 'B'),
        SNAPPY_PICKLE           ((byte) 'C'),
        PYTHON_REFERENCE        ((byte) 'D'),
        SHMDATA                 ((byte) 'E'),
        BESTEFFORT_PICKLE       ((byte) 'F'),
        BESTEFFORT_SNAPPY_PICKLE((byte) 'G'),
        ;

        /**
         * All the possible types, keyed by ID.
         */
        private static final PythonValueFormat[] VALUES =
            new PythonValueFormat[0xff];
        static {
            for (PythonValueFormat rf : PythonValueFormat.values()) {
                VALUES[Byte.toUnsignedInt(rf.id)] = rf;
            }
        }

        /**
         * The unique identifier of this return format.
         */
        public final byte id;

        /**
         * Get the PythonValueFormat instance for a given ID.
         */
        public static PythonValueFormat byId(final byte id)
        {
            PythonValueFormat result = VALUES[Byte.toUnsignedInt(id)];
            if (result == null) {
                throw new IllegalArgumentException(
                    "Bad PythonValueFormat value: '" + (char)id + "' " + (int)id
                );
            }
            return result;
        }

        /**
         * CTOR.
         */
        private PythonValueFormat(byte id)
        {
            this.id = id;
        }
    }

    /**
     * Flags associated with a type description.
     */
    private static enum TypeFlags
    {
        IS_PRIMITIVE           (1 <<  0),
        IS_THROWABLE           (1 <<  1),
        IS_INTERFACE           (1 <<  2),
        IS_ENUM                (1 <<  3),
        IS_ARRAY               (1 <<  4),
        IS_FUNCTIONAL_INTERFACE(1 <<  5);

        public final int value;

        private TypeFlags(final int value)
        {
            this.value = value;
        }
    }

    /**
     * What a type looks like.
     */
    private class TypeDescription
    {
        /**
         * The actual class we're representing.
         */
        private final Class<?> myClass;

        /**
         * The name of the type a returned by Class.getName().
         */
        private final String myName;

        /**
         * The flags associated with this type.
         */
        private final Set<TypeFlags> myFlags = EnumSet.noneOf(TypeFlags.class);

        /**
         * The ID of this type as defined by TypeMapping. The 'void' type is
         * always zero (but we can't have a static member for that inside a
         * non-static class; sigh).
         */
        private final int myTypeId;

        /**
         * The IDs of this types which the class the of this type represents.
         */
        private final int[] mySupertypeIds;

        /**
         * The array of publically accessible fields.
         */
        private final Field[] myFields;

        /**
         * The fields this type has.
         */
        private final FieldDescription[] myFieldDescriptions;

        /**
         * The array of public constructors which this type has.
         */
        private final Constructor<?>[] myConstructors;

        /**
         * The relative binding specificity of the constructors with one-
         * another as determined by {@link compareBySpecificity(Constructor,Constructor)}.
         * It's keyed as one constructor index vs the other (matching
         * myConstructors).
         */
        private final byte[][] myConstructorSpecificities;

        /**
         * The descriptions of the public constructors which this type has.
         * These are modeled as methods which return a class instance as the
         * result.
         */
        private final MethodDescription[] myConstructorDescriptions;

        /**
         * The array of public methods which this type has (including methods
         * inherited from superclasses).
         */
        private final Method[] myMethods;

        /**
         * The relative binding specificity of the methods with one-another as
         * determined by {@link compareBySpecificity(Method, Method)}. It's
         * keyed as one method index vs the other (matching myMethods).
         */
        private final byte[][] myMethodSpecificities;

        /**
         * The descriptions of the public methods which this type has (including
         * methods inherited from superclasses).
         */
        private final MethodDescription[] myMethodDescriptions;

        /**
         * Optional samplers for instrumenting method calls.
         */
        private final Instrumentor[] myMethodInstrumentors;

        /**
         * CTOR.
         */
        public TypeDescription(final Class<?> klass, final int id)
        {
            if (klass == null) {
                throw new NullPointerException(
                    "Attempt to create TypeDescription for null"
                );
            }

            myClass = klass;

            myName = klass.getName();

            myTypeId = id;

            Class<?>   superClass = klass.getSuperclass();
            Class<?>[] interfaces = klass.getInterfaces();
            mySupertypeIds = new int[interfaces.length +
                                     ((superClass == null) ? 0 : 1)];
            /*scope*/ {
                int index = 0;
                if (superClass != null) {
                    mySupertypeIds[index++] =
                        myTypeMapping.getDescription(superClass).getTypeId();
                }
                for (Class<?> i : interfaces) {
                    mySupertypeIds[index++] =
                        myTypeMapping.getDescription(i).getTypeId();
                }
            }

            switch (myName) {
            case "void":
            case "boolean":
            case "byte":
            case "char":
            case "double":
            case "float":
            case "int":
            case "long":
            case "short":
                myFlags.add(TypeFlags.IS_PRIMITIVE);
                break;

            default:
                // Not a known primitive
                break;
            }

            // Other flags
            if (Throwable.class.isAssignableFrom(klass)) {
                myFlags.add(TypeFlags.IS_THROWABLE);
            }
            if (klass.isArray()) {
                myFlags.add(TypeFlags.IS_ARRAY);
            }
            if (isFunctionalInterface(klass)) {
                myFlags.add(TypeFlags.IS_FUNCTIONAL_INTERFACE);
            }
            if (klass.isEnum()) {
                myFlags.add(TypeFlags.IS_ENUM);
            }
            if (klass.isInterface()) {
                myFlags.add(TypeFlags.IS_INTERFACE);
            }

            // Put all the fields, constructors and methods into arrays, we'll
            // look at them later. We don't attempt to create the Description
            // instances right away since they might involve this type which
            // will cause us to recurse infinitely.

            // Walk all the fields. We ensure that shadowing variables correctly
            // override one-another.
            final Map<String,Field> fields = new HashMap<>();
            for (Field field : klass.getFields()) {
                final String   name      = field.getName();
                final Class<?> container = field.getDeclaringClass();
                final Field    current   = fields.get(name);

                // If we've not seen this field before, or if we have and the
                // previous one is in a class which is further up the
                // inheritance tree from this one, then we should save it in the
                // mapping.
                if (current == null ||
                    current.getDeclaringClass().isAssignableFrom(container))
                {
                    fields.put(name, field);
                }
            }

            myFields            = fields.values().toArray(new Field[fields.size()]);
            myFieldDescriptions = new FieldDescription[myFields.length];

            myConstructors = klass.getConstructors();
            myConstructorSpecificities = new byte[myConstructors.length][];
            for (int i=0; i < myConstructors.length; i++) {
                myConstructorSpecificities[i] = new byte[myConstructors.length];
            }
            myConstructorDescriptions  =
                new MethodDescription[myConstructors.length];

            // Determine the relative binding specificities. The i==j diagonal
            // will be left as zero, which is correct (a constructor is
            // incomparible with itself).
            for (int i=0; i < myConstructors.length; i++) {
                for (int j=0; j < i; j++) {
                    final int cmp =
                        ourMethodUtil.compareConstructorBySpecificity(
                            myConstructors[i],
                            myConstructors[j]
                        );
                    myConstructorSpecificities[i][j] = (byte)Math.signum( cmp);
                    myConstructorSpecificities[j][i] = (byte)Math.signum(-cmp);
                }
            }

            // The way we get methods differs depending on whether klass is a
            // class or an interface
            myMethods = klass.isInterface() ? findInterfaceMethods(klass)
                                            : findClassMethods    (klass);

            myMethodSpecificities = new byte[myMethods.length][];
            for (int i=0; i < myMethods.length; i++) {
                myMethodSpecificities[i] = new byte[myMethods.length];
            }
            myMethodDescriptions = new MethodDescription[myMethods.length];
            myMethodInstrumentors =
                instrumentMethodCalls() ? new Instrumentor[myMethods.length]
                                        : null;

            // Determine the relative binding specificities. The i==j diagonal
            // will be left as zero, which is correct (a method is incomparible
            // with itself).
            for (int i=0; i < myMethods.length; i++) {
                for (int j=0; j < i; j++) {
                    final int cmp =
                        ourMethodUtil.compareMethodBySpecificity(
                            myMethods[i],
                            myMethods[j]
                        );
                    myMethodSpecificities[i][j] = (byte)Math.signum( cmp);
                    myMethodSpecificities[j][i] = (byte)Math.signum(-cmp);
                }
            }

            // Talk to the animals
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Created " + this);
            }
        }

        /**
         * Get the ID of this type.
         */
        public int getTypeId()
        {
            return myTypeId;
        }

        /**
         * Get the Class represented by this TypeDescription.
         */
        public Class<?> getRepresentedClass()
        {
            return myClass;
        }

        /**
         * Get the name of this type, as returned by Class.getName();
         */
        public String getName()
        {
            return myName;
        }

        /**
         * Whether this type is 'void'.
         */
        public boolean isVoid()
        {
            return myTypeId == 0;
        }

        /**
         * Whether this type is a primitive one or not.
         */
        public boolean isPrimitive()
        {
            return myFlags.contains(TypeFlags.IS_PRIMITIVE);
        }

        /**
         * Whether this type is an array.
         */
        public boolean isArray()
        {
            return myFlags.contains(TypeFlags.IS_ARRAY);
        }

        /**
         * Get the flags associated with this type, as an int.
         */
        public int getFlagsValue()
        {
            int result = 0;
            for (TypeFlags flag : myFlags) {
                result |= flag.value;
            }
            return result;
        }

        /**
         * If this is an array, get the class of the component type.
         */
        public Class<?> getArrayComponentType()
        {
            return myClass.getComponentType();
        }

        /**
         * Get the number types which this type inherits from (might be
         * interfaces or classes).
         */
        public int getNumSupertypes()
        {
            return mySupertypeIds.length;
        }

        /**
         * Get the method for the given index.
         */
        public int getSupertypeId(int index)
        {
            return mySupertypeIds[index];
        }

        /**
         * Get the number of public constructors this type has. This will always
         * be zero for primitive types and arrays.
         */
        public int getNumConstructors()
        {
            return (isPrimitive() || isArray()) ? 0 : myConstructors.length;
        }

        /**
         * Get the method for the given index.
         */
        public MethodDescription getConstructor(int index)
        {
            MethodDescription desc = myConstructorDescriptions[index];
            if (desc == null) {
                // Need to create it, grab the corresponding Java method
                final Constructor<?> ctor = myConstructors[index];

                // The method arguments
                final Class<?>[]        args           = ctor.getParameterTypes();
                final Parameter[]       params         = ctor.getParameters();
                final TypeDescription[] argTypes       = new TypeDescription[args.length];
                final String[]          parameterNames = new String[args.length];
                for (int i=0; i < args.length; i++) {
                    final Class<?> arg = args[i];
                    argTypes[i]        = myTypeMapping.getDescription(arg);
                    parameterNames[i]  = params[i].getName();
                }
                final boolean isDeprecated =
                    (ctor.getAnnotation(Deprecated.class) != null);
                final boolean isExplicit =
                    (ctor.getAnnotation(ExplicitBinding.class) != null);
                final Kwargs kwargs = ctor.getAnnotation(Kwargs.class);

                // Now we can create it
                desc =
                    new MethodDescription(
                        index,
                        ctor.toString(),
                        false, // static
                        isDeprecated,
                        false, // default
                        isExplicit,
                        this,  // return type
                        false, // generic
                        argTypes,
                        parameterNames,
                        kwargs
                    );

                // And remember it
                myConstructorDescriptions[index] = desc;
            }
            return desc;
        }

        /**
         * Get the relative specificity of one constructor vs another.
         */
        public byte getRelativeConstructorSpecificity(final int idx1,
                                                      final int idx2)
        {
            return myConstructorSpecificities[idx1][idx2];
        }

        /**
         * Call a constructor with the given args.
         */
        public Object callConstructor(int index, Object[] arguments)
            throws Throwable
        {
            try {
                return myConstructors[index].newInstance(arguments);
            }
            catch (InvocationTargetException e) {
                // Throw the exception which came out of the call
                throw e.getTargetException();
            }
        }

        /**
         * Get the number of public methods this type has. This will always be
         * zero for primitive types.
         */
        public int getNumMethods()
        {
            return isPrimitive() ? 0 : myMethods.length;
        }

        /**
         * Get the method for the given index.
         */
        public MethodDescription getMethod(int index)
        {
            MethodDescription desc = myMethodDescriptions[index];
            if (desc == null) {
                // Need to construct it, grab the corresponding Java method
                final Method method = myMethods[index];

                // The method arguments
                final Class<?>[]        args           = method.getParameterTypes();
                final Parameter[]       params         = method.getParameters();
                final TypeDescription[] argumentTypes  = new TypeDescription[args.length];
                final String[]          parameterNames = new String[args.length];
                for (int i=0; i < args.length; i++) {
                    final Class<?> arg = args[i];
                    argumentTypes[i]   = myTypeMapping.getDescription(arg);
                    parameterNames[i]  = params[i].getName();
                }

                // Special hack for getClass(). Since we don't have Foo.class on
                // the Python side we allow ourselves to call getClass() in a
                // static context.
                final boolean isStatic =
                    (method.getName().equals("getClass") && args.length == 0) ||
                    ((method.getModifiers() & Modifier.STATIC) != 0);

                // Determine if the return type is a generic; this is a little
                // tricky since it requires heuristics and they might get
                // fooled. If the type is not a generic in some way then
                // toString() will return the same for both, otherwise we get
                // things like "E" or "java.util.Set<E>" back. This code may
                // give false positives or false negatives if the heuristics are
                // broken.
                final String rt  = method.getReturnType()       .toString();
                final String grt = method.getGenericReturnType().toString();
                boolean isGenericReturnType =
                    (method.getAnnotation(GenericReturnType.class) != null);
                if (!rt.equals(grt)) {
                    // Okay, the names differ. We now have to determine whether
                    // the grt is a type param (e.g. "E") or a parameterised
                    // type (e.g. "java.util.Set<E>").
                    if (grt.indexOf('<') < 0 && grt.indexOf('>') < 0) {
                        isGenericReturnType = true;
                    }
                }

                // Whether the method is marked as deprecated
                final boolean isDeprecated =
                    (method.getAnnotation(Deprecated.class) != null);

                // Whether we only do explicit binding
                final boolean isExplicit =
                    (method.getAnnotation(ExplicitBinding.class) != null);

                // Whether the instrument has been tagged as taking keyword
                // arguments
                final Kwargs kwargs = method.getAnnotation(Kwargs.class);

                // Now we can create it
                desc =
                    new MethodDescription(
                        index,
                        method.getName(),
                        isStatic,
                        isDeprecated,
                        method.isDefault(),
                        isExplicit,
                        myTypeMapping.getDescription(method.getReturnType()),
                        isGenericReturnType,
                        argumentTypes,
                        parameterNames,
                        kwargs
                    );

                // And remember it
                myMethodDescriptions[index] = desc;
            }
            return desc;
        }

        /**
         * Get the relative specificity of one method vs another.
         */
        public byte getRelativeMethodSpecificity(final int idx1,
                                                 final int idx2)
        {
            return myMethodSpecificities[idx1][idx2];
        }

        /**
         * Call a given method on a object instance with the given args.
         */
        public Object callMethod(int index, Object instance, Object[] arguments)
            throws Throwable
        {
            final MethodDescription desc = myMethodDescriptions[index];

            // See if we are instrumenting our method calls, if we are then the
            // myMethodInstrumentors array will be non-null;
            final Instrumentor instr;
            final long start;
            if (myMethodInstrumentors == null) {
                instr = null;
                start = -1;
            }
            else {
                // Grab the instr, lazily creating it if needbe
                if (myMethodInstrumentors[index] == null) {
                    instr = getInstrumentor(PJRmi.this.myName + ':' +
                                            myName + '#' + desc.getName() + "()");
                    instr.setIntervalMod(1);
                    myMethodInstrumentors[index] = instr;
                }
                else {
                    instr = myMethodInstrumentors[index];
                }

                // And actually do any instrumenting
                start = instr.start();
            }

            try {
                // Handle our special getClass() hack
                if (instance == null &&
                    desc.getName().equals("getClass") &&
                    desc.getNumArguments() == 0)
                {
                    return getRepresentedClass();
                }
                else {
                    return myMethods[index].invoke(instance, arguments);
                }
            }
            catch (InvocationTargetException e) {
                // Throw the exception which came out of the call
                throw e.getTargetException();
            }
            catch (Throwable t) {
                // Augment and rethrow
                throw new IllegalArgumentException(
                    "Failed to call method \"" + myMethods[index] + "\", " +
                    "with arguments " + Arrays.toString(arguments),
                    t
                );
            }
            finally {
                // And accumulate any instrumentation time
                if (instr != null) {
                    instr.end(start);
                }
            }
        }

        /**
         * The number of public fields this type has. For primitive types this
         * will always be zero. For arrays this will also be zero, we model
         * 'fields' in them as their contents.
         */
        public int getNumFields()
        {
            return (isPrimitive() || isArray()) ? 0 : myFields.length;
        }

        /**
         * Get the member type description for the given index.
         */
        public FieldDescription getField(int index)
        {
            FieldDescription desc = myFieldDescriptions[index];
            if (desc == null) {
                // Need to construct it, grab the corresponding Java method
                final Field field = myFields[index];
                desc =
                    new FieldDescription(
                        field.getName(),
                        myTypeMapping.getDescription(field.getType()),
                        (field.getModifiers() & Modifier.STATIC) != 0
                    );

                // And remember it
                myFieldDescriptions[index] = desc;
            }
            return desc;
        }

        /**
         * Get the value associated with a given field index from an instance.
         */
        public Object getField(int index, Object instance)
            throws IllegalArgumentException,
                   IllegalAccessException
        {
            return myFields[index].get(instance);
        }

        /**
         * Set the value associated with a given field index from an instance.
         */
        public void setField(int index, Object instance, Object value)
            throws IllegalArgumentException,
                   IllegalAccessException
        {
            myFields[index].set(instance, value);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myName + "<" + myTypeId + ">";
        }

        /**
         * Look for methods in a class which are publicly accessible.
         *
         * <p>We might need to do this if, for example, the class we have is not
         * itself declared "public". If we don't do this then calling a
         * non-accessible method will result in an {@link IllegalAccessException}
         * being thrown. This is more involved than what we do for interfaces.
         * See the {@code findInterfaceMethods()} sister method.
         */
        private Method[] findClassMethods(final Class<?> klass)
        {
            // This not be called for interfaces
            if (klass.isInterface()) {
                throw new IllegalArgumentException(
                    "Called for an interface: " + klass
                );
            }

            // Simple case if public
            if ((klass.getModifiers() & Modifier.PUBLIC) != 0) {
                return klass.getMethods();
            }

            // Otherwise we need to go off and hunt for them. We only save each
            // method with a given signature once so that we don't get clashes.
            final Map<MethodSignature,Method> methods = new HashMap<>();

            // Handle all the interfaces
            for (Class<?> iface : klass.getInterfaces()) {
                for (Method method : iface.getMethods()) {
                    final MethodSignature key =
                        new MethodSignature(
                            method.getName(),
                            Arrays.asList(method.getParameterTypes())
                        );
                    if (!methods.containsKey(key)) {
                        methods.put(key, method);
                    }
                }
            }

            // And the superclass
            if (klass.getSuperclass() != null) {
                // Recurse here, applying the same logic to the superclass
                for (Method method : findClassMethods(klass.getSuperclass())) {
                    final MethodSignature key =
                        new MethodSignature(
                            method.getName(),
                            Arrays.asList(method.getParameterTypes())
                        );
                    if (!methods.containsKey(key)) {
                        methods.put(key, method);
                    }
                }
            }

            // Given them back as an array
            return methods.values().toArray(new Method[methods.size()]);
        }

        /**
         * Get the accessible methods for a given interface.
         *
         * <p>Since these are in an interface we can do it in one fell swoop.
         * This is a lot simpler than what we have to do for concrete classes.
         * See the {@code findClassMethods()} sister method.
         */
        private Method[] findInterfaceMethods(final Class<?> klass)
        {
            // This method is only for interfaces
            if (!klass.isInterface()) {
                throw new IllegalArgumentException(
                    "Called for a non-interface: " + klass
                );
            }

            // We need to manually add in the Object methods to interfaces since
            // they won't appear in the result of getMethods(), even though they
            // are there for any object instance. This is needed for things like
            // toString() and hashCode() to work as expected, when you are given
            // back an interface instance from a method.
            //
            // We have to be slightly careful here to avoid adding methods from
            // Object which have been specified in the interface though (people
            // do this). We use the interface's method in preference to Object's
            // one since the interface method's return type might have been
            // overloaded to be more specific.
            final Method[] klassMethods = klass.getMethods();
            final Set<String> klassMethodNames =
                Arrays.stream(klassMethods)
                      .map(Method::getName)
                      .collect(Collectors.toSet());

            // Start with all of klass's methods
            final List<Method> methods =
                new ArrayList<>(Arrays.asList(klassMethods));

            // And add in Object's ones, ensuring that the method doesn't appear
            // in klass's methods. This is potentially O(n^2) but there are only
            // 9 methods in Object so it's not awful, especially since we do the
            // name-check first.
            for (Method objectMethod : OBJECT_METHODS) {
                final String objectMethodName = objectMethod.getName();
                boolean contains = false;
                if (klassMethodNames.contains(objectMethodName)) {
                    for (Method klassMethod : klassMethods) {
                        // We can't do a straight equals() here since the
                        // methods are associated with different declaring
                        // classes.
                        if (objectMethodName.equals(klassMethod.getName()) &&
                            Arrays.equals(objectMethod.getParameterTypes(),
                                          klassMethod .getParameterTypes()))
                        {
                            // We matched on the name and params, that is
                            // enough. The return type doesn't when it comes to
                            // binding of overloaded methods.
                            contains = true;
                            break;
                        }
                    }
                }

                // It's not in the interface's methods so it's safe to add
                if (!contains) {
                    methods.add(objectMethod);
                }
            }

            // And convert the list an array to return it
            return methods.toArray(new Method[methods.size()]);
        }

        /**
         * See if this class is a functional interface.
         */
        private boolean isFunctionalInterface(final Class<?> klass)
        {
            // Any class?
            if (klass == null) {
                return false;
            }

            // First look at the annotations of this class
            for (Annotation annotation : klass.getAnnotations()) {
                if (annotation instanceof FunctionalInterface) {
                    return true;
                }
            }

            // Else, recurse on the interfaces
            for (Class<?> iface : klass.getInterfaces()) {
                if (isFunctionalInterface(iface)) {
                    return true;
                }
            }

            // And the superclass
            return isFunctionalInterface(klass.getSuperclass());
        }
    }

    // Classes for handling Python's subscriptable, item assignment and related
    // functionality.

    /**
     * A class which represents a Python subscriptable container object which can
     * have items associated with specific keys. This is anything which
     * supports the {@code __getitem__()} operation as far as Python is
     * concerned.
     */
    public static interface PythonSubscriptable
    {
        /**
         * The Python {@code __getitem__()} method, which takes a multi-
         * dimensional key.
         *
         * <p>E.g. {@code foo = bar[0,1,3]} on the Python side.
         *
         * <p>Implemenations of this method should include the
         * {@link GenericReturnType} annotation in their signature so that the
         * Python side sees the right type, and not just {@link Object}.
         *
         * @param key  The Python-style lookup key.
         *
         * @return  The value which was found, if any.
         */
        @GenericReturnType
        public Object __getitem__(final Object[] key);
    }

    /**
     * A class which represents a Python key-based container object which can
     * have items associated with specific keys. This is anything which supports
     * the {@code __setitem__()} operation as far as Python is concerned.
     */
    public static interface PythonItemAssignable
    {
        /**
         * The Python {@code __setitem__()} method, which takes a multi-
         * dimensional key and its associated value.
         *
         * <p>E.g. {@code foo [0,1,2] = 3} on the Python side.
         *
         * @param key    The Python-style lookup key.
         * @param value  The value to set with.
         */
        public void __setitem__(final Object[] key, final Object value);
    }

    /**
     * What an array looks like in Java, as far as Python is concerned.
     */
    public interface ArrayLike
        extends Iterable,
                PythonSubscriptable,
                PythonItemAssignable
    {
        /**
         * Python length operator.
         *
         * @return the size of the (first dimension of the) array.
         */
        public long __len__();
    }

    /**
     * A class which wraps a Java array and presents it as a Python array-like.
     * This will eventually handle more complex array operations.
     *
     * <p>This class is a work-in-progress and more about fleshing out the
     * semantics of using ArrayLikes and slice operations right now. It might
     * even go away altogether...
     */
    public static class WrappedArrayLike
        implements ArrayLike
    {
        /**
         * The iterator for this array-like
         */
        private class ArrayIterator
            implements Iterator<Object>
        {
            /**
             * Current index.
             */
            private int myIndex = 0;

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean hasNext()
            {
                return myIndex < __len__();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Object next()
                throws NoSuchElementException
            {
                if (myIndex < __len__()) {
                    final Object value = Array.get(myArray, myIndex++);
                    return (value != null && value.getClass().isArray())
                        ? new WrappedArrayLike(value)
                        : value;
                }
                else {
                    throw new NoSuchElementException();
                }
            }
        }

        /**
         * The array which we wrap and will attempt to access in an array-like
         * manner.
         */
        private final Object myArray;

        /**
         * The number of dimension of the wrapped array.
         */
        private final int myNumDims;

        /**
         * The element's class.
         */
        private final Class<?> myElementType;

        /**
         * Constructor.
         *
         * @param array  The array to wrap.
         */
        public WrappedArrayLike(final Object array)
            throws IllegalArgumentException
        {
            // Check before we do anything
            if (array == null) {
                throw new NullPointerException("Given array was null");
            }
            if (!array.getClass().isArray()) {
                throw new IllegalArgumentException(
                    "Given value was a " +
                    array.getClass().getSimpleName() + ", not an array"
                );
            }

            // Safe to assign
            myArray = array;

            // Now determine other meta-data
            Class<?> klass = array.getClass();
            int ndim = 0;
            while (true) {
                // We have a new dimension
                ndim++;
                klass = klass.getComponentType();

                // If we have gotten to the last array of a multidimensional one
                // then we have elements in it
                if (!klass.isArray()) {
                    myElementType = klass;
                    break;
                }
                // Otherwise we go around again...
            }
            assert(myElementType != null);
            myNumDims = ndim;
        }

        /**
         * Get the array which we are wrapping.
         *
         * @return the wrapped array.
         */
        @GenericReturnType
        public Object getWrappedArray()
        {
            return myArray;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long __len__()
        {
            return Array.getLength(myArray);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @GenericReturnType
        public Object __getitem__(final Object[] key)
            throws ArrayIndexOutOfBoundsException,
                   IllegalArgumentException,
                   NullPointerException
        {
            // Validate the key before we use it
            assertKeyCorrectness(key);

            // Keep walking down and attempt to give back whatever we find for
            // the key
            Object value = myArray;
            for (Object k : key) {
                if (k instanceof Number) {
                    final int index = ((Number)k).intValue();
                    try {
                        value = Array.get(value, index);
                    }
                    catch (ArrayIndexOutOfBoundsException e) {
                        throw new ArrayIndexOutOfBoundsException(
                            index + " was not in the range [0.." +
                            Array.getLength(value) + ")"
                        );
                    }
                }
                // TODO handle PythonSlice in here too
                else {
                    throw new IllegalArgumentException(
                        "Don't know how to index with " + k + " in key " +
                        Arrays.toString(key)
                    );
                }
            }

            // If we have an array then we wrap that, else we just return what
            // we got
            return (value != null && value.getClass().isArray())
                ? new WrappedArrayLike(value)
                : value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void __setitem__(final Object[] key, final Object value)
            throws ArrayIndexOutOfBoundsException,
                   IllegalArgumentException
        {
            // Validate the key before we use it
            assertKeyCorrectness(key);

            // Keep walking down until we get to the penultimate array
            Object array = myArray;
            for (int i=0; i < key.length-1; i++) {
                final Object k = key[i];
                if (k instanceof Number) {
                    final int index = ((Number)k).intValue();
                    try {
                        array = Array.get(value, ((Number)k).intValue());
                    }
                    catch (ArrayIndexOutOfBoundsException e) {
                        throw new ArrayIndexOutOfBoundsException(
                            index + " was not in the range [0.." +
                            Array.getLength(value) + ")"
                        );
                    }
                }
                // TODO handle PythonSlice in here too
                else {
                    throw new IllegalArgumentException(
                        "Don't know how to index with " + k + " in key " +
                        Arrays.toString(key)
                    );
                }
            }

            // Now attempt to use the last key to set the value
            try {
                final Object k = key[key.length-1];
                if (k instanceof Number) {
                    final int index = ((Number)k).intValue();
                    try {
                        Array.set(array, ((Number)k).intValue(), value);
                    }
                    catch (ArrayIndexOutOfBoundsException e) {
                        throw new ArrayIndexOutOfBoundsException(
                            index + " was not in the range [0.." +
                            Array.getLength(array) + ")"
                        );
                    }
                }
                else {
                    throw new IllegalArgumentException(
                        "Don't know how to index with " + k + " in key " +
                        Arrays.toString(key)
                    );
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "Failed to set in array of " + array.getClass() + " " +
                    "with value of type " +
                    (value == null ? "null" : value.getClass().getSimpleName()),
                    e
                );
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<Object> iterator()
        {
            return new ArrayIterator();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            toStringBuilder(sb, myArray);
            return sb.toString();
        }

        /**
         * Recurse down into an array and keep printing its contents.
         */
        private void toStringBuilder(final StringBuilder sb, final Object array)
        {
            sb.append('[');
            if (array != null) {
                final boolean isArray =
                    array.getClass().getComponentType().isArray();
                for (int i = 0, len = Array.getLength(array); i < len; i++) {
                    final Object element = Array.get(array, i);
                    if (element.getClass().isArray()) {
                        toStringBuilder(sb, element);
                    }
                    else {
                        sb.append(element);
                    }
                    if (i < len-1) {
                        sb.append(", ");
                    }
                }
            }
            sb.append(']');
        }

        /**
         * Check that a key is correct.
         */
        private void assertKeyCorrectness(final Object[] key)
        {
            // Check key correctness
            if (key == null) {
                throw new NullPointerException("Given a null key");
            }
            if (key.length > myNumDims) {
                throw new ArrayIndexOutOfBoundsException(
                    "Too many indices for array; " +
                    "array is " + myNumDims + "-dimensional, " +
                    "but " + key.length + " were indexed"
                );
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * A specialisation of MethodUtil which handles noisy semantics of certain
     * Python types.
     */
    private static class PythonicMethodUtil
        extends MethodUtil
    {
        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isMoreSpecific(final Class<?> c1, final Class<?> c2)
        {
            // We say that Strings are more specific than char[]s or byte[]s,
            // since we want a Python str to bind to String.
            if (String.class.equals(c1) &&
                (char[].class.equals(c2) || byte[].class.equals(c2)))
            {
                return true;
            }

            // Defer to the parent method
            return super.isMoreSpecific(c1, c2);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isArrayMoreSpecific(final Class<?> c1, final Class<?> c2)
        {
            // We say that a char[] is more specific than a byte[] one, so that
            // a Python str is interpreted as a char[] where we have ambuiguity
            // in binding.
            if (char[].class.equals(c1) && byte[].class.equals(c2)) {
                return true;
            }

            // Defer to the parent method
            return super.isArrayMoreSpecific(c1, c2);
        }
    }

    /**
     * How we order methods. We put them in order of:
     *  1. Name
     *  2. Number of arguments
     *  3. Most specific
     * The final point (3) is the important one here since the Python code will
     * bind to the first match it finds. As such you want it to bind to, say,
     * {@code void Foo(String)} before it binds to {@code void Foo(Object)}
     * and {@code String Foo()} before {@code Object Foo()}.
     *
     * <p>Note that this somewhat breaks the Comparable contract since methods
     * may not have a canonical partial ordering. This is because we rely on
     * method arguments, and return types, to order the methods. Since there is
     * no "true" ordering of the classes within an inheritance tree, there can
     * be no "true" ordering of things based on this. Alternatively, one can say
     * that the ordering is not transitive; you potentially have a form of
     * "paper, scissors, stone" ordering in some cases. This can break things
     * like TimSort (used by Arrays.sort()) which rely on a true partial
     * ordering.
     */
    private static final class MethodComparator
        implements Comparator<Method>
    {
        /**
         * Our singleton instance.
         */
        public static final MethodComparator INSTANCE = new MethodComparator();

        /**
         * How we ensure that longs, ints, etc. are orders with the largest one
         * first. This is needed since we might be inferring the type on the
         * Python side and we want to avoid throwing a ImpreciseRepresentationError
         * in the wrong place.
         */
        private static final List<Class<?>> PRIMITIVE_PRIORITIES;
        static {
            PRIMITIVE_PRIORITIES = new ArrayList<>();
            PRIMITIVE_PRIORITIES.add(Double .TYPE);
            PRIMITIVE_PRIORITIES.add(Float  .TYPE);
            PRIMITIVE_PRIORITIES.add(Long   .TYPE);
            PRIMITIVE_PRIORITIES.add(Integer.TYPE);
            PRIMITIVE_PRIORITIES.add(Short  .TYPE);
            PRIMITIVE_PRIORITIES.add(Byte   .TYPE);
        }

        /**
         * Singleton CTOR.
         */
        private MethodComparator()
        {
            // Nothing
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compare(Method m1, Method m2)
        {
            // First by name
            int c = m1.getName().compareTo(m2.getName());
            if (c != 0) {
                return c;
            }

            // Next by number of arguments
            Class<?>[] p1 = m1.getParameterTypes();
            Class<?>[] p2 = m2.getParameterTypes();
            c = p1.length - p2.length;
            if (c != 0) {
                return c;
            }

            // Next, by specificity
            for (int i=0; i < p1.length; i++) {
                // We need to ensure that longs come before ints come before
                // shorts etc. We need this since long and int are both
                // assignable from one-another(!).
                final int i1 = PRIMITIVE_PRIORITIES.indexOf(p1[i]);
                final int i2 = PRIMITIVE_PRIORITIES.indexOf(p2[i]);
                if (i1 >= 0 && i2 >= 0 && i1 != i2) {
                    return i1 - i2;
                }

                // Now fall back assignment comparison
                if ( p2[i].isAssignableFrom(p1[i]) &&
                    !p1[i].isAssignableFrom(p2[i]))
                {
                    return -1;
                }
                if ( p1[i].isAssignableFrom(p2[i]) &&
                    !p2[i].isAssignableFrom(p1[i]))
                {
                    return 1;
                }
            }

            // Now by return type. Sort methods so that the ones which have
            // subclasses as their return type come first. This ensures that
            // the the "most" overridden method is called and the caller gets
            // an object with the appropriate return type.
            final Class<?> r1 = m1.getReturnType();
            final Class<?> r2 = m2.getReturnType();
            if ( r2.isAssignableFrom(r1) &&
                !r1.isAssignableFrom(r2))
            {
                return -1;
            }
            if ( r1.isAssignableFrom(r2) &&
                !r2.isAssignableFrom(r1))
            {
                return 1;
            }

            // Finally, as a tie-break, we sort by class name so as to ensure
            // that we have a total ordering (required for TimSort to work)
            for (int i=0; i < p1.length; i++) {
                final int cmp = p1[i].getName().compareTo(p2[i].getName());
                if (cmp != 0) {
                    return cmp;
                }
            }

            // Must be the same
            return 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object that)
        {
            return (this == that);
        }
    }

    /**
     * A name and list of classes which uniquely define a method signature.
     */
    private static class MethodSignature
    {
        /**
         * The name of the method.
         */
        private final String myName;

        /**
         * The classes of each of its parameters.
         */
        private final List<Class<?>> myParamTypes;

        /**
         * CTOR.
         */
        public MethodSignature(String name, List<Class<?>> paramTypes)
        {
            myName       = name;
            myParamTypes = paramTypes;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            return myName      .hashCode() ^
                   myParamTypes.hashCode();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof MethodSignature) {
                final MethodSignature that = (MethodSignature)obj;
                return this.myName      .equals(that.myName      ) &&
                       this.myParamTypes.equals(that.myParamTypes);
            }
            else {
                return false;
            }
        }
    }

    /**
     * The result of {@code readObject()}.
     */
    private static class ReadObjectResult
    {
        // The result is an offset and the object read
        public final int    offset;
        public final Object object;

        /**
         * CTOR.
         */
        public ReadObjectResult(final int offset, final Object object)
        {
            this.offset = offset;
            this.object = object;
        }
    }

    /**
     * What a field looks like.
     */
    private static class FieldDescription
    {
        /**
         * The method name.
         */
        private final String myName;

        /**
         * The field type.
         */
        private final TypeDescription myType;

        /**
         * Whether this is a static field.
         */
        private final boolean myIsStatic;

        /**
         * CTOR.
         */
        public FieldDescription(final String          name,
                                final TypeDescription type,
                                final boolean         isStatic)
        {
            myName     = name;
            myType     = type;
            myIsStatic = isStatic;

            // Talk to the animals
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Created " + this);
            }
        }

        /**
         * Get the method name.
         */
        public String getName()
        {
            return myName;
        }

        /**
         * Get the type.
         */
        public TypeDescription getType()
        {
            return myType;
        }

        /**
         * Whether this is a static method.
         */
        public boolean isStatic()
        {
            return myIsStatic;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myName;
        }
    }

    /**
     * Flags associated with a method description.
     */
    private static enum MethodFlags
    {
        IS_STATIC    ((short)(1 << 0)),
        IS_DEPRECATED((short)(1 << 1)),
        IS_EXPLICIT  ((short)(1 << 2)),
        HAS_KWARGS   ((short)(1 << 3)),
        UNUSED_04    ((short)(1 << 4)),
        UNUSED_05    ((short)(1 << 5)),
        UNUSED_06    ((short)(1 << 6)),
        UNUSED_07    ((short)(1 << 7)),
        IS_DEFAULT   ((short)(1 << 8));

        public final short value;

        private MethodFlags(final short value)
        {
            this.value = value;
        }
    }

    /**
     * How a method is being called.
     */
    private static enum SyncMode
    {
        SYNCHRONOUS((byte) 'S'),
        JAVA_THREAD((byte) 'J');

        /**
         * All the possible modes, keyed by ID.
         */
        private static final SyncMode[] VALUES = new SyncMode[0xff];
        static {
            for (SyncMode sm : SyncMode.values()) {
                VALUES[Byte.toUnsignedInt(sm.id)] = sm;
            }
        }

        /**
         * The unique identifier of this mode.
         */
        public final byte id;

        /**
         * Get the SyncMode instance for a given ID.
         */
        public static SyncMode byId(final byte id)
        {
            SyncMode result = VALUES[Byte.toUnsignedInt(id)];
            if (result == null) {
                throw new IllegalArgumentException(
                    "Bad SyncMode value: '" + (char)id + "' " + (int)id
                );
            }
            return result;
        }

        /**
         * CTOR.
         */
        private SyncMode(byte id)
        {
            this.id = id;
        }
    }

    /**
     * What we get back when we call a method asynchronously.
     */
    private static class MethodCallFuture
        implements Future<Object>
    {
        /**
         * Our description.
         */
        private String myDescription;

        /**
         * Our result.
         */
        private volatile Object myResult;

        /**
         * Whether the call is done.
         */
        private volatile boolean myIsDone;

        /**
         * Whether the call resulted in an exception, which should be thrown.
         */
        private volatile boolean myIsException;

        /**
         * CTOR.
         */
        public MethodCallFuture(final String desc)
        {
            myDescription = desc;
            myResult      = null;
            myIsDone      = false;
            myIsException = false;
        }

        /**
         * Called when done.
         */
        public void done(final Object  result,
                         final boolean isException)
        {
            myResult      = result;
            myIsException = isException;
            myIsDone      = true; // <-- Must be last
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // Can't be cancelled
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCancelled()
        {
            // Can't be cancelled
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isDone()
        {
            return myIsDone;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Calling this more than once will result in an exception being
         * thrown since the result is released once collected.
         */
        @Override
        public Object get()
            throws ExecutionException,
                   InterruptedException
        {
            try {
                return get(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException e) {
                throw new ExecutionException(
                    "Timed out but should not have",
                    e
                );
            }
        }

        /**
         * {@inheritDoc}
         *
         * <p>Calling this more than once will result in an exception being
         * thrown since the result is released once collected.
         */
        @Override
        public Object get(long timeout, TimeUnit unit)
            throws ExecutionException,
                   InterruptedException,
                   TimeoutException
        {
            // See how long to wait until
            final long untilMs;
            if (timeout < 0) {
                untilMs = System.currentTimeMillis();
            }
            else {
                // Need to handle overflow here
                final long end =
                    System.currentTimeMillis() + unit.toMillis(timeout);
                untilMs = (end < 0) ? Long.MAX_VALUE : end;
            }

            // Now do the get
            while (true) {
                // See if we got a result
                if (myIsDone) {
                    // Give back whatever it was
                    if (myIsException) {
                        if (myResult != null) {
                            throw new ExecutionException((Throwable)myResult);
                        }
                        else {
                            throw new ExecutionException(
                                "An unknown error occurred",
                                new Throwable()
                            );
                        }
                    }
                    else {
                        // Get the result and give it back. This may only be
                        // called once since we release the handle on the result
                        // so that it may be GC'd.
                        final Object result = myResult;
                        myResult =
                            new IllegalStateException("Result already collected");
                        myIsException = true;
                        myDescription += " [COLLECTED]";
                        return result;
                    }
                }
                else if (System.currentTimeMillis() >= untilMs) {
                    throw new TimeoutException();
                }
                else {
                    // Wait a microsecond
                    Thread.sleep(0, 1000);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myDescription;
        }
    }

    /**
     * What a method looks like.
     */
    private static class MethodDescription
    {
        /**
         * Method argument types as they come over the wire.
         */
        private static enum ArgumentType
        {
            REFERENCE ((byte) 'R'), // Value is a handle
            VALUE     ((byte) 'V'), // Value is raw bits
            SHMDATA   ((byte) 'S'), // Value is written in a shared file
            METHOD    ((byte) 'M'), // Value is a method handle
            LAMBDA    ((byte) 'L'), // Value is supplied by invoking a lambda
            ;

            /**
             * All the possible types, keyed by ID.
             */
            private static final ArgumentType[] VALUES = new ArgumentType[0xff];
            static {
                for (ArgumentType mt : ArgumentType.values()) {
                    VALUES[Byte.toUnsignedInt(mt.id)] = mt;
                }
            }

            /**
             * The unique identifier of this message type.
             */
            public final byte id;

            /**
             * Get the ArgumentType instance for a given ID.
             */
            public static ArgumentType byId(final byte id)
            {
                ArgumentType result = VALUES[Byte.toUnsignedInt(id)];
                if (result == null) {
                    throw new IllegalArgumentException(
                        "Bad ArgumentType value: '" + (char)id + "' " + (int)id
                    );
                }
                return result;
            }

            /**
             * CTOR.
             */
            private ArgumentType(byte id)
            {
                this.id = id;
            }
        }

        /**
         * The method's index; this will be unique in each category of "methods"
         * and "constructors".
         */
        private final int myIndex;

        /**
         * The method name.
         */
        private final String myName;

        /**
         * The {@link MethodFlags} associated with this method.
         */
        private final short myFlags;

        /**
         * The return type, may be VOID.
         */
        private final TypeDescription myReturnType;

        /**
         * Whether the return type is a type as specified as a parameter to a
         * generic declaration.
         */
        private final boolean myIsGenericReturnType;

        /**
         * The arguments, if any.
         */
        private final TypeDescription[] myArguments;

        /**
         * The parameter names, if any.
         */
        private final String[] myParameterNames;

        /**
         * The toString() representation.
         */
        private final String myString;

        /**
         * The keyword arguments to accept, if they are to be limited.
         */
        private final String[] myAcceptedKwargs;

        /**
         * Get any kwargs name array from a {@link Kwargs} instance.
         */
        private static String[] splitKwargs(final Kwargs kwargs)
        {
            if (kwargs         == null ||
                kwargs.value() == null ||
                kwargs.value().isEmpty())
            {
                return null;
            }

            final String[] split  = kwargs.value().split(",");
            final String[] result = new String[split.length];
            for (int i=0; i < split.length; i++) {
                result[i] = split[i].trim();
            }
            return result;
        }

        /**
         * CTOR.
         */
        public MethodDescription(final int               index,
                                 final String            name,
                                 final boolean           isStatic,
                                 final boolean           isDeprecated,
                                 final boolean           isDefault,
                                 final boolean           isExplicit,
                                 final TypeDescription   returnType,
                                 final boolean           isGenericReturnType,
                                 final TypeDescription[] arguments,
                                 final String[]          parameterNames,
                                 final Kwargs            kwargs)
        {
            myIndex               = index;
            myName                = name;
            myReturnType          = returnType;
            myIsGenericReturnType = isGenericReturnType;
            myArguments           = arguments;
            myParameterNames      = parameterNames;
            myAcceptedKwargs      = splitKwargs(kwargs);

            final boolean hasKwargs = (myAcceptedKwargs != null);

            myFlags = (short)((isStatic     ? MethodFlags.IS_STATIC    .value : 0) |
                              (isDeprecated ? MethodFlags.IS_DEPRECATED.value : 0) |
                              (isDefault    ? MethodFlags.IS_DEFAULT   .value : 0) |
                              (hasKwargs    ? MethodFlags.HAS_KWARGS   .value : 0));

            // Create the string representation to save work later
            StringBuilder sb = new StringBuilder();
            sb.append(returnType.toString()).append(' ')
              .append(name).append('(');
            String comma = "";
            for (TypeDescription td : arguments) {
                sb.append(comma).append(td.toString());
                comma = ", ";
            }
            sb.append(')');

            myString = sb.toString();

            // Talk to the animals
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Created " + this);
            }
        }

        /**
         * Get the method index.
         */
        public int getIndex()
        {
            return myIndex;
        }

        /**
         * Get the method name.
         */
        public String getName()
        {
            return myName;
        }

        /**
         * Get the {@link MethodFlags} mask for this method.
         */
        public short getFlags()
        {
            return myFlags;
        }

        /**
         * Whether this is a static method.
         */
        public boolean isStatic()
        {
            return (MethodFlags.IS_STATIC.value & myFlags) != 0;
        }

        /**
         * Whether this is a deprecated method.
         */
        public boolean isDeprecated()
        {
            return (MethodFlags.IS_DEPRECATED.value & myFlags) != 0;
        }

        /**
         * Whether this is a default method.
         */
        public boolean isDefault()
        {
            return (MethodFlags.IS_DEFAULT.value & myFlags) != 0;
        }

        /**
         * Whether this method accepts keyword arguments.
         */
        public boolean hasKwargs()
        {
            return (MethodFlags.HAS_KWARGS.value & myFlags) != 0;
        }

        /**
         * Get the return type.
         */
        public TypeDescription getReturnType()
        {
            return myReturnType;
        }

        /**
         * Whether the return type is a type specified as a generic parameter
         * (e.g. the E in List&lt;E&gt;).
         */
        public boolean isGenericReturnType()
        {
            return myIsGenericReturnType;
        }

        /**
         * Get the number of arguments.
         */
        public short getNumArguments()
        {
            return (short)myArguments.length;
        }

        /**
         * Get the nth argument.
         */
        public TypeDescription getArgument(short index)
        {
            return myArguments[index];
        }

        /**
         * Get the nth parameter name.
         */
        public String getParameterName(short index)
        {
            return myParameterNames[index];
        }

        /**
         * Get the number of accepted keyword arguments, if we are limiting them.
         */
        public short getNumAcceptedKwargs()
        {
            return (myAcceptedKwargs != null)
                ? (short)Math.min(myAcceptedKwargs.length, Short.MAX_VALUE)
                : 0;
        }

        /**
         * Get the nth accepted keyword argument name.
         */
        public String getAcceptedKwargName(short index)
        {
            if (myAcceptedKwargs == null) {
                throw new IndexOutOfBoundsException(index);
            }
            else {
                return myAcceptedKwargs[index];
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myString;
        }
    }

    /**
     * The type to ID mapping.
     */
    private class TypeMapping
    {
        /**
         * The mappings.
         */
        private TypeDescription[] myIdToDescription = new TypeDescription[1024];
        private final Map<Class<?>,TypeDescription> myClassToDescription =
            new HashMap<>();

        /**
         * The next ID to use.
         */
        private int myNextId = 0;

        /**
         * CTOR.
         */
        public TypeMapping()
        {
            // Ensure void is ID zero; not actually required but make debugging
            // easier etc. when you have things like NULL and Void be zero
            final TypeDescription voidType = getDescription(Void.TYPE);
            assert(voidType.getTypeId() == 0);
        }

        /**
         * Get the ID for a given object's class, creating it if need be.
         */
        public synchronized int getId(final Object object)
        {
            if (object == null) {
                throw new NullPointerException(
                    "Asked for the type ID of a null object"
                );
            }
            return getId(object.getClass());
        }

        /**
         * Get the TypeDescription for a given class, creating it if need be.
         */
        public synchronized TypeDescription getDescription(final Class<?> klass)
        {
            if (klass == null) {
                throw new NullPointerException(
                    "Asked for the type ID of a null class"
                );
            }
            TypeDescription desc = myClassToDescription.get(klass);
            if (desc == null) {
                final int id = myNextId++;
                if (id >= myIdToDescription.length) {
                    // This will fail if we manage to get more than 2^31 types
                    // active at any one time. I hope that this is unlikely.
                    myIdToDescription =
                        Arrays.copyOf(myIdToDescription,
                                      myIdToDescription.length << 1);
                }

                desc = new TypeDescription(klass, id);
                myClassToDescription.put(klass, desc);
                myIdToDescription[id] = desc;

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Created type mapping " +
                               "from " + klass + " to " + desc);
                }
            }
            return desc;
        }

        /**
         * Get the TypeDescription for a given ID, if any.
         */
        public synchronized TypeDescription getDescription(final int id)
        {
            return (0 <= id && id < myIdToDescription.length)
                   ? myIdToDescription[id]
                   : null;
        }

        /**
         * Get the ID for a given class, creating it if needbe.
         */
        public synchronized int getId(final Class<?> klass)
        {
            return getDescription(klass).getTypeId();
        }

        /**
         * Get a copy of the set of mapped classes.
         */
        public synchronized Set<Class<?>> getClasses()
        {
            return new HashSet<>(myClassToDescription.keySet());
        }
    }

    /**
     * The handle to object mapping.
     */
    private static class HandleMapping
    {
        /**
         * A reference to an object, with a count.
         */
        private static class Reference
        {
            public final Object object;
            public final long   handle;
            public       int    count;

            public Reference(Object object, long handle)
            {
                this.object = object;
                this.handle = handle;
                this.count  = 0;
            }
        }

        /**
         * The handle value for "null".
         */
        public static final long NULL_HANDLE = 0;

        /*
         * The mappings.
         */
        private final Map<Long,Reference> myHandleToReference =
            new HashMap<>();
        private final Map<Object,Reference> myObjectToReference =
            new IdentityHashMap<>();

        /**
         * The next handle to use.
         */
        private long myNextHandle;

        /**
         * CTOR.
         */
        public HandleMapping(int salt)
        {
            // Start from an arbitrary value; never zero since we use that for
            // NULL_HANDLE
            myNextHandle = Math.abs((long)salt << 32) + 1;
        }

        /**
         * Add a reference for a given object, returning the handle.
         */
        public synchronized long addReference(final Object object)
        {
            if (object == null) {
                throw new NullPointerException(
                    "Attempt to add a reference to a null object"
                );
            }

            // Get the reference, lazily creating
            Reference ref = myObjectToReference.get(object);
            if (ref == null) {
                ref = new Reference(object, myNextHandle++);
                myObjectToReference.put(ref.object, ref);
                myHandleToReference.put(ref.handle, ref);

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Created object handle " +
                        "from " + ref.handle + " " +
                        "to object of class " + object.getClass()
                    );
                }
            }

            // Actually add the reference
            ref.count++;
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Reference count now " + ref.count + " for " + ref.handle
                );
            }

            // And ensure the user knows the handle
            return ref.handle;
        }

        /**
         * Add a reference for a given handle.
         */
        public synchronized void addReference(final long handle)
        {
            Reference ref = myHandleToReference.get(handle);
            if (ref != null) {
                ref.count++;

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Reference count now " + ref.count + " for " + handle
                    );
                }
            }
        }

        /**
         * Drop the reference for a given handle.
         */
        public synchronized void dropReference(final long handle)
        {
            final Reference ref = myHandleToReference.get(handle);
            if (ref == null) {
                // Warn?
                return;
            }

            // Decrement the reference counter
            ref.count--;
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Reference count now " + ref.count + " for " + ref.handle
                );
            }

            // Anything left?
            if (ref.count <= 0) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Dropping reference to " + ref.handle);
                }
                myHandleToReference.remove(ref.handle);
                myObjectToReference.remove(ref.object);
            }
        }

        /**
         * Get the object for a given handle.
         */
        public synchronized Object getObject(final long handle)
        {
            final Reference ref = myHandleToReference.get(handle);
            return (ref == null) ? null : ref.object;
        }

        /**
         * Drop all handles.
         */
        public synchronized void clear()
        {
            myHandleToReference.clear();
            myObjectToReference.clear();
        }
    }

    /**
     * How we inject classes into a running instance from the outside world.
     */
    private static class ClassInjector
        extends ClassLoader
    {
        /**
         * Inject a new class definition.
         */
        public Class<?> inject(final byte[] buffer, final int len)
            throws ClassFormatError,
                   IndexOutOfBoundsException,
                   SecurityException
        {
            Class<?> result = defineClass(null, buffer, 0, len);
            resolveClass(result);
            return result;
        }
    }

    /**
     * A ByteArrayOutputStream wrapped in a DataOutputStream.
     */
    private static class ByteArrayDataOutputStream
    {
        /* The ByteArrayOutputStream which is fed by the DataOutputStream. */
        public final ByteArrayOutputStream bytes   = new ByteArrayOutputStream(1024);
        public final DataOutputStream      dataOut = new DataOutputStream(bytes);

        /** Sugar method to reset the {@link ByteArrayOutputStream}. */
        public void reset() { bytes.reset(); }
    }

    /**
     * A thread-local instance of the ByteArrayDataOutputStream.
     */
    private static class ThreadLocalByteArrayDataOutputStream
        extends ThreadLocal<ByteArrayDataOutputStream>
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public ByteArrayDataOutputStream get()
        {
            final  ByteArrayDataOutputStream buffer = super.get();
            buffer.reset();
            return buffer;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected ByteArrayDataOutputStream initialValue()
        {
            return new ByteArrayDataOutputStream();
        }
    }

    /**
     * A thread with a numeric ID, like what Python doth use.
     */
    private interface ThreadId
    {
        /**
         * Get the thread ID associated with this caller.
         */
        public long getThreadId();
    }

    /**
     * An active connection. This is where most of the logic lives.
     */
    private class Connection
        extends    Thread
        implements PythonMinion
    {
        /**
         * A worker thread for this connection. Each incoming request gets
         * handled by a worker so that we never block, even if someone is in the
         * middle of a call.
         */
        private class Worker
            extends    Thread
            implements ThreadId
        {
            /*
             * The parameters of what we're working on.
             */
            private volatile MessageType      myMessageType = MessageType.NONE;
            private volatile long             myThreadId    = -1L;
            private volatile VirtualThread    myThread      = null;
            private volatile int              myRequestId   = -1;
            private final    ByteList         myPayload     = new ByteList(1024 * 1024);
            private volatile DataOutputStream myOut         = null;

            /**
             * This gets set to false if we are ever done. It is IMPORTANT that
             * it is never set to false when this thread is in the worker queue,
             * otherwise you will lose messages.
             */
            private volatile boolean myActive = true;

            /**
             * The sending buffer instance used by this worker thread. This is
             * used to build up the message which this worker creates via the
             * receive() calls.
             */
            private final ByteArrayDataOutputStream mySendBuf =
                new ByteArrayDataOutputStream();

            /**
             * Constructor.
             */
            public Worker(String name)
            {
                super(name);
                setDaemon(true);
            }

            /**
             * Tell this worker to work.
             */
            public void handle(final MessageType      type,
                               final long             threadId,
                               final VirtualThread    thread,
                               final int              reqId,
                               final ByteList         payload,
                               final DataOutputStream out)
            {
                // Set the params
                myMessageType = type;
                myThreadId    = threadId;
                myThread      = thread;
                myRequestId   = reqId;
                myPayload.addAll(payload);
                myOut         = out;

                // And wake up the thread
                LockSupport.unpark(this);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void run()
            {
                LOG.fine("Worker thread starts");

                while (myActive) {
                    // Anything to do?
                    if (myOut != null) {
                        work();
                    }
                    else {
                        // Wait for a bit, we might be woken sooner. It seems
                        // that, in order to be decently responsive, the sleep
                        // time needs to be small here. (It seems unpark() can
                        // be slow.)
                        LockSupport.parkNanos(1000);
                    }
                }

                LOG.fine("Worker thread stops");
            }

            /**
             * Called when it's time for the thread to die.
             *
             * <p>This should <b>NEVER</b> be called if the thread is in the
             * worker pool. If that happens then you may lose messages.
             */
            public void terminate()
            {
                myActive = false;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public long getThreadId()
            {
                return myThreadId;
            }

            /**
             * We've received something to do, so do it;
             */
            private void work()
            {
                // What we are about to do
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("ThreadId " + myThreadId + ":" + myThread + " " +
                               "handling " + myMessageType + ", " +
                               "request ID " + myRequestId + ": " +
                               PJRmi.toString(myPayload));
                }

                // Time this operation
                final Instrumentor instr = myInstrumentors[myMessageType.ordinal()];
                final long start = instr.start();

                // Whether we locked the global lock
                boolean lockedGlobal = false;

                // Get the sending buffer ready for use
                mySendBuf.reset();

                // Any exception which we catch below
                Throwable caught = null;

                // Do all this inside a try-catch since we don't want any form
                // of failure to take down the thread
                try {
                    // First thing to do is to set our virtual thread. (This is
                    // needed by the LockManager to determine "who" is doing the
                    // locking.)
                    VirtualThreadLock.setThread(myThread);

                    // Attempt to lock and then handle what we're handling
                    if (myMessageType.shouldLockFor) {
                        myLockManager.lockGlobal();
                        lockedGlobal = true;
                    }

                    // And actually handle the request
                    receive(myMessageType,
                            myThreadId,
                            myThread,
                            myRequestId,
                            myPayload,
                            mySendBuf,
                            myOut);
                }
                catch (Throwable e) {
                    // We caught something
                    caught = e;

                    // Attempt to return everything back to the caller.
                    // This might be a problem with the connection
                    // itself, in which case we'll simply bail out at
                    // the end anyhow (when we try to send the
                    // exception).
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("Encountered exception " +
                                 "when handling " + myMessageType + " " +
                                 PJRmi.toString(myPayload) + ":\n" +
                                 stackTraceToString(e));
                    }

                    // What we'll be sending back
                    final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();

                    // We'll treat this like a normal object
                    final TypeDescription exDesc =
                        myTypeMapping.getDescription(e.getClass());
                    final long exHandle = myHandleMapping.addReference(e);

                    // Populate with the exception information
                    try {
                        bados.dataOut.writeInt (exDesc.getTypeId());
                        bados.dataOut.writeLong(exHandle);
                        bados.dataOut.writeInt (-1);

                        // And create it. We will need to reset the send buffer
                        // here since it might have been partially written to
                        mySendBuf.reset();
                        buildMessage(mySendBuf.dataOut,
                                     MessageType.EXCEPTION,
                                     myThreadId,
                                     myRequestId,
                                     bados.bytes);
                    }
                    catch (IOException ioe) {
                        // This should never happen when writing to a byte
                        // stream
                        throw new RuntimeException("Should not happen", ioe);
                    }
                }
                finally {
                    // Done with the global lock
                    if (lockedGlobal) {
                        myLockManager.unlockGlobal();
                    }
                    instr.end(start);

                    // Now that we have dropped the lock it's safe to send the
                    // reply. This is the last thing we need to do as part of
                    // our work for the client.
                    try {
                        send(mySendBuf, myOut);
                    }
                    catch (Throwable e) {
                        // This is probably fine if the other side closed the
                        // connection. If that happened we'll see an
                        // EOFException.
                        if (caught instanceof EOFException) {
                            LOG.info("Looks like the client disconnected: " + e);
                        }
                        else if (caught == null) {
                            LOG.warning("Error when sending: " + e);
                        }
                        else {
                            LOG.warning("Error when sending back " + caught + ": " + e);
                        }
                    }

                    // Zero out our params
                    myMessageType = MessageType.NONE;
                    myRequestId   = -1;
                    myThreadId    = -1;
                    myThread      = null;
                    myPayload.clear();
                    myOut         = null;

                    // Disassociate
                    VirtualThreadLock.setThread(null);

                    // If our connection is closed then we should mark ourselves
                    // as inactive. This is important since the Connection
                    // thread will drain the myWorker queue to terminate the
                    // workers and we don't want to add ourselves after that has
                    // happened (else we will never die).
                    if (Connection.this.isClosed()) {
                        myActive = false;
                    }

                    // If we are active then return ourselves to the queue. This
                    // is a best effort operation.
                    if (myActive && !myWorkers.offer(this)) {
                        // We failed to offer ourselves to the queue. This means
                        // that are done and should pass into that gentle night.
                        myActive = false;
                    }
                }
            }
        }

        /**
         * Thread to handle asynchronous method calls. We have specialised
         * threads for this since they need to have unique VirtualThread IDs, in
         * order to respect the locking semantics.
         */
        private class MethodCaller
            extends    Thread
            implements ThreadId
        {
            /**
             * The (hopefully) globally unique ID of this thread. This should
             * not clash with that of the Python threads.
             */
            private final long myThreadId;

            /**
             * The VirtualThread, unique to this thread.
             */
            private final VirtualThread myThread;

            /**
             * The {@link Runnable} which we are working for.
             */
            private volatile Runnable myRunnable;

            /**
             * This gets set to false if we are ever done.
             */
            private volatile boolean myActive;

            /**
             * The last time we were invoked, as per {@code System.nanoTime()}.
             */
            private long myLastCallNs;

            /**
             * Constructor.
             */
            public MethodCaller()
            {
                // Mimic Python for this, kinda. We parse the ID back out of the
                // thread name so that we are sure to match. We want the thread
                // IDs to be unique so we use nanotime which, until computers
                // get _really_ fast, should be different each time it's called.
                super(
                    "MethodCaller:" +
                    ((System.nanoTime() ^ THREAD_ID_XOR) & 0x7fffffffffffffffL)
                );
                myThreadId   = Long.parseLong(getName().substring(13)); // Erk!
                myThread     = new VirtualThread(getName());
                myRunnable   = null;
                myActive     = true;
                myLastCallNs = System.nanoTime(); // Kinda...
                setDaemon(true);
            }

            /**
             * Tell this caller to call, using the given runnable. It is
             * <b>IMPORTANT</b> that the given runnable does not throw an
             * exception.
             */
            public void handle(final Runnable runnable)
                throws IllegalStateException
            {
                // We should not be in the queue if we're working already
                if (myRunnable != null) {
                    throw new IllegalStateException(
                        "Already working on " + myRunnable
                    );
                }

                // Set the runnable and wake up the thread
                myRunnable = runnable;
                LockSupport.unpark(this);
            }

            /**
             * Called when it's time for the thread to die.
             */
            public void terminate()
            {
                myActive = false;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public long getThreadId()
            {
                return myThreadId;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void run()
            {
                LOG.fine("MethodCaller thread starts");

                // We're running, so we associate this thread with the virtual
                // one. This must be called from within the thread.
                VirtualThreadLock.setThread(myThread);

                // Now enter the worker loop, working until we're told to stop
                while (myActive) {
                    // Anything to do?
                    if (myRunnable != null) {
                        try {
                            // Invoke the run and note when we did it
                            myLastCallNs = System.nanoTime();
                            myRunnable.run();
                        }
                        catch (Throwable t) {
                            // We can't do a lot about this. It's up to the
                            // caller to construct a runnable which won't throw.
                            LOG.log(Level.SEVERE,
                                    "Runnable threw an exception; " +
                                    "that should ideally never happen",
                                    t);
                        }
                        finally {
                            // We're done. Clear out the runnable and return
                            // ourselves to the queue.
                            myRunnable = null;
                            myMethodCallers.offer(this);
                        }
                    }
                    else {
                        // Wait for at least 1us, and at most 1s, here since
                        // these threads are long-lived and we don't care _too_
                        // much about being reactive for asynchronous method
                        // calls. As time goes on we back off on the wake-up
                        // period so as not to drain CPU resources. Hopefully
                        // unpark() will wake us up in _reasonable_ time anyhow.
                        LockSupport.parkNanos(
                            Math.min(1_000_000_000L,
                                     Math.max(1_000L,
                                              System.nanoTime() - myLastCallNs))
                        );
                    }
                }

                LOG.fine("MethodCaller thread stops");
            }
        }

        /**
         * Our extended version of the {@link PJRmiPythonPickle} class, which
         * can be used to instantiate Java objects on the other side.
         */
        private class BestEffortPythonPickle
            extends PJRmiPythonPickle
        {
            /**
             * {@inheritDoc}
             */
            @Override
            protected void saveObject(final Object obj)
                throws UnsupportedOperationException
            {
                // We'll need to know the type and the handle
                final TypeDescription type =
                    myTypeMapping.getDescription(
                        (obj == null) ? Object.class : obj.getClass()
                    );
                final long handle = (obj == null)
                    ? HandleMapping.NULL_HANDLE
                    : myHandleMapping.addReference(obj);

                // We'll call this method to unmarshall it on the other side
                // with 3 arguments which are turned into a tuple3 before being
                // passed to the pjrmi module's function call.
                saveGlobal ("pjrmi", "_handle_pickle_bytes_create_object");
                saveInteger(myPythonId);        // PJRmi instance ID
                saveInteger(type.getTypeId());  // Type ID
                saveInteger(handle);            // Object handle
                write      (Operations.TUPLE3); // Turn those into args
                write      (Operations.REDUCE); // Call the function
            }
        }

        // -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -

        // Python callback support

        /**
         * How a PythonCallback receives its result.
         */
        private class PythonCallbackResult
        {
            /**
             * The thread which is receiving the callback.
             */
            private final Thread myReceiver;

            /**
             * What is received.
             */
            private volatile Object myResult;

            /**
             * Whether myResult is an exception to be thrown.
             */
            private volatile boolean myIsException;

            /**
             * Whether myResult is ready for reading.
             */
            private volatile boolean myReady;

            /**
             * Constructor.
             */
            public PythonCallbackResult(final Thread receiver)
            {
                myReceiver = receiver;
                myResult   = null;
                myReady    = false;
            }

            /**
             * Set the result.
             */
            public void setResult(final boolean isException, final Object result)
            {
                myResult      = result;
                myIsException = isException;
                myReady       = true;
                LockSupport.unpark(myReceiver);
            }

            /**
             * Whether the result is ready.
             */
            public boolean isReady()
            {
                return myReady;
            }

            /**
             * Whether the result is an exception to be thrown
             */
            public boolean isException()
            {
                return myIsException;
            }

            /**
             * Get the result
             */
            public Object getResult()
            {
                return myResult;
            }
        }

        /**
         * A callback into Python from Java.
         */
        private abstract class PythonCallback<T>
            implements PythonKwargsFunction<T>
        {
            /**
             * The Python ID of the function.
             */
            private final int myFunctionId;

            /**
             * How we send the message to Python.
             */
            private final DataOutputStream myOut;

            /**
             * Constructor.
             */
            public PythonCallback(final int functionId,
                                  final DataOutputStream out)
            {
                myFunctionId = functionId;
                myOut = out;
            }

            /**
             * The type description of this function. This should be the public
             * interface, not the implementation.
             */
            public abstract TypeDescription getType();

            /**
             * {@inheritDoc}
             */
            @Override
            public T invoke(final Map<String,Object> kwargs,
                            final Object...          args)
                throws Throwable
            {
                return doInvoke(kwargs, args);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                return getClass().getSimpleName() + "#" + myFunctionId;
            }

            /**
             * How we make the call. This sends a message over to the Python
             * side which should hopefully be picked up.
             */
            protected T doInvoke(final Map<String,Object> kwargs,
                                 final Object...          args)
                throws IOException,
                       PythonCallbackException
            {
                // Say we're doing it
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Invoking " + this + " " +
                               "with args " + Arrays.toString(args));
                }

                // Figure out our thread ID
                final Thread thread = Thread.currentThread();
                final long threadId = (thread instanceof ThreadId)
                    ? ((ThreadId)thread).getThreadId()
                    : -1;

                // The python callback request ID
                final int requestId = myPythonCallbackRequestId.getAndIncrement();

                // Build the data to make the call
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeInt(requestId);
                bados.dataOut.writeInt(myFunctionId);
                if (args == null) {
                    bados.dataOut.writeInt(0);
                }
                else {
                    bados.dataOut.writeInt(args.length);
                    for (Object arg : args) {
                        writeArgument(bados.dataOut, arg);
                    }
                }

                // The number of keyword arguments
                if (kwargs == null) {
                    bados.dataOut.writeInt(0);
                }
                else {
                    bados.dataOut.writeInt(kwargs.size());
                    for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                        // Write name and argument
                        writeUTF16   (bados.dataOut, entry.getKey());
                        writeArgument(bados.dataOut, entry.getValue());
                    }
                }

                // Register ourselves for the callback
                final PythonCallbackResult result = new PythonCallbackResult(thread);
                myPythonCallbackResults.put(requestId, result);

                // Send the request over
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Calling back to Python with thread ID " + threadId + " " +
                        "and Java request ID " + requestId
                    );
                }
                sendMessage(myOut,
                            MessageType.CALLBACK,
                            threadId,
                            CALLBACK_REQUEST_ID,
                            bados.bytes);

                // Now wait for the result
                return awaitCallbackReponse(result);
            }
        }

        /**
         * A Runnable implemented using a PythonCallback.
         */
        private class PythonCallbackRunnable
            extends PythonCallback<Object>
            implements Runnable
        {
            /**
             * Constructor.
             */
            public PythonCallbackRunnable(final int functionId,
                                          final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(Runnable.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void run()
            {
                try {
                    invoke();
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A UnaryOperator implemented using a PythonCallback.
         */
        private class PythonCallbackUnaryOperator<T>
            extends PythonCallback<T>
            implements UnaryOperator<T>
        {
            /**
             * Constructor.
             */
            public PythonCallbackUnaryOperator(final int functionId,
                                               final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(UnaryOperator.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public T apply(T arg)
            {
                try {
                    return invoke(arg);
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A Function implemented using a PythonCallback.
         */
        private class PythonCallbackFunction<T,R>
            extends PythonCallback<R>
            implements Function<T,R>
        {
            /**
             * Constructor.
             */
            public PythonCallbackFunction(final int functionId,
                                          final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(Function.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public R apply(T arg)
            {
                try {
                    return invoke(arg);
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A BiFunction implemented using a PythonCallback.
         */
        private class PythonCallbackBiFunction<T,R,U>
            extends PythonCallback<R>
            implements BiFunction<T,U,R>
        {
            /**
             * Constructor.
             */
            public PythonCallbackBiFunction(final int functionId,
                                            final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(BiFunction.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public R apply(T arg1, U arg2)
            {
                try {
                    return invoke(arg1, arg2);
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A Consumer implemented using a PythonCallback.
         */
        private class PythonCallbackConsumer<T>
            extends PythonCallback<Object>
            implements Consumer<T>
        {
            /**
             * Constructor.
             */
            public PythonCallbackConsumer(final int functionId,
                                          final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(Consumer.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void accept(T arg)
            {
                try {
                    invoke(arg);
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A BiConsumer implemented using a PythonCallback.
         */
        private class PythonCallbackBiConsumer<T,U>
            extends PythonCallback<Object>
            implements BiConsumer<T,U>
        {
            /**
             * Constructor.
             */
            public PythonCallbackBiConsumer(final int functionId,
                                            final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(BiConsumer.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void accept(T arg1, U arg2)
            {
                try {
                    invoke(arg1, arg2);
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * A Predicate implemented using a PythonCallback.
         */
        private class PythonCallbackPredicate<T>
            extends PythonCallback<Object>
            implements Predicate<T>
        {
            /**
             * Constructor.
             */
            public PythonCallbackPredicate(final int functionId,
                                           final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(Predicate.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean test(T arg)
            {
                try {
                    // We'll try to keep the same semantics that we expect in
                    // Python given various types
                    final Object result = invoke(arg);
                    if (result instanceof Boolean) {
                        return ((Boolean)result).booleanValue();
                    }
                    else if (result instanceof String) {
                        return !((String)result).isEmpty();
                    }
                    else if (result instanceof Number) {
                        return ((Number)result).doubleValue() != 0.0;
                    }
                    else {
                        return (result != null);
                    }
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }


        /**
         * A BiPredicate implemented using a PythonCallback.
         */
        private class PythonCallbackBiPredicate<T,U>
            extends PythonCallback<Object>
            implements BiPredicate<T,U>
        {
            /**
             * Constructor.
             */
            public PythonCallbackBiPredicate(final int functionId,
                                             final DataOutputStream out)
            {
                super(functionId, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypeDescription getType()
            {
                return myTypeMapping.getDescription(BiPredicate.class);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean test(T arg1, U arg2)
            {
                try {
                    // We'll try to keep the same semantics that we expect in
                    // Python given various types
                    final Object result = invoke(arg1, arg2);
                    if (result instanceof Boolean) {
                        return ((Boolean)result).booleanValue();
                    }
                    else if (result instanceof String) {
                        return !((String)result).isEmpty();
                    }
                    else if (result instanceof Number) {
                        return ((Number)result).doubleValue() != 0.0;
                    }
                    else {
                        return (result != null);
                    }
                }
                catch (PythonCallbackException e) {
                    throw new RuntimeException("Failed to invoke callback",
                                               e.getCause());
                }
                catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke callback", t);
                }
            }
        }

        /**
         * The invocation handler for calling a single Python function as a Java
         * lambda.
         */
        private class PythonLambdaHandler
            implements InvocationHandler
        {
            /**
             * The name of the method which is being handled.
             */
            private final String myMethodName;

            /**
             * The Python ID of the function.
             */
            private final int myFunctionId;

            /**
             * How we send the message to Python.
             */
            private final DataOutputStream myOut;

            /**
             * Constructor.
             */
            public PythonLambdaHandler(final String methodName,
                                       final int functionId,
                                       final DataOutputStream out)
            {
                myMethodName = methodName;
                myFunctionId = functionId;
                myOut = out;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Object invoke(final Object proxy,
                                 final Method method,
                                 final Object[] args)
                throws Throwable
            {
                // Say we're doing it
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Invoking lambda method " + method + " " +
                               "with args " + Arrays.toString(args));
                }

                // Handle Object methods specially
                switch (method.getName()) {
                case "equals": {
                    final Object that = (args.length == 2) ? args[1] : null;
                    if (that instanceof PythonLambdaHandler) {
                        final PythonLambdaHandler plh = (PythonLambdaHandler)that;
                        return (plh.myMethodName.equals(myMethodName) &&
                                plh.myFunctionId == myFunctionId);
                    }
                    else {
                        return false;
                    }
                }

                case "getClass":
                    return super.getClass();

                case "hashCode":
                    return (myFunctionId ^ myMethodName.hashCode());

                case "notify":
                    super.notify();
                    return null;

                case "notifyAll":
                    super.notifyAll();
                    return null;

                case "toString":
                    return "PythonLambda<" + myMethodName + "()>";

                case "wait":
                    super.wait();
                    return null;
                }

                // Is this a default method? If so then fall back to the
                // interface implementation
                if (method.isDefault()) {
                    // Need to jump through some ugly hoops to allow
                    // "private" access to the method here
                    final Class<?> klass = method.getDeclaringClass();
                    final Lookup lookup = MethodHandles.lookup().in(klass);
                    final Field allowedModes =
                        Lookup.class.getDeclaredField("allowedModes");
                    allowedModes.setAccessible(true);
                    allowedModes.set(lookup, Modifier.PRIVATE);
                    return lookup.unreflectSpecial(method, klass)
                                 .bindTo(proxy)
                                 .invokeWithArguments(args);
                }
                else if (myMethodName.equals(method.getName())) {
                    // Figure out our thread ID
                    final Thread thread = Thread.currentThread();
                    final long threadId = (thread instanceof ThreadId)
                        ? ((ThreadId)thread).getThreadId()
                        : -1;

                    // The python callback request ID
                    final int requestId = myPythonCallbackRequestId.getAndIncrement();

                    // Build the data to make the call
                    final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                    bados.dataOut.writeInt(requestId);
                    bados.dataOut.writeInt(myFunctionId);
                    if (args == null) {
                        bados.dataOut.writeInt(0);
                    }
                    else {
                        bados.dataOut.writeInt(args.length);
                        for (Object arg : args) {
                            writeArgument(bados.dataOut, arg);
                        }
                    }

                    // No kwargs
                    bados.dataOut.writeInt(0);

                    // Register ourselves for the callback
                    final PythonCallbackResult result = new PythonCallbackResult(thread);
                    myPythonCallbackResults.put(requestId, result);

                    // Send the request over
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(
                            "Calling back to Python with thread ID " + threadId + " " +
                            "and Java request ID " + requestId
                        );
                    }
                    sendMessage(myOut,
                                MessageType.CALLBACK,
                                threadId,
                                CALLBACK_REQUEST_ID,
                                bados.bytes);

                    // Now wait for the result
                    return awaitCallbackReponse(result);
                }
                else {
                    // We don't know how to handle this. This isn't really
                    // expected given that we should be constructed in such a
                    // fashion as to only be working for one, non-default,
                    // method so we'll just throw here.
                    throw new IllegalArgumentException(
                        "Don't know how to handle call to " + method
                    );
                }
            }
        }

        /**
         * The invocation handler for calling methods on a Python object being
         * proxied by a Java one. This allows for duck-typing of Java
         * interfaces.
         */
        private class PythonProxyHandler
            implements InvocationHandler
        {
            /**
             * The Python ID of the object.
             */
            private final int myObjectId;

            /**
             * How we send the message to Python.
             */
            private final DataOutputStream myOut;

            /**
             * Constructor.
             */
            public PythonProxyHandler(final int objectId,
                                      final DataOutputStream out)
                throws IOException
            {
                // Init members
                myObjectId = objectId;
                myOut = out;

                // Tell Python that we are adding a reference
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeLong(myObjectId);
                sendMessage(myOut,
                            MessageType.ADD_REFERENCE,
                            -1, // Thread ID
                            CALLBACK_REQUEST_ID,
                            bados.bytes);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Object invoke(final Object proxy,
                                 final Method method,
                                 final Object[] args)
                throws Throwable
            {
                // Say we're doing it
                if (LOG.isLoggable(Level.FINEST)) {
                    final String argString;
                    if (args == null) {
                        argString = "<null>";
                    }
                    else {
                        // Turn the array of arguments into a string of
                        // <type:value> pairs
                        StringBuilder sb = new StringBuilder("[");
                        for (int i=0; i < args.length; i++) {
                            if (i > 0) {
                                sb.append(", ");
                            }
                            final Object arg = args[i];
                            if (arg == null) {
                                sb.append("<null>");
                            }
                            else {
                                sb.append(arg.getClass())
                                  .append(':')
                                  .append(arg);
                            }
                        }
                        sb.append(']');
                        argString = sb.toString();
                    }
                    LOG.finest("Invoking proxy " + method + " " +
                               "on " + toLocalString() + " " +
                               "with args " + argString);
                }

                // Special case: If this is an equals method with one argument
                // which is another proxy then we need to see if it's being
                // called with this proxy as the argument.
                if ("equals".equals(method.getName()) &&
                    args        != null               &&
                    args.length == 1                  &&
                    args[0] instanceof Proxy)
                {
                    final InvocationHandler handler = Proxy.getInvocationHandler(args[0]);
                    if (handler instanceof PythonProxyHandler) {
                        final PythonProxyHandler that = (PythonProxyHandler)handler;
                        if (LOG.isLoggable(Level.FINEST)) {
                            LOG.finest(
                                "Handling call of equals(" + that.toLocalString() + ")"
                            );
                        }
                        return this.myObjectId == that.myObjectId;
                    }
                }

                // Figure out our thread ID
                final Thread thread = Thread.currentThread();
                final long threadId = (thread instanceof ThreadId)
                    ? ((ThreadId)thread).getThreadId()
                    : -1;

                // The python callback request ID
                final int requestId = myPythonCallbackRequestId.getAndIncrement();

                // Build the data to make the call
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeInt(requestId);
                bados.dataOut.writeInt(myObjectId);
                bados.dataOut.writeInt(myTypeMapping.getId(method.getReturnType()));
                writeUTF16            (bados.dataOut, method.getName());

                // Handle any arguments
                if (args == null) {
                    bados.dataOut.writeInt(0);
                }
                else {
                    bados.dataOut.writeInt(args.length);
                    for (Object arg : args) {
                        writeArgument(bados.dataOut, arg);
                    }
                }

                // No kwargs
                bados.dataOut.writeInt(0);

                // Register ourselves for the callback
                final PythonCallbackResult result = new PythonCallbackResult(thread);
                myPythonCallbackResults.put(requestId, result);

                // Send the request over
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Calling back to Python with thread ID " + threadId + " " +
                        "and Java request ID " + requestId
                    );
                }
                sendMessage(myOut,
                            MessageType.OBJECT_CALLBACK,
                            threadId,
                            CALLBACK_REQUEST_ID,
                            bados.bytes);

                try {
                    // Now wait for the result
                    return awaitCallbackReponse(result);
                }
                catch (PythonCallbackException e) {
                    // Fall back to any default implementation if we failed to
                    // find the method
                    if (e.getCause() instanceof NoSuchMethodException &&
                        method.isDefault())
                    {
                        // Need to jump through some ugly hoops to allow
                        // "private" access to the method here
                        final Class<?> klass = method.getDeclaringClass();
                        final Lookup lookup = MethodHandles.lookup().in(klass);
                        final Field allowedModes =
                            Lookup.class.getDeclaredField("allowedModes");
                        allowedModes.setAccessible(true);
                        allowedModes.set(lookup, Modifier.PRIVATE);
                        return lookup.unreflectSpecial(method, klass)
                                     .bindTo(proxy)
                                     .invokeWithArguments(args);
                    }

                    // Rethrow if we got here
                    throw e;
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            @SuppressWarnings("deprecation")
            protected void finalize()
                throws Throwable
            {
                try {
                    // Tell Python that it can forget about this object now
                    final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                    bados.dataOut.writeInt(1);
                    bados.dataOut.writeLong(myObjectId);
                    sendMessage(myOut,
                                MessageType.DROP_REFERENCES,
                                -1, // Thread ID
                                CALLBACK_REQUEST_ID,
                                bados.bytes);
                }
                finally {
                    super.finalize();
                }
            }

            /**
             * The Object ID.
             */
            private int getId()
            {
                return myObjectId;
            }

            /**
             * Get our local toString() representation.
             */
            private String toLocalString()
            {
                return getClass().getSimpleName() + "[" + getId() + "]";
            }
        }

        /**
         * A wrapper around an object sitting in the Python interpreter. This
         * should be the only implementation of PythonObject.
         */
        private class PythonObjectImpl
            extends PythonObject
        {
            /**
             * The name of the object.
             */
            private final String myName;

            /**
             * The ID of the object we represent.
             */
            private final int myObjectId;

            /**
             * How we send the message to Python.
             */
            private final DataOutputStream myOut;

            /**
             * Constructor.
             */
            private PythonObjectImpl(final String           name,
                                     final int              objectId,
                                     final DataOutputStream out)
                throws IOException
            {
                // Init members
                myName     = name;
                myObjectId = objectId;
                myOut      = out;

                // Tell Python that we are adding a reference
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeLong(myObjectId);
                sendMessage(myOut,
                            MessageType.ADD_REFERENCE,
                            -1, // Thread ID
                            CALLBACK_REQUEST_ID,
                            bados.bytes);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public <T> T invoke(final Class<T>           returnType,
                                final String             methodName,
                                final Map<String,Object> kwargs,
                                final Object...          args)
                throws Throwable
            {
                // Say we're doing it
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Invoking " + returnType + " " + methodName + "(" +
                            "args="   + Arrays.toString(args) + ", " +
                            "kawrgs=" + kwargs +
                        ")"
                    );
                }

                // Figure out our thread ID
                final Thread thread = Thread.currentThread();
                final long threadId = (thread instanceof ThreadId)
                    ? ((ThreadId)thread).getThreadId()
                    : -1;

                // The python callback request ID
                final int requestId = myPythonCallbackRequestId.getAndIncrement();

                // The ID of the return type. If it's null then just use
                // PythonObject to give back another wrapper instance. Most
                // likely it will just be thrown away anyhow. (Possibly we could
                // use Void here so as to force nothing/null to be returned.)
                final int returnTypeId =
                    myTypeMapping.getId((returnType == null) ? PythonObject.class :
                                                               returnType);

                // Build the data to make the call
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeInt(requestId);
                bados.dataOut.writeInt(myObjectId);
                bados.dataOut.writeInt(returnTypeId);
                writeUTF16            (bados.dataOut, methodName);

                // Handle any arguments
                if (args == null) {
                    bados.dataOut.writeInt(0);
                }
                else {
                    bados.dataOut.writeInt(args.length);
                    for (Object arg : args) {
                        writeArgument(bados.dataOut, arg);
                    }
                }

                // The number of keyword arguments
                if (kwargs == null) {
                    bados.dataOut.writeInt(0);
                }
                else {
                    bados.dataOut.writeInt(kwargs.size());
                    for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                        // Write name and argument
                        writeUTF16   (bados.dataOut, entry.getKey());
                        writeArgument(bados.dataOut, entry.getValue());
                    }
                }

                // Register ourselves for the callback
                final PythonCallbackResult result = new PythonCallbackResult(thread);
                myPythonCallbackResults.put(requestId, result);

                // Send the request over
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Calling back to Python with thread ID " + threadId + " " +
                        "and Java request ID " + requestId
                    );
                }
                sendMessage(myOut,
                            MessageType.OBJECT_CALLBACK,
                            threadId,
                            CALLBACK_REQUEST_ID,
                            bados.bytes);

                // Now wait for the result
                return awaitCallbackReponse(result);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public <T> T getattr(final Class<T> fieldType,
                                 final String   fieldName)
                throws Throwable
            {
                // Say we're doing it
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Getting field " + fieldName + " as a " + fieldType);
                }

                // Figure out our thread ID
                final Thread thread = Thread.currentThread();
                final long threadId = (thread instanceof ThreadId)
                    ? ((ThreadId)thread).getThreadId()
                    : -1;

                // The python callback request ID
                final int requestId = myPythonCallbackRequestId.getAndIncrement();

                // The ID of the field type. If it's null then just use
                // PythonObject to give back another wrapper instance.
                final int fieldTypeId =
                    myTypeMapping.getId((fieldType == null) ? PythonObject.class :
                                                              fieldType);

                // Build the data to make the call
                final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                bados.dataOut.writeInt(requestId);
                bados.dataOut.writeInt(myObjectId);
                bados.dataOut.writeInt(fieldTypeId);
                writeUTF16            (bados.dataOut, fieldName);

                // Register ourselves for the callback
                final PythonCallbackResult result = new PythonCallbackResult(thread);
                myPythonCallbackResults.put(requestId, result);

                // Send the request over
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Calling back to Python with thread ID " + threadId + " " +
                        "and Java request ID " + requestId
                    );
                }
                sendMessage(myOut,
                            MessageType.GETATTR,
                            threadId,
                            CALLBACK_REQUEST_ID,
                            bados.bytes);

                // Now wait for the result
                try {
                    return awaitCallbackReponse(result);
                }
                catch (PythonCallbackException e) {
                    throw e.getCause();
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            @SuppressWarnings("unchecked")
            public <T> T asProxy(final Class<T> klass)
                throws IOException
            {
                return (T) Proxy.newProxyInstance(
                    klass.getClassLoader(),
                    new Class<?>[] { klass },
                    new PythonProxyHandler(myObjectId, myOut)
                );
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                try {
                    // Attempt to get Python's str representation
                    return invoke(String.class, "__str__");
                }
                catch (Throwable t) {
                    // Fall back to the name
                    return myName;
                }
            }

            /**
             * The Object ID.
             */
            protected int getId()
            {
                return myObjectId;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            @SuppressWarnings("deprecation")
            protected void finalize()
                throws Throwable
            {
                try {
                    // Tell Python that it can forget about this object now
                    final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
                    bados.dataOut.writeInt(1);
                    bados.dataOut.writeLong(myObjectId);
                    sendMessage(myOut,
                                MessageType.DROP_REFERENCES,
                                -1, // Thread ID
                                CALLBACK_REQUEST_ID,
                                bados.bytes);
                }
                finally {
                    super.finalize();
                }
            }
        }

        // --------------------------------------------------------------------

        /**
         * Set to false when we are terminating.
         */
        private volatile boolean myIsActive;

        /**
         * The transport.
         */
        private final Transport myTransport;

        /**
         * The Python instance's ID.
         */
        private final long myPythonId;

        /**
         * Where data comes in.
         */
        private final DataInputStream myIn;

        /**
         * Where data goes out.
         */
        private final DataOutputStream myOut;

        /**
         * Our per-thread PythonPicklers, for converting values to Python's
         * pickle format in a best-effort fashion. These are connection-specific
         * since they need to be able to identify the PJRmi instance on the
         * Python side.
         */
        private final ThreadLocal<PythonPickle> myBestEffortPythonPickle;

        /**
         * Our object handle mappings.
         */
        private final HandleMapping myHandleMapping;

        /**
         * Our workers, if any. This will be null if useWorkers() is false.
         */
        private final BlockingQueue<Worker> myWorkers;

        /**
         * Our method callers.
         */
        private final BlockingQueue<MethodCaller> myMethodCallers;

        /**
         * The mapping from virtual thread ID to a virtual Java Thread. Only
         * needed (non-null) if we have workers.
         */
        private Map<Long,VirtualThread> myVirtualThreads;

        /**
         * How many workers we have created; used for naming.
         */
        private int myNumWorkers;

        /**
         * The next request ID for making Python callbacks over this connection.
         */
        private final AtomicInteger myPythonCallbackRequestId = new AtomicInteger();

        /**
         * The map used to receive python callback results.
         */
        private final Map<Integer,PythonCallbackResult> myPythonCallbackResults =
            new ConcurrentHashMap<>();

        /**
         * The "depth" into the call stack. This is to spot cases where we are
         * calling from Java to Python to Java to Python ad infinitum.
         */
        private int myCallDepth;

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /**
         * CTOR.
         */
        public Connection(final String name,
                          final Transport transport,
                          final long pythonId)
            throws IOException
        {
            super(name);

            myTransport = transport;
            myPythonId = pythonId;

            // We are active from the get-go
            myIsActive = true;

            // Set up the streams. It's important that we use a buffered output
            // stream for myOut since this will send the responses as a single
            // packet and this _greatly_ speeds up the way the clients work.
            // The 64k value is the usual MTU and should be way bigger than
            // anything we will end up sending.
            final InputStream  inStream  = transport.getInputStream();
            final OutputStream outStream = transport.getOutputStream();
            myIn = (inStream instanceof DataInputStream) ?
                (DataInputStream)inStream : new DataInputStream(inStream);
            myOut =
                new DataOutputStream(
                    new BufferedOutputStream(outStream, 65536)
                );

            // How we render with pickle in a best-effort fashion
            myBestEffortPythonPickle =
                ThreadLocal.withInitial(BestEffortPythonPickle::new);

            // Try to ensure that all the handles have a reasonably unique
            // identifier
            myHandleMapping =
                new HandleMapping(hashCode() & (int)System.nanoTime());

            // Where our workers, if any, live etc
            if (useWorkers()) {
                myWorkers        = new ArrayBlockingQueue<>(numWorkers());
                myVirtualThreads = new HashMap<>();
            }
            else {
                myWorkers        = null;
                myVirtualThreads = null;
            }
            myNumWorkers = 0;

            // Unbounded queue of these since we want them to be long-lived
            myMethodCallers = new LinkedBlockingQueue<>();

            // The depth of our stack
            myCallDepth = 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Started handler thread: " + this);

            try {
                // Listen until there is nothing else to hear
                listen();
            }
            finally {
                // Be nice to the GC
                myHandleMapping.clear();

                // Drop any locks, if we are holding them
                try {
                    myLockManager.dropAllThreadLocks();
                }
                catch (IllegalArgumentException e) {
                    // This is fine but log when debugging
                    LOG.fine("Problem dropping locks: " + e);
                }

                // Ensure the underlying connection is closed so that the other
                // side won't hang forever etc.
                try {
                    myTransport.close();
                }
                catch (Throwable t) {
                    // Nothing
                }
            }

            // Mark ourselves as inactive. We should do this before killing off
            // the workers so that none of them attempt to insert themselves
            // into the queue as we terminate them. See the Worker#work()
            // method.
            myIsActive = false;

            // If we have worker threads then close them down
            if (myWorkers != null) {
                LOG.fine("Terminating workers");
                for (Worker worker = myWorkers.poll();
                     worker != null;
                     worker = myWorkers.poll())
                {
                    worker.terminate();
                }
            }
            LOG.fine("Terminating method callers");
            for (MethodCaller caller = myMethodCallers.poll();
                 caller != null;
                 caller = myMethodCallers.poll())
            {
                caller.terminate();
            }

            LOG.info("Exiting handler thread: " + this);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T eval(final String string, final Class<T> returnType)
            throws ClassCastException,
                   IOException,
                   PythonCallbackException
        {
            return evalOrExec(true, string, returnType);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void exec(final String string)
            throws IOException,
                   PythonCallbackException
        {
            try {
                evalOrExec(false, string, Object.class);
            }
            catch (ClassCastException e) {
                // Shouldn't happen
                throw new AssertionError("Unexpected error", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setGlobalVariable(final String name, final Object value)
            throws IOException,
                   PythonCallbackException
        {
            // We need a name
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Empty name given");
            }

            // The python callback request ID
            final int requestId = myPythonCallbackRequestId.getAndIncrement();

            // Build the data to make the call
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(requestId);

            // What name for the variable
            writeUTF16(bados.dataOut, name);

            // Send the value we're setting by reference, as an "argument".
            // (It's sort of an argument to the set function. Sort of...)
            final TypeDescription type =
                myTypeMapping.getDescription(
                    (value == null) ? Object.class : value.getClass()
                );
            bados.dataOut.writeByte(PythonValueFormat.REFERENCE.id);
            writeObject(bados.dataOut, value, type);

            // Figure out our thread ID
            final Thread thread = Thread.currentThread();
            final long threadId = (thread instanceof ThreadId)
                ? ((ThreadId)thread).getThreadId()
                : -1;

            // Register ourselves for the callback
            final PythonCallbackResult result = new PythonCallbackResult(this);
            myPythonCallbackResults.put(requestId, result);

            // Send the request over
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Setting global variable " + name + " " +
                    "to value " + value + " " +
                    "with request ID " + requestId + " " +
                    "from Java thread ID " + threadId
                );
            }
            sendMessage(myOut,
                        MessageType.SET_GLOBAL_VARIABLE,
                        threadId,
                        CALLBACK_REQUEST_ID,
                        bados.bytes);

            // Now wait for the result, might be null or an exception
            awaitCallbackReponse(result);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T invoke(final String    functionName,
                            final Class<T>  returnType,
                            final Object... args)
            throws ClassCastException,
                   IOException,
                   PythonCallbackException
        {
            // The python callback request ID
            final int requestId = myPythonCallbackRequestId.getAndIncrement();

            // Get the thread ID
            final long threadId = getThreadId();

            // Build the data to make the call
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(requestId);
            bados.dataOut.writeInt(myTypeMapping.getId(returnType));
            writeUTF16(bados.dataOut, functionName);
            if (args == null) {
                bados.dataOut.writeInt(0);
            }
            else {
                bados.dataOut.writeInt(args.length);
                for (Object arg : args) {
                    writeArgument(bados.dataOut, arg);
                }
            }

            // Register ourselves for the callback
            final PythonCallbackResult result = new PythonCallbackResult(this);
            myPythonCallbackResults.put(requestId, result);

            // Send the request over
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Sending invocation of " + functionName + " " +
                    "to Python with thread ID " + threadId + " " +
                    "and Java request ID " + requestId
                );
            }
            sendMessage(myOut,
                        MessageType.PYTHON_INVOKE,
                        threadId,
                        CALLBACK_REQUEST_ID,
                        bados.bytes);

            // Now wait for the result
            return awaitCallbackReponse(result);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public PythonObject getObject(final String string,
                                      final String name)
            throws Throwable
        {
            // The python callback request ID
            final int requestId = myPythonCallbackRequestId.getAndIncrement();

            // Get the thread ID
            final long threadId = getThreadId();

            // Build the data to make the call
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(requestId);
            writeUTF16(bados.dataOut, string);

            // Register ourselves for the callback
            final PythonCallbackResult result = new PythonCallbackResult(this);
            myPythonCallbackResults.put(requestId, result);

            // Send the request over
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Sending object request to Python with thread ID " +
                    threadId + " and Java request ID " + requestId
                );
            }
            sendMessage(myOut,
                        MessageType.GET_OBJECT,
                        threadId,
                        CALLBACK_REQUEST_ID,
                        bados.bytes);

            // Get back the Python object ID and use it to construct the
            // wrapper. A negative ID means null.
            final Integer objectId = awaitCallbackReponse(result);
            return (objectId < 0)
                ? null
                : new PythonObjectImpl(name == null ? string : name, objectId, myOut);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public PythonObject invokeAndGetObject(final String functionName,
                                               final Object... args)
            throws Throwable
        {
            // The python callback request ID
            final int requestId = myPythonCallbackRequestId.getAndIncrement();

            // Get the thread ID
            final long threadId = getThreadId();

            // Build the data to make the call
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(requestId);
            writeUTF16(bados.dataOut, functionName);
            if (args == null) {
                bados.dataOut.writeInt(0);
            }
            else {
                bados.dataOut.writeInt(args.length);
                for (Object arg : args) {
                    writeArgument(bados.dataOut, arg);
                }
            }

            // Register ourselves for the callback
            final PythonCallbackResult result = new PythonCallbackResult(this);
            myPythonCallbackResults.put(requestId, result);

            // Send the request over
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Sending object request to Python with thread ID " +
                    threadId + " and Java request ID " + requestId
                );
            }
            sendMessage(myOut,
                        MessageType.INVOKE_AND_GET_OBJECT,
                        threadId,
                        CALLBACK_REQUEST_ID,
                        bados.bytes);

            // Get back the Python object ID and use it to construct the
            // wrapper. A negative ID means null.
            final Integer objectId = awaitCallbackReponse(result);
            return (objectId < 0)
                ? null
                : new PythonObjectImpl(functionName, objectId, myOut);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
        {
            myTransport.close();
        }

        /**
         * Whether this connection is closed or not.
         */
        public boolean isClosed()
        {
            return !myIsActive || myTransport.isClosed();
        }

        /**
         * Returns the thread ID of the current thread, or if that is
         * unavailable then a best-effort version.
         */
        private long getThreadId()
        {
            final long threadId = Thread.currentThread().getId();
            if (threadId < 0) {
                return ProcessHandle.current().pid();
            }
            else {
                return threadId;
            }
        }

        /**
         * Do the Python eval or exec.
         */
        private <T> T evalOrExec(final boolean isEval,
                                 final String string,
                                 final Class<T> returnType)
            throws ClassCastException,
                   IOException,
                   PythonCallbackException
        {
            // The python callback request ID
            final int requestId = myPythonCallbackRequestId.getAndIncrement();

            // Get the thread ID
            final long threadId = getThreadId();

            // Build the data to make the call
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(requestId);
            bados.dataOut.writeBoolean(isEval);
            bados.dataOut.writeInt(myTypeMapping.getId(returnType));
            writeUTF16(bados.dataOut, string);

            // Register ourselves for the callback
            final PythonCallbackResult result = new PythonCallbackResult(this);
            myPythonCallbackResults.put(requestId, result);

            // Send the request over
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Sending eval to Python with thread ID " + threadId + " " +
                    "and Java request ID " + requestId
                );
            }
            sendMessage(myOut,
                        MessageType.PYTHON_EVAL_OR_EXEC,
                        threadId,
                        CALLBACK_REQUEST_ID,
                        bados.bytes);

            // Now wait for the result
            return awaitCallbackReponse(result);
        }

        /**
         * Write out a function argument. This might be by reference or by
         * value.
         */
        private void writeArgument(final DataOutputStream out, final Object arg)
            throws IOException
        {
            if (arg instanceof ByValue) {
                // Sending by value
                final Object reference = ((ByValue)arg).get();
                if (reference == null) {
                    // Can't send the value of NULL, fall back to a NULL reference
                    out.writeByte(PythonValueFormat.REFERENCE.id);
                    writeObject(out,
                                null,
                                myTypeMapping.getDescription(Object.class));
                }
                else {
                    // We'd prefer to send it via SHMDATA methods, if we can:
                    // Right now, this means we're using a transport where both
                    // sides know they are on the same host and the feature is
                    // enabled.

                    // Here's where we can store the native array info
                    // if needed. If the object is not one of primitive types,
                    // it will be null.
                    JniPJRmi.ArrayHandle arrayInfo = null;

                    if (myUseShmdata && myTransport.isLocalhost()) {
                        // Do this inside a try-catch since we don't want any
                        // form of exception to take down the thread if we can
                        // help it.
                        try {
                            // Determine the type of the array and write it
                            // natively, after casting to the appropriate type.
                            if (reference instanceof boolean[]) {
                                arrayInfo = JniPJRmi.writeArray((boolean[])reference);
                            }
                            else if (reference instanceof byte[]) {
                                arrayInfo = JniPJRmi.writeArray((byte   [])reference);
                            }
                            else if (reference instanceof short[]) {
                                arrayInfo = JniPJRmi.writeArray((short  [])reference);
                            }
                            else if (reference instanceof int[]) {
                                arrayInfo = JniPJRmi.writeArray((int    [])reference);
                            }
                            else if (reference instanceof long[]) {
                                arrayInfo = JniPJRmi.writeArray((long   [])reference);
                            }
                            else if (reference instanceof float[]) {
                                arrayInfo = JniPJRmi.writeArray((float  [])reference);
                            }
                            else if (reference instanceof double[]) {
                                arrayInfo = JniPJRmi.writeArray((double [])reference);
                            }
                        }
                        catch (Throwable e) {
                            // Nothing, we'll proceed with the previous Pickle
                            // format
                            LOG.log(Level.FINE, "writeArray() failed unexpectedly", e);
                        }
                    }

                    // Did we use the native method successfully? If so, we'll
                    // send the info from that process.
                    if (arrayInfo != null) {
                        out.writeByte(PythonValueFormat.SHMDATA.id);
                        writeUTF16   (out, arrayInfo.filename);
                        out.writeInt (arrayInfo.numElems);
                        out.writeChar(arrayInfo.type);
                    }

                    // Otherwise, proceed with normal pickle protocol
                    else {
                        // Convert it to a byte[] and compress
                        final byte[] bytes =
                            Snappy.compress(
                                ourPythonPickle.get().toByteArray(reference)
                            );

                        // Marshall it
                        out.writeByte(PythonValueFormat.SNAPPY_PICKLE.id);
                        out.writeInt (bytes.length);
                        out.write    (bytes, 0, bytes.length);
                    }
                }
            }
            else if (arg instanceof PythonObjectImpl) {
                out.writeByte(PythonValueFormat.PYTHON_REFERENCE.id);
                out.writeInt (((PythonObjectImpl)arg).getId());
            }
            else {
                // Sending by reference
                final TypeDescription argType =
                    myTypeMapping.getDescription(
                        (arg == null) ? Object.class : arg.getClass()
                    );
                out.writeByte(PythonValueFormat.REFERENCE.id);
                writeObject(out, arg, argType);
            }
        }

        /**
         * Listen for incoming data. We do this while the connection is still
         * good.
         */
        private void listen()
        {
            // Variables for stats gathering
            final long startTimeMs = System.currentTimeMillis();
            int        numRequests  = 0;

            // How we pull in the data
            final ByteList payload = new ByteList(1024 * 1024);
            final byte[]   header  = new byte[17];
            final byte[]   buffer  = new byte[64 * 1024];

            // Keep reading the stream socket until it's done
            while (true) {
                // Set everything to empty to start with
                payload.clear();
                byte        typeId   = -1;
                long        threadId = -1;
                int         reqId    = -1;
                MessageType type     = null;
                try {
                    // Read in the header; this should be a byte (type ID)
                    // followed by an int (size). We try to read this in a
                    // single go since reading a byte and then an int winds
                    // making 5 calls to recvfrom(), as opposed to just 1!
                    int headerRead = 0;
                    while (headerRead < header.length) {
                        final int read = myIn.read(header,
                                                   headerRead,
                                                   header.length - headerRead);
                        if (read < 0) {
                            break;
                        }
                        else {
                            headerRead += read;
                        }
                    }
                    if (headerRead < header.length) {
                        throw new EOFException("EOF when reading PJRmi header");
                    }

                    // Okay, unpack the header now
                    typeId = header[0];
                    threadId       = (((((long)header[ 1]) & 0xff) << 56) |
                                      ((((long)header[ 2]) & 0xff) << 48) |
                                      ((((long)header[ 3]) & 0xff) << 40) |
                                      ((((long)header[ 4]) & 0xff) << 32) |
                                      ((((long)header[ 5]) & 0xff) << 24) |
                                      ((((long)header[ 6]) & 0xff) << 16) |
                                      ((((long)header[ 7]) & 0xff) <<  8) |
                                      ((((long)header[ 8]) & 0xff)      ));
                    reqId          = (((( (int)header[ 9]) & 0xff) << 24) |
                                      ((( (int)header[10]) & 0xff) << 16) |
                                      ((( (int)header[11]) & 0xff) <<  8) |
                                      ((( (int)header[12]) & 0xff)      ));
                    final int size = (((( (int)header[13]) & 0xff) << 24) |
                                      ((( (int)header[14]) & 0xff) << 16) |
                                      ((( (int)header[15]) & 0xff) <<  8) |
                                      ((( (int)header[16]) & 0xff)      ));

                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(
                            "Read " +
                            "typeId = "   + (int)typeId  + " " +
                            "('"          + (char)typeId + "'), " +
                            "threadId = " + threadId     + " " +
                            "reqId = "    + reqId        + " " +
                            "size = "     + size
                        );
                    }

                    // Now read the payload. We keep reading until we believe
                    // that we got everything we care about. The payload might
                    // be split over several packets etc.
                    int totalRead = 0;
                    while (totalRead < size) {
                        // Pull in all the data we can into the local buffer
                        final int read =
                            myIn.read(
                                buffer,
                                0,
                                Math.min(buffer.length, size - totalRead)
                            );

                        // Check for EOF
                        if (read < 0) {
                            break;
                        }
                        else {
                            payload.append(buffer, 0, read);
                            totalRead += read;
                        }
                    }

                    // Now figure out the incoming message type
                    type = MessageType.byId(typeId);
                    if (type == null) {
                        throw new IllegalArgumentException(
                            "Unknown message type ID: " + typeId
                        );
                    }

                    // Good?
                    if (totalRead == size) {
                        // Now we have the payload we can log
                        if (LOG.isLoggable(Level.FINER)) {
                            LOG.finer(
                                "Received " +
                                "typeId = "    + (int)typeId  + " " +
                                "('"           + (char)typeId + "'), " +
                                PJRmi.toString(payload)
                            );
                        }
                        numRequests++;

                        // See if we have workers or not
                        if (myWorkers == null) {
                            // Nope, handle directly, and time it here
                            final Instrumentor instr = myInstrumentors[type.ordinal()];
                            final long start = instr.start();

                            // Whether we acquired the global lock, or not
                            boolean lockedGlobal = false;

                            // The result goes in here. It's important that
                            // no-one else uses this buffer for anything; we own
                            // it here.
                            final ByteArrayDataOutputStream sendBuf = mySendBufs.get();

                            // Do all this inside a try-catch since we don't
                            // want any form of exception to take down the
                            // thread if we can help it
                            try {
                                if (type.shouldLockFor) {
                                    myLockManager.lockGlobal();
                                    lockedGlobal = true;
                                }

                                // Handle the incoming payload. We don't have a
                                // VirtualThread in this case so we pass in null.
                                receive(type,
                                        threadId,
                                        null,
                                        reqId,
                                        payload,
                                        sendBuf,
                                        myOut);
                            }
                            finally {
                                if (lockedGlobal) {
                                    // Release the global lock now. This should
                                    // never fail in a perfect world but it's
                                    // possible for users to do something bad to
                                    // the lock. If that's the case then we
                                    // simply drop the connection by exiting
                                    // this method. The issue here is that
                                    // receive() will have already sent a
                                    // response and we don't want to send yet
                                    // _another_ response (with this exception)
                                    // since that will pollute the protocol
                                    // stream.
                                    try {
                                        myLockManager.unlockGlobal();
                                    }
                                    catch (Throwable t) {
                                        LOG.log(
                                            Level.SEVERE,
                                            "Terminating listener on locking exception",
                                            t
                                        );
                                        return;
                                    }
                                }

                                // Try to send the reply. This has to be done
                                // after we've dropped the global lock since we
                                // don't want the client thread initiating
                                // another request (over a different connection
                                // to us) and touching the lock. This is an
                                // unlikely version of the same problem in the
                                // worker pattern.
                                send(sendBuf, myOut);

                                // And we're finally done
                                instr.end(start);
                            }
                        }
                        else {
                            // Hand off to a worker
                            Worker worker = myWorkers.poll();
                            if (worker == null) {
                                // Need to create a new worker and set it running
                                worker = new Worker(getName() + "#Worker" + ++myNumWorkers);
                                worker.start();
                            }

                            // Find the virtual thread associated with the
                            // threadId. The myVirtualThreads Map is only ever
                            // touched in this thread.
                            VirtualThread thread = myVirtualThreads.get(threadId);
                            if (thread == null) {
                                thread = new VirtualThread(getName() + ":" + threadId);
                                myVirtualThreads.put(threadId, thread);
                            }
                            worker.handle(type, threadId, thread, reqId, payload, myOut);
                        }
                    }
                    else {
                        // Junk this, we don't understand it
                        throw new IllegalArgumentException(
                            "Received malformed request: " +
                            "typeId = "    + (int)typeId  + " " +
                            "('"           + (char)typeId + "'), " +
                            "threadId = "  + threadId     + " " +
                            "reqId = "     + reqId        + " " +
                            "size = "      + size         + " " +
                            "size-read = " + totalRead    + ": " +
                            PJRmi.toString(payload)
                        );
                    }
                }
                catch (Throwable e) {
                    // Attempt to return everything back to the caller. This
                    // might be a problem with the connection itself, in which
                    // case we'll simply bail out at the end anyhow (when we try
                    // to send the exception).
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(
                            "Encountered exception " +
                            "when handling " + type + " " + PJRmi.toString(payload) + ":\n" +
                            stackTraceToString(e)
                        );
                    }

                    // What we'll be sending back
                    final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();

                    // We'll treat this like a normal object
                    final TypeDescription exDesc =
                        myTypeMapping.getDescription(e.getClass());
                    final long exHandle = myHandleMapping.addReference(e);

                    try {
                        // Populate with the exception information
                        bados.dataOut.writeInt (exDesc.getTypeId());
                        bados.dataOut.writeLong(exHandle);
                        bados.dataOut.writeInt (-1);

                        // And send
                        sendMessage(myOut,
                                    MessageType.EXCEPTION,
                                    threadId,
                                    reqId,
                                    bados.bytes);
                    }
                    catch (SocketException ee) {
                        // This is probably fine if the other side closed the
                        // connection
                        if (e instanceof EOFException) {
                            LOG.info("Looks like the client disconnected: " + ee);
                        }
                        else {
                            LOG.warning("Error when sending back " + e + ": " + ee);
                        }
                        break;
                    }
                    catch (IOException ee) {
                        // This is probably fine if the other side closed the
                        // connection. We may get IOException on some transports
                        // while trying to send the exception.
                        if (e instanceof EOFException) {
                            LOG.info("Looks like the client disconnected: " + ee);
                        }
                        else {
                            LOG.severe("IO error when sending " + e + ": " + ee);
                        }
                        break;
                    }
                }
            }

            // Dump some auditing information
            final long durationMs = System.currentTimeMillis() - startTimeMs;
            final Collection<String> classes =
                myTypeMapping.getClasses()
                             .stream()
                             .map(Class::getName)
                             .collect(Collectors.toSet());
            LOG.info("ConnectionStats:: " +
                     "Name: "               + myName            + "; " +
                     "Duration: "           + durationMs + "ms" + "; " +
                     "NumRequests: "        + numRequests       + "; " +
                     "NumAccessedClasses: " + classes.size()    + "; " +
                     "AccessedClasses: "    + classes);
        }
        /** For use in the listen() method only. */
        private final ThreadLocalByteArrayDataOutputStream mySendBufs =
            new ThreadLocalByteArrayDataOutputStream();

        /**
         * Handle a raw set of bytes making up a payload. This could throw
         * pretty much any exception depending on what it does.
         *
         * <p>The clientReceiver stream should <i>not</i> be used for sending back
         * messages directly from this method; they should be put into the
         * result stream. This is to avoid race conditions in the worker code.
         */
        private void receive(final MessageType type,
                             final long threadId,
                             final VirtualThread thread,
                             final int reqId,
                             final ByteList payload,
                             final ByteArrayDataOutputStream result,
                             final DataOutputStream clientReceiver)
            throws Throwable
        {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Handling " + type + ": " + PJRmi.toString(payload));
            }

            // This should always be empty when we get it
            if (result.bytes.size() != 0) {
                throw new IllegalStateException(
                    "Result buffer was non-empty: " +
                    PJRmi.toString(result.bytes.toByteArray())
                );
            }

            // Handle whatever it ws
            switch (type) {
            case INSTANCE_REQUEST:
                handleInstanceRequest(threadId, reqId, payload, result);
                return;

            case ADD_REFERENCE:
                handleAddReference(threadId, reqId, payload, result);
                return;

            case DROP_REFERENCES:
                handleDropReferences(threadId, reqId, payload, result);
                return;

            case TYPE_REQUEST:
                handleTypeRequest(threadId, reqId, payload, result);
                return;

            case METHOD_CALL:
                handleMethodCall(threadId, thread, reqId, payload, result);
                return;

            case TO_STRING:
                handleToString(threadId, reqId, payload, result);
                return;

            case GET_FIELD:
                handleGetField(threadId, reqId, payload, result);
                return;

            case SET_FIELD:
                handleSetField(threadId, reqId, payload, result);
                return;

            case GET_ARRAY_LENGTH:
                handleGetArrayLength(threadId, reqId, payload, result);
                return;

            case NEW_ARRAY_INSTANCE:
                handleNewArrayInstance(threadId, reqId, payload, result);
                return;

            case OBJECT_CAST:
                handleObjectCast(threadId, reqId, payload, result);
                return;

            case LOCK:
                handleLock(threadId, reqId, payload, result);
                return;

            case UNLOCK:
                handleUnlock(threadId, reqId, payload, result);
                return;

            case INJECT_CLASS:
                handleInjectClass(threadId, reqId, payload, result);
                return;

            case INJECT_SOURCE:
                handleInjectSource(threadId, reqId, payload, result);
                return;

            case GET_VALUE_OF:
                handleGetValueOf(threadId, reqId, payload, result);
                return;

            case GET_CALLBACK_HANDLE:
                handleGetCallbackHandle(threadId, reqId, payload, result,
                                        clientReceiver);
                return;

            case CALLBACK_RESPONSE:
                handleCallbackResponse(threadId, reqId, payload, result);
                return;

            case GET_PROXY:
                handleGetProxy(threadId, reqId, payload, result,
                               clientReceiver);
                return;

            default:
                throw new IllegalArgumentException(
                    "Unhandled incoming message type: " + type
                );
            }
        }

        /**
         * Put a ByteArrayOutputStream as the payload into a buffer which we can
         * later send over the wire.
         *
         * <p>The payload may be null if there is none. This can be true for
         * simple ACK messages, for example.
         */
        private void buildMessage(final DataOutputStream      out,
                                  final MessageType           type,
                                  final long                  threadId,
                                  final int                   reqId,
                                  final ByteArrayOutputStream payload)
            throws IOException
        {
            final long start = myInstrumentors[type.ordinal()].start();
            try {
                // The message type is always the first byte in the message
                out.writeByte(type.id);

                // And the thread ID and request number is the next 12 (8 and 4)
                out.writeLong(threadId);
                out.writeInt (reqId);

                // Send any payload
                if (payload != null && payload.size() > 0) {
                    if (LOG.isLoggable(Level.FINER)) {
                        LOG.finer(
                            "Creating " + type + " '" + (char)type.id + "' " +
                            "for thread ID " + threadId + " " +
                            "and request ID " + reqId + ": " +
                            PJRmi.toString(payload.toByteArray())
                        );
                    }

                    // Write out the payload size and whatever it is
                    out.writeInt(payload.size());
                    payload.writeTo(out);
                }
                else {
                    if (LOG.isLoggable(Level.FINER)) {
                        LOG.finer("Building: " + type + " <>");
                    }

                    // No payload so size is zero
                    out.writeInt(0);
                }

                // Ensure everything is in there
                out.flush();
            }
            finally {
                myInstrumentors[type.ordinal()].end(start);
            }
        }

        /**
         * Send a ByteArrayDataOutputStream over the wire.
         *
         * <p>This could be called from multiple threads so it needs to be
         * synchronized in order to prevent mangling the output stream.
         */
        private synchronized void send(final ByteArrayDataOutputStream msg,
                                       final DataOutputStream          out)
            throws IOException
        {
            // Push the bytes to the output stream and ensure that they are
            // sent, using a flush()
            if (msg.bytes.size() > 0) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer("Sending: " + PJRmi.toString(msg.bytes.toByteArray()));
                }
                msg.bytes.writeTo(out);
                out.flush();
            }
        }

        /**
         * Build a message and send it.
         */
        private void sendMessage(final DataOutputStream      out,
                                 final MessageType           type,
                                 final long                  threadId,
                                 final int                   reqId,
                                 final ByteArrayOutputStream payload)
            throws IOException
        {
            final ByteArrayDataOutputStream sendBuf = mySendMessageBufs.get();
            buildMessage(sendBuf.dataOut, type, threadId, reqId, payload);
            send(sendBuf, out);
        }
        /** Only used by sendMessage(). */
        private final ThreadLocalByteArrayDataOutputStream mySendMessageBufs =
            new ThreadLocalByteArrayDataOutputStream();

        /**
         * Convert an object to a payload byte stream, following the given
         * return format.
         *
         * @param objectType Type that the client should use when interpreting
         *                   the object. May be {@code null} when
         *                   {@code valueFormat} is not
         *                   {@link PythonValueFormat#REFERENCE}
         */
        private void renderObject(final long                      threadId,
                                  final int                       reqId,
                                  final ByteArrayDataOutputStream buf,
                                  final PythonValueFormat         valueFormat,
                                  final Object                    object,
                                  final TypeDescription           objectType)
            throws Throwable
        {
            // What we'll be sending back
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();

            switch (valueFormat) {
            case REFERENCE:
            case PYTHON_REFERENCE:
                // What do we have to give back?
                if (object instanceof PythonObjectImpl) {
                    // A wrapped Python object
                    bados.dataOut.writeInt(((PythonObjectImpl)object).getId());
                    buildMessage(buf.dataOut,
                                 MessageType.PYTHON_REFERENCE,
                                 threadId,
                                 reqId,
                                 bados.bytes);
                }
                else if (object instanceof PythonProxyHandler) {
                    // A Python object proxying for a Java one
                    bados.dataOut.writeInt(((PythonProxyHandler)object).getId());
                    buildMessage(buf.dataOut,
                                 MessageType.PYTHON_REFERENCE,
                                 threadId,
                                 reqId,
                                 bados.bytes);
                }
                else if (valueFormat == PythonValueFormat.PYTHON_REFERENCE) {
                    // We can't turn this into a Python object reference, sorry
                    throw new IllegalArgumentException(
                        "Can't send a " +
                        (object == null ? "null" : object.getClass().toString()) + " " +
                        "as a Python reference"
                    );
                }
                else {
                    // This this is just a Java object
                    writeObject(bados.dataOut, object, objectType);
                    buildMessage(buf.dataOut,
                                 MessageType.ARBITRARY_ITEM,
                                 threadId,
                                 reqId,
                                 bados.bytes);
                }
                return;

            case RAW_PICKLE:
            case SNAPPY_PICKLE:
            case BESTEFFORT_PICKLE:
            case BESTEFFORT_SNAPPY_PICKLE:
                // Figure out which PythonPickle instance to use
                final PythonPickle pickle;
                if (valueFormat == PythonValueFormat.RAW_PICKLE ||
                    valueFormat == PythonValueFormat.SNAPPY_PICKLE)
                {
                    pickle = ourPythonPickle.get();
                }
                else {
                    pickle = myBestEffortPythonPickle.get();
                }

                // Convert it to a byte[], and possibly compress it
                byte[] bytes = pickle.toByteArray(object);
                if (valueFormat == PythonValueFormat.SNAPPY_PICKLE ||
                    valueFormat == PythonValueFormat.BESTEFFORT_SNAPPY_PICKLE)
                {
                    bytes = Snappy.compress(bytes);
                }

                // Stuff this into our buffer
                // Number of bytes sent = data size + valueFormat byte
                bados.dataOut.writeInt (bytes.length + 1);
                bados.dataOut.writeByte(valueFormat.id);
                bados.dataOut.write    (bytes, 0, bytes.length);

                // And package it up
                buildMessage(buf.dataOut,
                             MessageType.PICKLE_BYTES,
                             threadId,
                             reqId,
                             bados.bytes);
                return;

            default:
                throw new IllegalArgumentException(
                    "Unhandled return format" + valueFormat
                );
            }
        }

        /**
         * Convert a deconstructed {@link JniPJRmi$ArrayHandle} object to a
         * payload byte stream.
         *
         * @param threadId Logical thread ID.
         * @param reqId    Request number.
         * @param buf      The buffer which will eventually be sent.
         * @param filename Name of the file to read from.
         * @param numElems Number of elements we expect to read.
         * @param type     Type of the array represented as a char, that
         *                 {@link JniPJRmi} understands.
         */
        private void writeShmObject(final long                      threadId,
                                    final int                       reqId,
                                    final ByteArrayDataOutputStream buf,
                                    final String                    filename,
                                    final int                       numElems,
                                    final char                      type)
            throws Throwable
        {
            // What we'll be sending back
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();

            // Let 'em know we're sending an ArrayHandle
            bados.dataOut.writeByte(PythonValueFormat.SHMDATA.id);

            // Write each input object to the output stream
            writeUTF16(bados.dataOut, filename);
            bados.dataOut.writeInt (numElems);
            bados.dataOut.writeChar(type);

            // And push it out over the wire
            buildMessage(buf.dataOut,
                         MessageType.SHMDATA_BYTES,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Write an Object to an output stream along with its type information,
         * according to that given type information.
         */
        private void writeObject(final DataOutputStream dataOut,
                                 final Object           object,
                                 final TypeDescription  typeInfo)
            throws IOException
        {
            final long start = myWriteObjectInstrumentor.start();

            // Always write the type
            dataOut.writeInt(typeInfo.getTypeId());

            // Now handle the object itself
            if (!typeInfo.isPrimitive()) {
                // Get a handle on the object to give back. If it's null then we
                // use the NULL handle.
                if (object == null) {
                    dataOut.writeLong(HandleMapping.NULL_HANDLE);
                }
                else {
                    dataOut.writeLong(
                        myHandleMapping.addReference(object)
                    );
                }

                // Potentially write out the value as raw data too. This is pro-
                // active support for the boxing on the Python side (so that it
                // doesn't have to make another callback to determine the raw
                // value of a boxed object). Note that not all primitive types
                // are boxed on the Python side.
                if (object instanceof java.lang.String) {
                    // Don't send "large" strings automatically out over the
                    // wire; wait for them to be requested.
                    final String string = (String)object;
                    if (string.length() < 32768) {
                        writeUTF16(dataOut, string);
                    }
                    else {
                        dataOut.writeInt(-1);
                    }
                }
                else if (object instanceof java.lang.Boolean) {
                    dataOut.writeInt(1);
                    dataOut.writeBoolean((Boolean)object);
                }
                else if (object instanceof java.lang.Byte) {
                    dataOut.writeInt(Byte.BYTES);
                    dataOut.writeByte((Byte)object);
                }
                else if (object instanceof java.lang.Short) {
                    dataOut.writeInt(Short.BYTES);
                    dataOut.writeShort((Short)object);
                }
                else if (object instanceof java.lang.Integer) {
                    dataOut.writeInt(Integer.BYTES);
                    dataOut.writeInt((Integer)object);
                }
                else if (object instanceof java.lang.Long) {
                    dataOut.writeInt(Long.BYTES);
                    dataOut.writeLong((Long)object);
                }
                else if (object instanceof java.lang.Float) {
                    dataOut.writeInt(Float.BYTES);
                    dataOut.writeFloat((Float)object);
                }
                else if (object instanceof java.lang.Double) {
                    dataOut.writeInt(Double.BYTES);
                    dataOut.writeDouble((Double)object);
                }
                else {
                    // No raw representation
                    dataOut.writeInt(-1);
                }
            }
            else if (typeInfo.isVoid()) {
                // You can't have an object of type 'void' so grumble about this
                if (object != null) {
                    throw new IllegalArgumentException(
                        "Attempt to write out a non-null as 'void': " + object
                    );
                }

                // Push out the NULL handle
                dataOut.writeLong(HandleMapping.NULL_HANDLE);
                dataOut.writeInt(-1);
            }
            else if (object == null) {
                throw new NullPointerException(
                    "Attempt to write out null as a primitive"
                );
            }
            else {
                // We need to marshal the object as a native value
                switch (typeInfo.getName()) {
                case "boolean":
                    dataOut.writeBoolean((Boolean)object);
                    break;

                case "byte":
                    dataOut.writeByte((Byte)object);
                    break;

                case "char":
                    dataOut.writeChar((Character)object);
                    break;

                case "double":
                    dataOut.writeDouble((Double)object);
                    break;

                case "float":
                    dataOut.writeFloat((Float)object);
                    break;

                case "int":
                    dataOut.writeInt((Integer)object);
                    break;

                case "long":
                    dataOut.writeLong((Long)object);
                    break;

                case "short":
                    dataOut.writeShort((Short)object);
                    break;

                case "void":
                    // We write nothing out
                    break;

                default:
                    throw new RuntimeException(
                        "Unhandled type when marshalling object: " +
                        typeInfo
                    );
                }
            }
            myWriteObjectInstrumentor.end(start);
        }

        /**
         * Read an Object from an input stream along with its type information.
         */
        private ReadObjectResult readObject(final ByteList bytes, int offset)
            throws IOException
        {
            final long start = myReadObjectInstrumentor.start();

            // How it's being sent down the wire.
            final MethodDescription.ArgumentType wireType =
                MethodDescription.ArgumentType.byId(bytes.get(offset++));

            // What we'll give back
            Object result;

            // See what we were given
            switch (wireType) {
            case VALUE: {
                // First read the type information
                final int typeId = readInt(bytes, offset);
                offset += Integer.BYTES;

                final TypeDescription typeDesc = myTypeMapping.getDescription(typeId);
                if (typeDesc == null) {
                    throw new IllegalArgumentException("Unknown type ID: " + typeId);
                }

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(
                        "Unmarshalling " + typeId + " " +
                        "from raw bits at offset " + offset
                    );
                }

                // If it's sent natively down the wire then we convert from
                // the raw bits
                if (typeDesc.getName().equals("void")) {
                    // Like Method#invoke() we treat voids as nulls; there will
                    // not be any associated data.
                    result = null;
                }
                else if (typeDesc.getName().equals("boolean") ||
                         typeDesc.getName().equals("java.lang.Boolean"))
                {
                    result = Boolean.valueOf(
                        bytes.get(offset++) != 0
                    );
                }
                else if (typeDesc.getName().equals("byte") ||
                         typeDesc.getName().equals("java.lang.Byte"))
                {
                    result = Byte.valueOf(
                        bytes.get(offset++)
                    );
                }
                else if (typeDesc.getName().equals("double") ||
                         typeDesc.getName().equals("java.lang.Double"))
                {
                    result = Double.valueOf(
                        Double.longBitsToDouble(readLong(bytes, offset))
                    );
                    offset += Long.BYTES;
                }
                else if (typeDesc.getName().equals("float") ||
                         typeDesc.getName().equals("java.lang.Float"))
                {
                    result = Float.valueOf(
                        Float.intBitsToFloat(readInt(bytes, offset))
                    );
                    offset += Integer.BYTES;
                }
                else if (typeDesc.getName().equals("int") ||
                         typeDesc.getName().equals("java.lang.Integer"))
                {
                    result = Integer.valueOf(readInt(bytes, offset));
                    offset += Integer.BYTES;
                }
                else if (typeDesc.getName().equals("long") ||
                         typeDesc.getName().equals("java.lang.Long"))
                {
                    result = Long.valueOf(readLong(bytes, offset));
                    offset += Long.BYTES;
                }
                else if (typeDesc.getName().equals("short") ||
                         typeDesc.getName().equals("java.lang.Short"))
                {
                    result = Short.valueOf(readShort(bytes, offset));
                    offset += Short.BYTES;
                }
                else if (typeDesc.getName().equals("char")                ||
                         typeDesc.getName().equals("java.lang.Character") ||
                         typeDesc.getName().equals("[C")                  ||
                         typeDesc.getName().equals("java.lang.String"))
                {
                    // Strings, char[]s, chars are sent over as UTF-16 strings
                    // and handled appropriately
                    final int count = readInt(bytes, offset);
                    offset += Integer.BYTES;
                    final byte[] buffer = getByteArray(count);
                    for (int i=0; i < count; i++) {
                        buffer[i] = bytes.get(offset++);
                    }
                    final String string = new String(buffer, 0, count, "UTF-16");

                    // Switch to the desired type
                    if (typeDesc.getName().equals("char") ||
                        typeDesc.getName().equals("java.lang.Character"))
                    {
                        if (string.length() != 1) {
                            throw new IllegalArgumentException(
                                "Got a char in a String of length other than 1: " +
                                "'" + string + "'"
                            );
                        }
                        result = Character.valueOf(string.charAt(0));
                    }
                    else if (typeDesc.getName().equals("[C")) {
                        result = string.toCharArray();
                    }
                    else if (typeDesc.getName().equals("java.lang.String")) {
                        result = string;
                    }
                    else {
                        // Ensure that the if statements all match up
                        throw new AssertionError("Unexpected type: " + typeDesc);
                    }
                }
                else if (typeDesc.getName().equals("[Z")) {
                    final boolean[] array = new boolean[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = (
                            bytes.get(offset++) != 0
                        );
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[B")) {
                    final byte[] array = new byte[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = (
                            bytes.get(offset++)
                        );
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[D")) {
                    final double[] array = new double[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = Double.longBitsToDouble(readLong(bytes, offset));
                        offset += Long.BYTES;
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[F")) {
                    final float[] array = new float[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = Float.intBitsToFloat(readInt(bytes, offset));
                        offset += Integer.BYTES;
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[I")) {
                    final int[] array = new int[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = readInt(bytes, offset);
                        offset += Integer.BYTES;
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[J")) {
                    final long[] array = new long[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = readLong(bytes, offset);
                        offset += Long.BYTES;
                    }
                    result = array;
                }
                else if (typeDesc.getName().equals("[S")) {
                    final short[] array = new short[readInt(bytes, offset)];
                    offset += Integer.BYTES;
                    for (int i=0; i < array.length; i++) {
                        array[i] = readShort(bytes, offset);
                        offset += Short.BYTES;
                    }
                    result = array;
                }
                else if (typeDesc.getName().startsWith("[")) {
                    // An array of <something>s with a known length
                    final int length = readInt(bytes, offset);
                    offset += Integer.BYTES;

                    // Create an array of the right type by reflection, and
                    // populate it
                    result = Array.newInstance(typeDesc.getArrayComponentType(),
                                               length);
                    for (int i=0; i < length; i++) {
                        final ReadObjectResult ror = readObject(bytes, offset);
                        offset = ror.offset;
                        Array.set(result, i, ror.object);
                    }
                }
                else if (typeDesc.getName().equals("java.util.Map")) {
                    // How many entries
                    final int count = readInt(bytes, offset);
                    offset += Integer.BYTES;

                    // Create and populate
                    final Map<Object, Object> map = new HashMap<>();
                    for (int i=0; i < count; i++) {
                        // Pull out the key and value
                        final ReadObjectResult keyRor   = readObject(bytes, offset);
                        offset = keyRor.offset;
                        final ReadObjectResult valueRor = readObject(bytes, offset);
                        offset = valueRor.offset;

                        // Stuff them into the map
                        map.put(keyRor.object, valueRor.object);
                    }

                    result = map;
                }
                else if (typeDesc.getName().equals("java.util.Set")) {
                    final int count = readInt(bytes, offset);
                    offset += Integer.BYTES;

                    final Set<Object> set = new HashSet<>();
                    for (int i=0; i < count; i++) {
                        final ReadObjectResult ror = readObject(bytes, offset);
                        offset = ror.offset;
                        set.add(ror.object);
                    }
                    result = set;
                }
                else if (typeDesc.getName().equals("java.util.Collection") ||
                         typeDesc.getName().equals("java.util.List"))
                {
                    final int count = readInt(bytes, offset);
                    offset += Integer.BYTES;

                    final List<Object> list = new ArrayList<>(count);
                    for (int i=0; i < count; i++) {
                        final ReadObjectResult ror = readObject(bytes, offset);
                        offset = ror.offset;
                        list.add(ror.object);
                    }
                    result = list;
                }
                else if (typeDesc.getName().equals("com.deshaw.pjrmi.PythonObject")) {
                    // A negative ID means null
                    final int objectId = readInt(bytes, offset);
                    offset += Integer.BYTES;
                    result = (objectId < 0)
                        ? null
                        : new PythonObjectImpl("PythonObject#" + objectId,
                                               objectId,
                                               myOut);
                }
                else if (typeDesc.getName().equals("com.deshaw.pjrmi.PythonSlice")) {
                    ReadObjectResult ror = readObject(bytes, offset);
                    offset = ror.offset;
                    final Object sliceStart = ror.object;
                    ror = readObject(bytes, offset);
                    offset = ror.offset;
                    final Object sliceStop = ror.object;
                    ror = readObject(bytes, offset);
                    offset = ror.offset;
                    final Object sliceStep = ror.object;

                    // Now build it. We might have been given any form of Number
                    // but we convert them all to longs.
                    result = new PythonSlice(
                        (sliceStart != null) ? ((Number)sliceStart).longValue() : null,
                        (sliceStop  != null) ? ((Number)sliceStop ).longValue() : null,
                        (sliceStep  != null) ? ((Number)sliceStep ).longValue() : null
                    );
                }
                else if (typeDesc.getName().equals("com.deshaw.hypercube.Hypercube")) {
                    // We have a basic understanding for certain types of
                    // hypercube. In the future we will add code to allow the
                    // array data to be broken up into chunks, so that cubes of
                    // more than 2^31 elements can be passed (and we don't need
                    // to change the wire format to handle it). Since that's
                    // quite a lot to be passing as a single argument, we don't
                    // yet supoort it, and right now we can't do that in a
                    // ByteList anyhow.
                    ReadObjectResult ror = readObject(bytes, offset);
                    offset = ror.offset;
                    final Object shape = ror.object;
                    final int chunks = readInt(bytes, offset);
                    offset += Integer.BYTES;
                    ror = readObject(bytes, offset);
                    offset = ror.offset;
                    final Object array = ror.object;
                    final Hypercube<?> cube;
                    if (chunks != 1) {
                        throw new IllegalArgumentException(
                            "Expected only one array chunk"
                        );
                    }
                    if (array instanceof boolean[]) {
                        cube = BooleanHypercube.wrap((boolean[])array);
                    }
                    else if (array instanceof float[]) {
                        cube = FloatHypercube.wrap((float[])array);
                    }
                    else if (array instanceof double[]) {
                        cube = DoubleHypercube.wrap((double[])array);
                    }
                    else if (array instanceof int[]) {
                        cube = IntegerHypercube.wrap((int[])array);
                    }
                    else if (array instanceof long[]) {
                        cube = LongHypercube.wrap((long[])array);
                    }
                    else {
                        throw new UnsupportedOperationException(
                            "Don't know how to wrap a " +
                            array.getClass().getSimpleName() + " in a hypercube"
                        );
                    }
                    result = cube.reshape((long[])shape);
                }
                else {
                    throw new UnsupportedOperationException(
                        "Don't know how to convert " + typeDesc + " " +
                        "from raw bytes"
                    );
                }
            }   break;

            case REFERENCE: {
                // Simply grab the handle and add the associated object, it
                // may be null which is okay
                final long handle = readLong(bytes, offset);
                offset += Long.BYTES;

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Grabbed handle " + handle);
                }
                result = myHandleMapping.getObject(handle);
            }   break;

            case SHMDATA: {
                // The object of interest was written natively, and we have to
                // read it. We use the incoming object, which is a deconstructed
                // {@link JniPJRmi$ArrayHandle}, to read it.

                // First, we read the filename as a String
                final int countString = readInt(bytes, offset);
                offset += Integer.BYTES;
                final byte[] bufferString = getByteArray(countString);
                for (int i=0; i < countString; i++) {
                    bufferString[i] = bytes.get(offset++);
                }
                final String filename = new String(bufferString, 0, countString, "UTF-16");

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("SHMDATA filename: " + filename);
                }

                // Then we read the number of elements
                final int numElems = Integer.valueOf(readInt(bytes, offset));
                offset += Integer.BYTES;

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("SHMDATA numElems: " + numElems);
                }

                // Read in the array type
                final int countChar = readInt(bytes, offset);
                offset += Integer.BYTES;
                final byte[] bufferChar = getByteArray(countChar);
                for (int i=0; i < countChar; i++) {
                    bufferChar[i] = bytes.get(offset++);
                }
                final String stringType = new String(bufferChar, 0, countChar, "UTF-16");

                // Let's make sure it was actually a char
                if (stringType.length() != 1) {
                    throw new IllegalArgumentException(
                        "Got a char in a String of length other than 1: " +
                        "'" + stringType + "'"
                    );
                }
                final char type = Character.valueOf(stringType.charAt(0));

                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("SHMDATA type: " + type);
                }

                // Now, we can read from the file
                result = JniPJRmi.readArray(filename, numElems, type);
            }   break;

            case METHOD: {
                // This is a method handle which we want to turn into a
                // functional interface.
                //
                // Right now we expect to be given the exact method which is
                // going to be used by the functional interface. However, that
                // requires the user to determine this up front. While that's
                // not particularly onerous, we might one day look to perform
                // the method binding on this side, as an extended feature. We
                // would probably do this by adding a flag to the request
                // indicating as such, and just using the method ID to allow us
                // to look up the name. Getting such method binding correct
                // probably isn't hard, per se, but will require a lot of care;
                // there are a number of gotchas in it. The method's relative
                // specificities values might come into play if and when we
                // choose to do it.
                //
                // The first value we read is a boolean represented by a byte.
                // At some point we might want to turn this into an 8bit flags
                // value (see above), but we can do that without changing the
                // wire format.

                final boolean isConstructor = (bytes.get(offset++) != 0);
                final int ifaceId = readInt(bytes, offset);
                offset += Integer.BYTES;
                final int klassId = readInt(bytes, offset);
                offset += Integer.BYTES;
                final int methodId = readInt(bytes, offset);
                offset += Integer.BYTES;
                final long handleId = readLong(bytes, offset);
                offset += Long.BYTES;

                // Now resolve what we can
                final TypeDescription iface = myTypeMapping.getDescription(ifaceId);
                if (iface == null) {
                    throw new IllegalArgumentException(
                        "No known class type for interface ID " + ifaceId
                    );
                }
                final TypeDescription klass = myTypeMapping.getDescription(klassId);
                if (klass == null) {
                    throw new IllegalArgumentException(
                        "No known class type for method class ID " + klassId
                    );
                }
                final MethodDescription method = isConstructor
                    ? klass.getConstructor(methodId)
                    : klass.getMethod     (methodId);
                if (method == null) {
                    throw new IllegalArgumentException(
                        (isConstructor ? "Constructor" : "Method") + " " +
                        "with ID " + methodId + " not found in " + klass
                    );
                }

                // It's okay for the instance to be null, depending on how we're
                // binding etc.
                final Object instance = myHandleMapping.getObject(handleId);

                // And defer to the helper method to give back the object that
                // we want
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Turning " +
                               (isConstructor ? "constructor " : "method ") +
                               method + " into functional call for " + iface);
                }
                result = getProxyForMethod(iface.getRepresentedClass(),
                                           klass,
                                           method,
                                           isConstructor,
                                           instance);
            }   break;

            case LAMBDA: {
                // The argument is given by invoking a lambda. We should get
                // method details (similar to the above) and then the arguments.
                final boolean isConstructor = (bytes.get(offset++) != 0);
                final int klassId = readInt(bytes, offset);
                offset += Integer.BYTES;
                final int methodId = readInt(bytes, offset);
                offset += Integer.BYTES;
                final long handleId = readLong(bytes, offset);
                offset += Long.BYTES;

                // Now the arguments
                final short numArgs = readShort(bytes, offset);
                offset += Short.BYTES;
                final Object args[] = new Object[numArgs];
                for (int i=0; i < numArgs; i++) {
                    final ReadObjectResult ror = readObject(bytes, offset);
                    args[i] = ror.object;
                    offset  = ror.offset;
                }

                // Now resolve what we got
                final TypeDescription typeDesc =
                    myTypeMapping.getDescription(klassId);
                if (typeDesc == null) {
                    throw new IllegalArgumentException(
                        "No known class type for method class ID " + klassId
                    );
                }
                final MethodDescription method =
                    isConstructor ? typeDesc.getConstructor(methodId)
                                  : typeDesc.getMethod     (methodId);
                if (method == null) {
                    throw new IllegalArgumentException(
                        (isConstructor ? "Constructor" : "Method") + " " +
                        "with ID " + methodId + " not found in " + typeDesc
                    );
                }
                final Object instance = myHandleMapping.getObject(handleId);

                // We invoke the method and give back its result
                try {
                    result =
                        isConstructor ? typeDesc.callConstructor(method.getIndex(),
                                                                 args)
                                      : typeDesc.callMethod     (method.getIndex(),
                                                                 instance,
                                                                 args);
                }
                catch (Throwable t) {
                    throw new RuntimeException(
                        "Failed to invoke " +
                        (isConstructor ? "constructor" : "method") + " " +
                        "with ID " + methodId + " in " + typeDesc +
                        (instance == null ? "" : " on object"),
                        t
                    );
                }
            }   break;

            default:
                throw new RuntimeException(
                    "Unhandled argument type " +
                    "received on the wire: " + wireType
                );
            }

            myReadObjectInstrumentor.end(start);
            return new ReadObjectResult(offset, result);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
        //                             HANDLERS                                  //

        /**
         * Handle an INSTANCE_REQUEST message.
         *
         * This is of the form:
         *  int16   : String length
         *  byte[]  : String bytes (string length in total)
         *
         * Gives back:
         *  int32   : Type ID
         *  int64   : Handle
         *  int32   : -1 (no raw data)
         * where Handle will be -ve if the result was null.
         */
        private void handleInstanceRequest(final long                      threadId,
                                           final int                       reqId,
                                           final ByteList                  payload,
                                           final ByteArrayDataOutputStream buf)
            throws IOException
        {
            // Need at least 4 bytes at the start of the message, for the string length
            if (payload.size() < 4) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // Grab the name of the object which we want
            final int size = readInt(payload, 0);

            // These are sent over as UTF-16
            final byte[] buffer = getByteArray(size);
            for (int i=0; i < size; i++) {
                buffer[i] = payload.get(i + 4);
            }
            final String name = new String(buffer, 0, size, "UTF-16");

            // Get the object
            final Object instance = getObjectInstance(name);
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            if (instance == null) {
                bados.dataOut.writeInt (myTypeMapping.getId(Object.class));
                bados.dataOut.writeLong(HandleMapping.NULL_HANDLE);
                bados.dataOut.writeInt (-1);
            }
            else {
                bados.dataOut.writeInt (myTypeMapping  .getId       (instance));
                bados.dataOut.writeLong(myHandleMapping.addReference(instance));
                bados.dataOut.writeInt (-1);
            }
            buildMessage(buf.dataOut,
                         MessageType.OBJECT_REFERENCE,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle an ADD_REFERENCE message.
         *
         * This is of the form:
         *  int64   : Handle
         *
         * Gives back and empty ACK.
         */
        private void handleAddReference(final long                      threadId,
                                        final int                       reqId,
                                        final ByteList                  payload,
                                        final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() != 8) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final long handle = readLong(payload, 0);
            myHandleMapping.addReference(handle);

            buildMessage(buf.dataOut, MessageType.EMPTY_ACK, threadId, reqId, null);
        }

        /**
         * Handle a DROP_REFERENCES message.
         *
         * This is of the form:
         *  int32     : Num handles
         *  handles[] :
         *    int64   : Handle
         *
         * Gives back an empty ACK.
         */
        private void handleDropReferences(final long                      threadId,
                                          final int                       reqId,
                                          final ByteList                  payload,
                                          final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() < 4) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // Our position in the payload data
            int offset = 0;

            // How many to drop
            final int count = readInt(payload, offset);
            offset += Integer.BYTES;

            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Dropping " + count + " references...");
            }

            // Drop them
            for (int i=0; i < count; i++) {
                final long handle = readLong(payload, offset);
                offset += Long.BYTES;

                myHandleMapping.dropReference(handle);
            }

            // ACK back
            buildMessage(buf.dataOut, MessageType.EMPTY_ACK, threadId, reqId, null);
        }

        /**
         * Handle a TYPE_REQUEST message.
         *
         * This is of the form:
         *  boolean : by-id or by-string flag
         *  type:
         *   int32   : Type ID
         *  or
         *   int32   : Name length
         *   bytes[] : Name
         *
         * Gives back by deferring to writeTypeDesc().
         */
        private void handleTypeRequest(final long                      threadId,
                                       final int                       reqId,
                                       final ByteList                  payload,
                                       final ByteArrayDataOutputStream buf)
            throws ClassNotFoundException,
                   IOException,
                   SecurityException
        {
            if (payload.size() < 1) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // Our position in the payload data
            int offset = 0;

            // Start off by finding out what sort of call this is
            final boolean isById = (payload.get(offset++) != 0);

            // Get the type description
            final TypeDescription desc;
            final int int32 = readInt(payload, offset);
            offset += Integer.BYTES;

            if (isById) {
                desc = myTypeMapping.getDescription(int32);
            }
            else {
                final StringBuilder sb = new StringBuilder();
                while (sb.length() < int32) {
                    sb.append((char)payload.get(offset++));
                }
                final String className = sb.toString();

                // See if we are allowed to give back this class
                if (!isClassPermitted(className)) {
                    final String msg = getClassNotPermittedMessage();
                    throw new SecurityException(
                        "Access permission denied for class \"" + className + "\"" +
                        (msg == null ? "" : ": " + msg)
                    );
                }

                // Okay to give back, handle requests for primitives specially
                final Class<?> klass;
                switch (className) {
                case "void":    klass = Void     .TYPE;           break;
                case "boolean": klass = Boolean  .TYPE;           break;
                case "byte":    klass = Byte     .TYPE;           break;
                case "short":   klass = Short    .TYPE;           break;
                case "int":     klass = Integer  .TYPE;           break;
                case "long":    klass = Long     .TYPE;           break;
                case "float":   klass = Float    .TYPE;           break;
                case "double":  klass = Double   .TYPE;           break;
                case "char":    klass = Character.TYPE;           break;
                default:        klass = Class.forName(className); break;
                }
                desc = myTypeMapping.getDescription(klass);
            }

            // Send it back
            writeTypeDesc(desc, threadId, reqId, buf);
        }

        /**
         * Handle an METHOD_CALL message.
         *
         * This is of the form:
         *  boolean : isConstructor flag
         *  int32   : Type ID
         *  byte    : PythonValueFormat
         *  int64   : Object handle
         *  int32   : Method/constructor index as defined by TypeDescription
         *  bytes[] : Arguments (if any) as raw bytes
         *
         * Gives back:
         *  int32    : Result type (possibly void)
         *  bytes[]  : The result (if native), or its handle (if an Object),
         *             or nothing (if void)
         */
        private void handleMethodCall(final long                      threadId,
                                      final VirtualThread             virtualThread,
                                      final int                       reqId,
                                      final ByteList                  payload,
                                      final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() < 18) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // Our position in the payload data
            int offset = 0;

            // Pull in the header information from the wire, everything up to
            // the, optional, arguments
            final boolean isConstructor = (payload.get(offset++) != 0);

            final int typeId = readInt(payload, offset);
            offset += Integer.BYTES;

            final byte valueFormatId = payload.get(offset++);

            final byte syncModeId = payload.get(offset++);

            final long handle = readLong(payload, offset);
            offset += Long.BYTES;

            final int index = readInt(payload, offset);
            offset += Integer.BYTES;

            // Figure out what we need to know in order to invoke this method
            final Object          object = myHandleMapping.getObject(handle);
            final TypeDescription klass  = myTypeMapping.getDescription(typeId);

            // Known type?
            if (klass == null) {
                throw new IllegalArgumentException("Unknown type " + typeId);
            }

            // Get the return format
            final PythonValueFormat valueFormat =
                PythonValueFormat.byId(valueFormatId);

            // Get the calling mode
            final SyncMode syncMode = SyncMode.byId(syncModeId);

            // Grab the method info, different depending on the type of call
            final MethodDescription method;
            if (isConstructor) {
                if (index < 0 || index >= klass.getNumConstructors()) {
                    throw new ArrayIndexOutOfBoundsException(
                        "Bad constructor index: " + index
                    );
                }
                else {
                    method = klass.getConstructor(index);
                }
            }
            else {
                if (index < 0 || index >= klass.getNumMethods()) {
                    throw new ArrayIndexOutOfBoundsException(
                        "Bad method index: " + index
                    );
                }
                else {
                    method = klass.getMethod(index);
                }
            }

            // What we now know
            if (LOG.isLoggable(Level.FINEST)) {
                // Don't call toString() on the object here since it might be a
                // Python shim and we don't want to wind up either recursing
                // indefinitely or similar if that call invokes Java methods or
                // similar.
                LOG.finest(
                    "Going to call " + method + " on " +
                    ((object == null) ? "" : "an instance of ") +
                    "class " + klass
                );
            }

            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            // Pull out all the arguments and turn them into Objects
            final Object[] args = new Object[method.getNumArguments()];
            for (short i=0; i < args.length; i++) {
                // Pull it from the data
                final ReadObjectResult read = readObject(payload, offset);
                offset  = read.offset;
                args[i] = read.object;

                // What did we get?
                if (LOG.isLoggable(Level.FINEST)) {
                    try {
                        LOG.finest("Argument " + i + " is: <" + args[i] + ">");
                    }
                    catch (Exception e) {
                        // Proxy classes might balk here, for example
                        LOG.finest("Error rendering argument " + i + ": e" + e);
                    }
                }
            }

            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            try {
                // We're calling down a level in the "stack"
                myCallDepth++;
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Call depth is now " + myCallDepth);
                }
                if (myCallDepth >= MAX_CALL_DEPTH) {
                    throw new StackOverflowError("Call depth became " + myCallDepth);
                }

                // Say what we're about to do
                if (LOG.isLoggable(Level.FINEST)) {
                    final StringBuilder sb = new StringBuilder();
                    for (Object arg : args) {
                        if (sb.length() > 0) {
                            sb.append(", ");
                        }
                        try {
                            sb.append(arg);
                        }
                        catch (Exception e) {
                            sb.append("<???>");
                        }
                        sb.append('<')
                          .append((arg == null) ? "NULL" : arg.getClass())
                          .append('>');
                    }
                    if (isConstructor) {
                        LOG.finest("Calling constructor " + index + " for " +
                                   klass + " with arguments [" + sb + "]");
                    }
                    else {
                        LOG.finest("Calling method " + index + " "+
                                   "'" + method + "' " +
                                   "with arguments [" + sb + "]");
                    }
                }

                // Now we can actually call the method
                final Object result;
                if (syncMode == SyncMode.SYNCHRONOUS) {
                    // A straight call. Give back whatever we got.
                    result = isConstructor
                        ? klass.callConstructor(index,         args)
                        : klass.callMethod     (index, object, args);
                }
                else if (syncMode == SyncMode.JAVA_THREAD) {
                    // Create a Future instance to give back and spawn the
                    // thread to set the result into that future. Try to give
                    // the future a reasonable name so that the user knows what
                    // it is. However we avoid expanding all the arguments for
                    // the same of brevity.
                    final MethodCallFuture future =
                        new MethodCallFuture(
                            "Future for call to: " +
                            method.getReturnType      ()
                                  .getRepresentedClass()
                                  .getSimpleName      () + " " +
                            klass .getRepresentedClass()
                                  .getSimpleName      () + "." +
                            (isConstructor ? "<init>"
                                           : method.getName() + "") +
                            (args.length > 0 ? "(...)" : "()")
                        );

                    // Get a caller, creating if needbe
                    MethodCaller caller = myMethodCallers.poll();
                    if (caller == null) {
                        // Create and spawn the caller thread
                        caller = new MethodCaller();
                        caller.start();
                    }

                    // And actually tell it to do the work
                    caller.handle(
                        () -> {
                            // Make the call, reaping any exceptions along the
                            // way and also giving them back via the Future
                            try {
                                // This is a method call so we need to acquire
                                // the global lock, if any, like any other
                                // method call would.
                                myLockManager.lockGlobal();
                                future.done(
                                    isConstructor
                                        ? klass.callConstructor(index,         args)
                                        : klass.callMethod     (index, object, args),
                                    false
                                );
                            }
                            catch (Throwable t) {
                                future.done(t, true);
                            }
                            finally {
                                myLockManager.unlockGlobal();
                            }
                        }
                    );

                    // And we'll be giving back the Future, rather than the
                    // result, for this calling mode
                    result = future;
                }
                else {
                    throw new UnsupportedOperationException(
                        "Unhandle sync mode " + syncMode
                    );
                }

                // What did we get?
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Result was <" + result + ">");
                }

                // Figure out what type to associate with the result.
                //
                // What we are trying to deal with here is type erasure of the
                // information associated with generics. For something like
                // Set<Integer> the get() method will always return an Object
                // since the runtime environment doesn't know the real type;
                // that it becomes an Int happens because the compiler inserts a
                // cast along the way. (This is why it's possible to put a
                // Double into such a List if we cast away its type information.
                // We'll only discover that when we pull the Double out as an
                // Integer.)
                //
                // What this means for us is that such methods return fun values
                // which have to be downcast into their "real" type on the
                // Python side if we want to use them. In order to deal with
                // this we give back the _actual_ type associated with the value
                // which the method returns. This is slightly icky since it
                // means that, say, get() on a List of CharSequences will
                // actually return a String, or StringBuilder, or whatever the
                // concrete implementation happens to be. Since we have _no_ way
                // to knowing at runtime that the E in List<E> is a CharSequence
                // we don't know to hand back a CharSequence. We argue that this
                // is actually okay and vaguely Pythonic since Python employs
                // duck-typing most of the time and what we hand back conforms
                // better with this view of the world. However, it's open to
                // abuse on the other side and we simply have to trust that the
                // Python users won't to anything special with the concrete
                // class; if they do then their code may fail if the class
                // changes.
                final TypeDescription returnType;
                if (syncMode != SyncMode.SYNCHRONOUS) {
                    assert(result instanceof Future);
                    returnType = myTypeMapping.getDescription(Future.class);
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest("Return type is " + returnType);
                    }
                }
                else if (result != null && method.isGenericReturnType()) {
                    returnType = myTypeMapping.getDescription(result.getClass());
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest("Return type is exposed as " + returnType);
                    }
                }
                else {
                    returnType = method.getReturnType();
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(
                            "Return type matches method result " + returnType
                        );
                    }
                }

                // Convert the result to a payload byte stream
                renderObject(threadId, reqId, buf, valueFormat, result, returnType);
            }
            finally {
                // We're coming back up here so reduce the depth
                myCallDepth--;
            }
        }

        /**
         * Handle a TO_STRING message.
         *
         * This is of the form:
         *  int64   : Handle
         *
         * Gives back the UTF16 string representation, or -1
         */
        private void handleToString(final long                      threadId,
                                    final int                       reqId,
                                    final ByteList                  payload,
                                    final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() != 8) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final long handle = readLong(payload, 0);

            final Object instance = myHandleMapping.getObject(handle);
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            if (instance == null) {
                bados.dataOut.writeInt(-1);
            }
            else {
                final String string = instance.toString();
                writeUTF16(bados.dataOut, string);
            }
            buildMessage(buf.dataOut,
                         MessageType.UTF16_VALUE,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle an GET_FIELD message.
         *
         * This is of the form:
         *  int32   : Object type ID
         *  int64   : Object handle
         *  int32   : Field index as defined by TypeDescription
         *
         * Gives back:
         *  int32    : Field type ID
         *  bytes[]  : The field (if native), or its handle (if an Object)
         */
        private void handleGetField(final long                      threadId,
                                    final int                       reqId,
                                    final ByteList                  payload,
                                    final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() != 16) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final int  typeId = readInt (payload,  0);
            final long handle = readLong(payload,  4);
            final int  index  = readInt (payload, 12);

            final TypeDescription desc = myTypeMapping.getDescription(typeId);
            final Object object = myHandleMapping.getObject(handle);

            // Grab the field and its type
            final Object          field;
            final TypeDescription fieldType;
            if (desc.isArray()) {
                // For arrays, this is the array index
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Getting array index " + index + " from " + handle);
                }
                field     = Array.get(object, index);
                fieldType =
                    myTypeMapping.getDescription(
                        object.getClass().getComponentType()
                    );
            }
            else {
                // Else it's just an actual field, grab its value
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("Getting field index " + index + " from " + handle);
                }
                field = desc.getField(index, object);

                // The type is the declared type, not the type of the object
                // (which might be a subclass)
                fieldType = desc.getField(index).getType();
            }

            // Simply marshal up the value which we got back
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            writeObject(bados.dataOut, field, fieldType);
            buildMessage(buf.dataOut,
                         MessageType.ARBITRARY_ITEM,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle an SET_FIELD message.
         *
         * This is of the form:
         *  int64   : Object handle
         *  int32   : Field index as defined by TypeDescription
         *  int32   : Field type
         *  bytes[] : The new value of the field (if native), or a handle
         *
         * Gives back empty ACK.
         */
        private void handleSetField(final long                      threadId,
                                    final int                       reqId,
                                    final ByteList                  payload,
                                    final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() < 17) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final int objTypeId = readInt (payload,  0);
            final long handle   = readLong(payload,  4);
            final int index     = readInt (payload, 12);

            final Object value = readObject(payload, 16).object;

            // The object we're setting on, this may be null if we're setting a
            // static field
            final Object object = myHandleMapping.getObject(handle);

            // The type information comes from the object itself
            final TypeDescription desc = myTypeMapping.getDescription(objTypeId);

            // Now just set it
            if (desc.isArray()) {
                // For arrays, this is the array index
                Array.set(object, index, value);
            }
            else {
                // Else it's just an actual field
                desc.setField(index, object, value);
            }

            // ACK comes back
            buildMessage(buf.dataOut, MessageType.EMPTY_ACK, threadId, reqId, null);
        }

        /**
         * Handle a GET_ARRAY_LENGTH message.
         *
         * This is of the form:
         *  int64   : Handle
         *
         * Gives back:
         *  int32   : The length
         */
        private void handleGetArrayLength(final long                      threadId,
                                          final int                       reqId,
                                          final ByteList                  payload,
                                          final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() != 8) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final long handle = readLong(payload, 0);

            final Object object = myHandleMapping.getObject(handle);
            if (object == null) {
                throw new IllegalArgumentException(
                    "No object for handle " + handle
                );
            }

            // Simply marshal up the value which we got back. If the object is
            // not an array then getLength() will throw an exception.
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt(Array.getLength(object));
            buildMessage(buf.dataOut,
                         MessageType.ARRAY_LENGTH,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle a NEW_ARRAY_INSTANCE message.
         *
         * This is of the form:
         *  int32   : Type ID
         *  int32   : Length
         *
         * Gives back:
         *  int32   : Type ID
         *  int64   : The array handle
         *  int32   : -1 (no raw data)
         */
        private void handleNewArrayInstance(final long                      threadId,
                                            final int                       reqId,
                                            final ByteList                  payload,
                                            final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() != 8) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final int typeId = readInt(payload, 0);
            final int length = readInt(payload, 4);

            // Known type?
            final TypeDescription klass  = myTypeMapping.getDescription(typeId);
            if (klass == null) {
                throw new IllegalArgumentException("Unknown type " + typeId);
            }

            // Create it
            final Object array =
                Array.newInstance(klass.getArrayComponentType(), length);

            // Return the new instance along with its type information
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt (typeId);
            bados.dataOut.writeLong(myHandleMapping.addReference(array));
            bados.dataOut.writeInt (-1);
            buildMessage(buf.dataOut,
                         MessageType.OBJECT_REFERENCE,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle a OBJECT_CAST message.
         *
         * This is of the form:
         *  int32   : Type ID
         *  int64   : Handle
         *
         * Gives back:
         *  int32   : Type ID
         *  int64   : The cast'd instance's handle
         *  int32   : -1 (no raw data)
         */
        private void handleObjectCast(final long                      threadId,
                                      final int                       reqId,
                                      final ByteList                  payload,
                                      final ByteArrayDataOutputStream buf)
            throws IOException
        {
            if (payload.size() != 12) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            final int typeId  = readInt (payload, 0);
            final long handle = readLong(payload, 4);

            // Known type?
            final TypeDescription klass  = myTypeMapping.getDescription(typeId);
            if (klass == null) {
                throw new IllegalArgumentException("Unknown type " + typeId);
            }

            // Get the object for this handle, allow it to be the null pointer
            final Object object = myHandleMapping.getObject(handle);
            if (object == null && handle != HandleMapping.NULL_HANDLE) {
                throw new IllegalArgumentException(
                    "No object for handle " + handle
                );
            }

            // Do the cast, we simply try to do it here; it doesn't matter
            // what we get back as we are only interested in if it throws an
            // exception when we do so.
            klass.getRepresentedClass().cast(object);

            // That worked so add a new reference to this object since the other
            // side will create a new object to track it
            myHandleMapping.addReference(handle);

            // Echo back the instance along with the type ID
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            bados.dataOut.writeInt (typeId);
            bados.dataOut.writeLong(handle);
            bados.dataOut.writeInt (-1);
            buildMessage(buf.dataOut,
                         MessageType.OBJECT_REFERENCE,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle a LOCK message.
         *
         * This is of the form:
         *  int32   : lock-name length
         *  bytes   : lock-name as ASCII bytes
         *
         * Gives back empty ACK.
         */
        private void handleLock(final long                      threadId,
                                final int                       reqId,
                                final ByteList                  payload,
                                final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() < 4) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }
            else {
                final int length = readInt(payload, 0);
                final CharSequence name = new HashableSubSequence(payload, 4, length);
                myLockManager.getExclusiveLockFor(name).lock();
            }

            // ACK comes back
            buildMessage(buf.dataOut, MessageType.EMPTY_ACK, threadId, reqId, null);
        }

        /**
         * Handle an UNLOCK message.
         *
         * This is of the form:
         *  int32   : lock-name length
         *  bytes   : lock-name as ASCII bytes
         *
         * Gives back empty ACK.
         */
        private void handleUnlock(final long                      threadId,
                                  final int                       reqId,
                                  final ByteList                  payload,
                                  final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() < 4) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }
            else {
                final int length = readInt(payload, 0);
                final CharSequence name = new HashableSubSequence(payload, 4, length);
                myLockManager.getExclusiveLockFor(name).unlock();
            }

            // ACK comes back
            buildMessage(buf.dataOut,
                         MessageType.EMPTY_ACK,
                         threadId,
                         reqId,
                         null);
        }

        /**
         * Handle an INJECT_CLASS message.
         *
         * Gives back by deferring to writeTypeDesc().
         */
        private void handleInjectClass(final long                      threadId,
                                       final int                       reqId,
                                       final ByteList                  payload,
                                       final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() == 0) {
                throw new IllegalArgumentException("Got an empty payload");
            }

            // Disallow class injection?
            if (!isClassInjectionPermitted()) {
                throw new SecurityException("Class injection not permitted");
            }

            // Inject it
            final Class<?> klass =
                myClassInjector.inject(payload.toArray(), payload.size());

            // And give it back
            writeTypeDesc(myTypeMapping.getDescription(klass),
                          threadId,
                          reqId,
                          buf);
        }

        /**
         * Handle an INJECT_SOURCE message.
         *
         * This is of the form:
         *  int32   : Class name length
         *  bytes[] : Class name
         *  int32   : Source code length
         *  bytes[] : Source code
         *
         * Gives back by deferring to writeTypeDesc().
         */
        private void handleInjectSource(final long                      threadId,
                                        final int                       reqId,
                                        final ByteList                  payload,
                                        final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() == 0) {
                throw new IllegalArgumentException("Got an empty payload");
            }

            // Disallow source injection? We don't allow this if we don't allow
            // Java class injection; they are basically the same thing when it
            // comes to security.
            if (!isClassInjectionPermitted()) {
                throw new SecurityException("Source injection not permitted");
            }

            // Our position in the payload data
            int offset = 0;

            // Get the class name
            final int lenClassName = readInt(payload, offset);
            offset += Integer.BYTES;
            final StringBuilder sbClassName = new StringBuilder();
            while (sbClassName.length() < lenClassName) {
                sbClassName.append((char)payload.get(offset++));
            }
            final String className = sbClassName.toString();

            // Get the source code
            final int lenSource = readInt(payload, offset);
            offset += Integer.BYTES;
            final StringBuilder sbSource = new StringBuilder();
            while (sbSource.length() < lenSource) {
                sbSource.append((char)payload.get(offset++));
            }
            final String source = sbSource.toString();

            // Compile the class and make it available to the JVM
            final Class<?> klass = mySourceInjector.inject(className, source);

            // And send it back
            writeTypeDesc(myTypeMapping.getDescription(klass),
                          threadId,
                          reqId,
                          buf);
        }

        /**
         * Handle a GET_VALUE_OF message.
         *
         * This is of the form:
         *  int64   : Object handle
         *  byte    : PythonValueFormat
         *
         * Gives back:
         *  int32    : Number of bytes
         *  bytes[]  : The pickled form of the object, or each component of a
         *             {@link JniPJRmi$ArrayHandle}, depending on the specified
         *             {@link PythonValueFormat}.
         */
        private void handleGetValueOf(final long                      threadId,
                                      final int                       reqId,
                                      final ByteList                  payload,
                                      final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() != 9) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // The object handle
            final long handle = readLong(payload, 0);

            // The return format. Make sure it's kosher.
            final PythonValueFormat valueFormat =
                PythonValueFormat.byId(payload.get(8));
            if (valueFormat == null) {
                throw new IllegalArgumentException(
                    "Unknown return format #" + payload.get(8)
                );

            }
            switch (valueFormat) {
            case RAW_PICKLE:
            case SNAPPY_PICKLE:
            case BESTEFFORT_PICKLE:
            case BESTEFFORT_SNAPPY_PICKLE:
            case SHMDATA:
                // These are handled below
                break;

            default:
                // Anything else isn't
                throw new IllegalArgumentException(
                  "Unhandled return format `" + valueFormat + "`; " +
                  "only Python pickle and SHM-data formats are supported."
                );
            }

            // Get the object for this handle, allow it to be the null pointer
            final Object object = myHandleMapping.getObject(handle);
            if (object == null && handle != HandleMapping.NULL_HANDLE) {
                throw new IllegalArgumentException(
                    "No object for handle " + handle
                );
            }

            // Send result based on return type
            if (valueFormat == PythonValueFormat.RAW_PICKLE        ||
                valueFormat == PythonValueFormat.SNAPPY_PICKLE     ||
                valueFormat == PythonValueFormat.BESTEFFORT_PICKLE ||
                valueFormat == PythonValueFormat.BESTEFFORT_SNAPPY_PICKLE)
            {
                // Convert the result to a payload byte stream
                renderObject(threadId, reqId, buf, valueFormat, object, null);
            }
            else if (valueFormat == PythonValueFormat.SHMDATA) {
                // Here's where we'll store the information from the write
                final JniPJRmi.ArrayHandle arrayInfo;

                // Determine the type of the array and write it natively
                if (object instanceof boolean[]) {
                    arrayInfo = JniPJRmi.writeArray((boolean[])object);
                }
                else if (object instanceof byte[]) {
                    arrayInfo = JniPJRmi.writeArray((byte[])object);
                }
                else if (object instanceof short[]) {
                    arrayInfo = JniPJRmi.writeArray((short[])object);
                }
                else if (object instanceof int[]) {
                    arrayInfo = JniPJRmi.writeArray((int[])object);
                }
                else if (object instanceof long[]) {
                    arrayInfo = JniPJRmi.writeArray((long[])object);
                }
                else if (object instanceof float[]) {
                    arrayInfo = JniPJRmi.writeArray((float[])object);
                }
                else if (object instanceof double[]) {
                    arrayInfo = JniPJRmi.writeArray((double[])object);
                }
                else {
                    throw new IllegalArgumentException(
                      "Unhandled array type for object: " +
                      object.getClass() +
                      "`. Only primitive, non-char arrays are supported."
                    );
                }

                // Write the resulting information
                writeShmObject(threadId, reqId, buf, arrayInfo.filename,
                                                     arrayInfo.numElems,
                                                     arrayInfo.type);
            }
            else {
                throw new IllegalStateException("Someone can't code");
            }
        }

        /**
         * Handle a GET_CALLBACK_HANDLE message.
         *
         * This is of the form:
         *  int32   : Python function ID
         *  int32   : Java Type ID, or -1
         *  byte    : Argument count
         *
         * Gives back:
         *  int32    : Function type ID
         *  bytes[]  : The function type
         */
        private void handleGetCallbackHandle(final long                      threadId,
                                             final int                       reqId,
                                             final ByteList                  payload,
                                             final ByteArrayDataOutputStream buf,
                                             final DataOutputStream          out)
            throws Throwable
        {
            if (payload.size() != 9) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // The object handle
            final int functionId = readInt(payload, 0);
            final int typeId     = readInt(payload, 4);
            final int numArgs    = payload.get(8) & 0xff; // '&' up-casts to int

            // Create the appropriate function wrapper
            final Object callback;
            TypeDescription typeDesc = myTypeMapping.getDescription(typeId);
            if (typeDesc == null) {
                // Need to guess a function
                switch (numArgs) {
                case 1:
                    final PythonCallbackFunction<?,?> f =
                        new PythonCallbackFunction<>(functionId, out);
                    callback = f;
                    typeDesc = f.getType();
                    break;

                case 2:
                    final PythonCallbackBiFunction<?,?,?> bf =
                        new PythonCallbackBiFunction<>(functionId, out);
                    callback = bf;
                    typeDesc = bf.getType();
                    break;

                default:
                    final PythonCallback<TypeDescription> c =
                        new PythonCallback<TypeDescription>(functionId, out)
                        {
                            @Override public TypeDescription getType()
                            {
                                return myTypeMapping.getDescription(
                                    PythonKwargsFunction.class
                                );
                            }
                        };
                    callback = c;
                    typeDesc = c.getType();
                    break;
                }
            }
            else {
                // We should be able to create it directly since we know the
                // type of the class were are aiming to invoke for.
                final Class<?> klass = typeDesc.getRepresentedClass();
                if (Runnable.class.equals(klass)) {
                    callback = new PythonCallbackRunnable(functionId, out);
                }
                else if (Function.class.equals(klass)) {
                    callback = new PythonCallbackFunction<>(functionId, out);
                }
                else if (BiFunction.class.equals(klass)) {
                    callback = new PythonCallbackBiFunction<>(functionId, out);
                }
                else if (Consumer.class.equals(klass)) {
                    callback = new PythonCallbackConsumer<>(functionId, out);
                }
                else if (BiConsumer.class.equals(klass)) {
                    callback = new PythonCallbackBiConsumer<>(functionId, out);
                }
                else if (Predicate.class.equals(klass)) {
                    callback = new PythonCallbackPredicate<>(functionId, out);
                }
                else if (BiPredicate.class.equals(klass)) {
                    callback = new PythonCallbackBiPredicate<>(functionId, out);
                }
                else if (UnaryOperator.class.equals(klass)) {
                    callback = new PythonCallbackUnaryOperator<>(functionId, out);
                }
                else if (PythonFunction      .class.equals(klass) ||
                         PythonKwargsFunction.class.equals(klass))
                {
                    callback =
                        new PythonCallback<TypeDescription>(functionId, out)
                        {
                            @Override public TypeDescription getType()
                            {
                                return myTypeMapping.getDescription(klass);
                            }
                        };
                }
                else {
                    // Okay, we are going to try to inspect the class to see if
                    // it has a single non-default, non-static, method which
                    // looks like it might match the one we have been given.
                    // This is determined by looking at the number of args since
                    // we can't use anything else (the arg types are unknown and
                    // the name is irrelevant for lambdas). This is roughly
                    // similar to what Java does with allowing users to supply
                    // lambdas for factory classes etc.
                    final List<MethodDescription> methods = new ArrayList<>();
                    for (int i=0; i < typeDesc.getNumMethods(); i++) {
                        // Determine if the method is magically handled by
                        // PythonLambdaHandler or not. Ideally this would be a
                        // static method of the class but it's not a static
                        // class and the current Java version doesn't allow
                        // static methods in instance classes so...
                        final MethodDescription method = typeDesc.getMethod(i);
                        switch (method.getName()) {
                        case "getClass":
                        case "equals":
                        case "hashCode":
                        case "notify":
                        case "notifyAll":
                        case "toString":
                        case "wait":
                            // Magically handled
                            break;

                        default:
                            // Any non-default, non-static method is not handled
                            if (!method.isDefault() && !method.isStatic()) {
                                methods.add(method);
                            }
                        }
                    }

                    // Now, see if we got a single method with the right number
                    // of arguments
                    if (methods.size() == 1 &&
                        methods.get(0).getNumArguments() == numArgs)
                    {
                        // Okay, we got a match, create a proxy which wraps just
                        // that method alone
                        callback =
                            Proxy.newProxyInstance(
                                klass.getClassLoader(),
                                new Class<?>[] { klass },
                                new PythonLambdaHandler(
                                    methods.get(0).getName(),
                                    functionId,
                                    out
                                )
                            );
                    }
                    else {
                        // Failed, say why
                        throw new UnsupportedOperationException(
                            "Can't create a callback wrapper " +
                            "for Python function with " + numArgs + " " +
                            "argument" + (numArgs == 1 ? "" : "s") + " " +
                            "for " + klass + " " +
                            "with methods: " + methods
                        );
                    }
                }
            }

            // Convert the callback, whatever it was, to a payload byte stream
            // and send it
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            writeObject(bados.dataOut, callback, typeDesc);
            buildMessage(buf.dataOut,
                         MessageType.ARBITRARY_ITEM,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Handle a CALLBACK_RESPONSE message.
         *
         * This is of the form:
         *  int32   : Java request ID
         *  ...     : Arbitrary item
         *
         * Gives back:
         *  Nothing(!)
         */
        private void handleCallbackResponse(final long                      threadId,
                                            final int                       reqId,
                                            final ByteList                  payload,
                                            final ByteArrayDataOutputStream buf)
            throws Throwable
        {
            if (payload.size() < 5) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // The Java request ID
            final int     requestId   = readInt    (payload, 0);
            final boolean isException = readBoolean(payload, 4);
            final Object  result      = readObject (payload, 5).object;

            // Give them to the appropriate callback listener, if we can find it
            final PythonCallbackResult cbr =
                myPythonCallbackResults.remove(requestId);
            if (cbr != null) {
                cbr.setResult(isException, result);
            }
            else {
                LOG.warning(
                    "No callback response found for Java request ID " + requestId
                );
            }
        }

        /**
         * Handle a GET_PROXY message.
         *
         * This is of the form:
         *  int32   : Python object ID
         *  int32   : Java Type ID
         *
         * Gives back:
         *  ...     : Arbitrary item (Proxy instance)
         */
        private void handleGetProxy(final long                      threadId,
                                    final int                       reqId,
                                    final ByteList                  payload,
                                    final ByteArrayDataOutputStream buf,
                                    final DataOutputStream          out)
            throws Throwable
        {
            if (payload.size() != 8) {
                throw new IllegalArgumentException(
                    "Got a malformed payload: " + PJRmi.toString(payload)
                );
            }

            // The object handle
            final int objectId = readInt(payload, 0);
            final int typeId   = readInt(payload, 4);

            // Grab the type
            final TypeDescription typeDesc = myTypeMapping.getDescription(typeId);
            if (typeDesc == null) {
                throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }
            final Class<?> klass = typeDesc.getRepresentedClass();

            // We can create the proxy now
            final Object proxy =
                Proxy.newProxyInstance(
                    klass.getClassLoader(),
                    new Class<?>[] { klass },
                    new PythonProxyHandler(objectId, out)
                );

            // And send it back
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            writeObject(bados.dataOut, proxy, typeDesc);
            buildMessage(buf.dataOut,
                         MessageType.ARBITRARY_ITEM,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /**
         * Write a type description message into the given buffer.
         *
         * Gives back:
         *  int32    : Type ID, or -1 then nothing else if the type was not found
         *  int32    : Name length
         *  byte[]   : Name
         *  int32    : TypeFlags as int
         *  int32    : The type ID of the array element type, or -1 if not an array
         *  int32    : Number of super-type IDs
         *  types[]  :
         *    int32    : Type ID
         *  int32    : Number of fields
         *  Field[]  :
         *    int32    : Field name length
         *    byte[]   : Field name
         *    int32    : Type ID
         *  int32    : Number of constructors
         *  CTOR[]   :
         *    int16    : Flags
         *    int16    : Number of arguments
         *    Param[]  :
         *      int32    : Argument type ID
         *      int32    : Parameter name length
         *      byte[]   : Parameter name
         *    int16    : Number of accepted kwargs
         *    Kwarg[]  :
         *      int32    : Kwarg name length
         *      byte[]   : Kwarg name
         *    byte[]   : Relative specificities
         *  int32    : Number of methods
         *  Method[] :
         *    int32    : Method name length
         *    byte[]   : Method name
         *    int16    : Flags
         *    int32    : Return type ID
         *    int16    : Number of arguments
         *    Param[]  :
         *      int32    : Argument type ID
         *      int32    : Parameter name length
         *      byte[]   : Parameter name
         *    int16    : Number of accepted kwargs
         *    Kwarg[]  :
         *      int32    : Kwarg name length
         *      byte[]   : Kwarg name
         *    byte[]   : Relative specificities
         */
        private void writeTypeDesc(final TypeDescription           desc,
                                   final long                      threadId,
                                   final int                       reqId,
                                   final ByteArrayDataOutputStream buf)
            throws IOException
        {
            // Look up the description
            final ByteArrayDataOutputStream bados = ourByteOutBuffer.get();
            if (desc == null) {
                bados.dataOut.writeInt(-1);
            }
            else {
                bados.dataOut.writeInt(desc.getTypeId());

                final String typeName = desc.getName();
                bados.dataOut.writeInt  (typeName.length());
                bados.dataOut.writeBytes(typeName);
                bados.dataOut.writeInt  (desc.getFlagsValue());

                final Class<?> elementType = desc.getArrayComponentType();
                if (elementType == null) {
                    bados.dataOut.writeInt(-1);
                }
                else {
                    bados.dataOut.writeInt(
                        myTypeMapping.getDescription(elementType).getTypeId()
                    );
                }

                bados.dataOut.writeInt(desc.getNumSupertypes());
                for (int i=0; i < desc.getNumSupertypes(); i++) {
                    bados.dataOut.writeInt(desc.getSupertypeId(i));
                }

                bados.dataOut.writeInt(desc.getNumFields());
                for (int i=0; i < desc.getNumFields(); i++) {
                    final FieldDescription field = desc.getField(i);
                    final String fieldName = field.getName();
                    bados.dataOut.writeInt    (fieldName.length());
                    bados.dataOut.writeBytes  (fieldName);
                    bados.dataOut.writeInt    (field.getType().getTypeId());
                    bados.dataOut.writeBoolean(field.isStatic());
                }

                bados.dataOut.writeInt(desc.getNumConstructors());
                for (int i=0; i < desc.getNumConstructors(); i++) {
                    final MethodDescription ctor = desc.getConstructor(i);

                    bados.dataOut.writeShort(ctor.getFlags());

                    bados.dataOut.writeShort(ctor.getNumArguments());
                    for (short j=0; j < ctor.getNumArguments(); j++) {
                        final String parameterName = ctor.getParameterName(j);
                        bados.dataOut.writeInt(ctor.getArgument(j).getTypeId());
                        bados.dataOut.writeInt(parameterName.length());
                        bados.dataOut.writeBytes(parameterName);
                    }

                    bados.dataOut.writeShort(ctor.getNumAcceptedKwargs());
                    for (short j=0; j < ctor.getNumAcceptedKwargs(); j++) {
                        final String kwargName = ctor.getAcceptedKwargName(j);
                        bados.dataOut.writeInt(kwargName.length());
                        bados.dataOut.writeBytes(kwargName);
                    }

                    bados.dataOut.writeInt(desc.getNumConstructors());
                    for (int j=0; j < desc.getNumConstructors(); j++) {
                        bados.dataOut.write(
                            desc.getRelativeConstructorSpecificity(i, j)
                        );
                    }
                }

                bados.dataOut.writeInt(desc.getNumMethods());
                for (int i=0; i < desc.getNumMethods(); i++) {
                    final MethodDescription method = desc.getMethod(i);

                    final String methodName = method.getName();
                    bados.dataOut.writeInt  (methodName.length());
                    bados.dataOut.writeBytes(methodName);

                    bados.dataOut.writeShort(method.getFlags());

                    bados.dataOut.writeInt(method.getReturnType().getTypeId());

                    bados.dataOut.writeShort(method.getNumArguments());
                    for (short j=0; j < method.getNumArguments(); j++) {
                        final String parameterName = method.getParameterName(j);
                        bados.dataOut.writeInt(method.getArgument(j).getTypeId());
                        bados.dataOut.writeInt(parameterName.length());
                        bados.dataOut.writeBytes(parameterName);
                    }

                    bados.dataOut.writeShort(method.getNumAcceptedKwargs());
                    for (short j=0; j < method.getNumAcceptedKwargs(); j++) {
                        final String kwargName = method.getAcceptedKwargName(j);
                        bados.dataOut.writeInt(kwargName.length());
                        bados.dataOut.writeBytes(kwargName);
                    }

                    bados.dataOut.writeInt(desc.getNumMethods());
                    for (int j=0; j < desc.getNumMethods(); j++) {
                        bados.dataOut.write(
                            desc.getRelativeMethodSpecificity(i, j)
                        );
                    }
                }
            }
            buildMessage(buf.dataOut,
                         MessageType.TYPE_DESCRIPTION,
                         threadId,
                         reqId,
                         bados.bytes);
        }

        /**
         * Read a boolean from a ByteList.
         */
        private boolean readBoolean(ByteList bytes, int offset)
        {
            return (bytes.get(offset) != 0);
        }

        /**
         * Read a short from a ByteList.
         */
        private short readShort(ByteList bytes, int offset)
        {
            return (short) ((Byte.toUnsignedInt(bytes.get(offset++)) << 8) |
                            (Byte.toUnsignedInt(bytes.get(offset++))     ));
        }

        /**
         * Read an integer from a ByteList.
         */
        private int readInt(ByteList bytes, int offset)
        {
            return (Byte.toUnsignedInt(bytes.get(offset++)) << 24) |
                   (Byte.toUnsignedInt(bytes.get(offset++)) << 16) |
                   (Byte.toUnsignedInt(bytes.get(offset++)) <<  8) |
                   (Byte.toUnsignedInt(bytes.get(offset++))      );
        }

        /**
         * Read a long from a ByteList.
         */
        private long readLong(ByteList bytes, int offset)
        {
            return (Byte.toUnsignedLong(bytes.get(offset++)) << 56) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) << 48) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) << 40) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) << 32) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) << 24) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) << 16) |
                   (Byte.toUnsignedLong(bytes.get(offset++)) <<  8) |
                   (Byte.toUnsignedLong(bytes.get(offset++))      );
        }

        /**
         * Await the response from a Python callback.
         */
        private <T> T awaitCallbackReponse(final PythonCallbackResult result)
            throws PythonCallbackException
        {
            // Loop until we get it back
            while (true) {
                // We won't get an answer back from a closed transport
                if (myTransport.isClosed()) {
                    throw new PythonCallbackException(
                        "Connection to Python is closed",
                        new IOException(
                            "Connection to Python is closed"
                        )
                    );
                }

                // Is the result ready?
                if (result.isReady()) {
                    final Object pythonResult = result.getResult();
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(
                            "Result was " +
                            pythonResult + " " +
                            (pythonResult == null ? "" : pythonResult.getClass())
                        );
                    }

                    // Either throw or return, depending
                    if (result.isException()) {
                        final Throwable t = (Throwable)pythonResult;
                        throw new PythonCallbackException(t.getMessage(), t);
                    }
                    else {
                        @SuppressWarnings("unchecked")
                        final T pr = (T)pythonResult;
                        return pr;
                    }
                }
                else {
                    // Busy-ish wait
                    LockSupport.parkNanos(1000);
                }
            }
        }

        /**
         * Return a proxy interface, targeting the given interface class, by
         * using the given method.
         */
        public Object getProxyForMethod(final Class<?> iface,
                                        final TypeDescription typeDesc,
                                        final MethodDescription methodDesc,
                                        final boolean isConstructor,
                                        final Object instance)
        {
            // Make sure everything is in order
            Objects.requireNonNull(iface);
            Objects.requireNonNull(typeDesc);
            Objects.requireNonNull(methodDesc);

            // We will build a proxy for the given interface class. For this we
            // will need a handler.
            final InvocationHandler handler =
                new InvocationHandler() {
                    public Object invoke(final Object proxy,
                                         final Method method,
                                         final Object[] args)
                        throws Throwable
                    {
                        // Determine the arguments which we are passing along
                        final Object   callInst;
                        final Object[] callArgs;
                        if (methodDesc.isStatic() || isConstructor) {
                            // This is a static method. Hence we will always
                            // pass in null as the first argument to its
                            // invocation and the call args are just passed
                            // through.
                            callInst = null;
                            callArgs = args;
                        }
                        else if (instance == null) {
                            // This is an instance method the method which we
                            // have was not bound to any particular instance.
                            // The first argument is thus the instance. E.g.
                            // map(values, Integer::toString).
                            if (args == null || args.length == 0) {
                                throw new IllegalArgumentException(
                                    "Given no arguments to instance method call " +
                                    method.getName() + " but need an instance"
                                );
                            }
                            callInst = args[0];
                            callArgs =
                                (args.length == 1)
                                ? EMPTY_OBJECTS // Avoid creating empty arrays
                                : Arrays.copyOfRange(args, 1, args.length);
                        }
                        else {
                            // We have an instance so use that, and just pass
                            // through the arguments. We could roll this into
                            // the first if block but (I assert without proof
                            // that) it's clearer this way.
                            callInst = instance;
                            callArgs = args;
                        }

                        // Do a bit of brief checking to make sure that the
                        // method being called looks vaguely compatible with the
                        // one which we're about to invoke. We'll catch any true
                        // problems when we actually come to invoke the method.
                        //
                        // First, check that we are not trying to get a value
                        // out of a function which has a void return type.
                        final Class<?> returnType =
                            methodDesc.getReturnType().getRepresentedClass();
                        if (!Void.TYPE.equals(method.getReturnType()) &&
                            Void.TYPE.equals(returnType))

                        {
                            throw new InvocationTargetException(
                                new IllegalArgumentException(
                                    "Functional method, " + method + ", " +
                                    "expects to return a " +
                                    method.getReturnType() + " " +
                                    "but target method, " + methodDesc + ", " +
                                    "is void"
                                )
                            );
                        }
                        // Second, check that the number of arguments looks okay
                        final int numCallArgs =
                            (callArgs == null) ? 0 : callArgs.length;
                        if (numCallArgs != methodDesc.getNumArguments()) {
                            throw new InvocationTargetException(
                                new IllegalArgumentException(
                                    "Functional method, " + method + ", " +
                                    "was given " + numCallArgs + " " +
                                    "argument" + (numCallArgs == 1 ? " " : "s ") +
                                    "but target method, " + methodDesc + ", " +
                                    "takes " + methodDesc.getNumArguments()
                                )
                            );
                        }

                        // Now attempt to invoke it
                        try {
                            return isConstructor
                                ? typeDesc.callConstructor(methodDesc.getIndex(),
                                                           callArgs)
                                : typeDesc.callMethod     (methodDesc.getIndex(),
                                                           callInst,
                                                           callArgs);
                        }
                        catch (Throwable t) {
                            // Give a decent message back
                            final String inst;
                            if (callInst == null) {
                                inst = "";
                            }
                            else {
                                inst = "on " + callInst + " " +
                                       "<" + callInst.getClass() + "> ";
                            }

                            // And throw
                            throw new InvocationTargetException(
                                t,
                                "Failed to call " +
                                (methodDesc.isStatic() ? "static " : "") +
                                "method " +
                                typeDesc.getName() + "::" +
                                methodDesc.getName() + " " + inst +
                                "with arguments " +
                                Arrays.toString(callArgs) + " " +
                                "via " +
                                iface.getSimpleName() + "::" + method.getName()
                            );
                        }
                    }
                };

            // Now it's easy to make the proxy class
            return Proxy.newProxyInstance(iface.getClassLoader(),
                                          new Class<?>[] { iface },
                                          handler);
        }
    }

    // ---------------------------------------------------------------------- //

    /**
     * Render a ByteList as a string, converting non-ASCII chars to sensible
     * printed values.
     */
    private static String toString(final ByteList bytes)
    {
        StringBuilder sb = ourByteListStringBuilder.get();

        sb.append('<');
        for (int i=0; i < bytes.size(); i++) {
            final byte b = bytes.getNoCheck(i);
            if (b < (byte)' ' || b > (byte)'~') {
                sb.append("\\x");
                appendHexByte(sb, b);
            }
            else if (b == '\\') {
                // Since we use '\' as an escape char we also need to escape it.
                // That makes cutting and pasting strings into python easier.
                sb.append("\\\\");
            }
            else {
                sb.append((char)b);
            }
        }
        sb.append('>');

        return sb.toString();
    }
    private static final ThreadLocalStringBuilder ourByteListStringBuilder =
        new ThreadLocalStringBuilder(1024);

    /**
     * Render a byte[] as a string, converting non-ASCII chars to readable
     * printed values.
     */
    private static String toString(final byte[] bytes)
    {
        StringBuilder sb = ourByteArrayStringBuilder.get();

        sb.append('<');
        for (byte b : bytes) {
            if (b < (byte)' ' || b > (byte)'~') {
                sb.append("\\x");
                appendHexByte(sb, b);
            }
            else {
                sb.append((char)b);
            }
        }
        sb.append('>');

        return sb.toString();
    }
    private static final ThreadLocalStringBuilder ourByteArrayStringBuilder =
        new ThreadLocalStringBuilder(1024);

    /**
     * Send a UTF16 string over the wire.
     *
     * <p>This method is needed because the DataOutputStream writeUTF() method
     * doesn't handle Strings longer than 64k in size(!) and is also a tad
     * inefficient and garbagy.
     *
     * <p>The format of data is an int32 denoting the length, followed by the
     * UTF16 bytes representing the chars.
     *
     * <p>See http://www.ietf.org/rfc/rfc2781.txt for more information
     */
    private static void writeUTF16(DataOutputStream out, String string)
        throws IOException
    {
        // We need to know the length of the string we are about to write out so
        // as to make sure that our buffer will be big enough.
        final int len = string.length();

        // The buffer must be big enough for a pathological UTF string, with 4
        // bytes at the front for the length
        final byte[] buffer = getByteArray(len * 4 + 4);

        // Pointer into the buffer for stuffing in the string's chars. This
        // starts 4 bytes in so as to leave room for the size at the front.
        int index = 4;

        // First we insert the byte-order mark so that the other side knows how
        // to decode the data. See section 3.2 of the RFC.
        buffer[index++] = (byte)0xfe;
        buffer[index++] = (byte)0xff;

        // Copy the data into our buffer
        for (int i=0; i < len; i++) {
            // How we handle the various UTF16 encodings. See section 2.1 in RFC2781:
            //   https://www.ietf.org/rfc/rfc2781.txt
            final char u = string.charAt(i);
            if (u < 0x10000) {
                buffer[index++] = (byte)((u >> 8) & 0xff);
                buffer[index++] = (byte)((u     ) & 0xff);
            }
            else {
                final int uu = (int)u - 0x10000;
                final int w1 = 0xd800 | ((uu >> 10) & 0x3ff);
                final int w2 = 0xdc00 | ((uu      ) & 0x3ff);
                buffer[index++] = (byte)((w1 >> 8) & 0xff);
                buffer[index++] = (byte)((w1     ) & 0xff);
                buffer[index++] = (byte)((w2 >> 8) & 0xff);
                buffer[index++] = (byte)((w2     ) & 0xff);
            }
        }

        // Now we know the number of bytes which make up the string
        // representation. Write this into the start of the buffer (network
        // byte-order).
        final int sz = (index - 4);
        buffer[0] = (byte)((sz >>> 24) & 0xff);
        buffer[1] = (byte)((sz >>> 16) & 0xff);
        buffer[2] = (byte)((sz >>>  8) & 0xff);
        buffer[3] = (byte)((sz       ) & 0xff);

        // And, finally, we can send it out over the wire
        out.write(buffer, 0, index);
    }

    /**
     * Get a thread-local byte[] of at least the given size.
     */
    private static byte[] getByteArray(final int size)
    {
        byte[] buffer = ourThreadLocalByteBuffer.get();
        if (buffer.length < size) {
            // Need a bigger buffer, with 10% overhead to prevent malloc
            // thrashing over time
            buffer = new byte[(int)(size * 1.1)];
            ourThreadLocalByteBuffer.set(buffer);
        }
        return buffer;
    }
    private static final ThreadLocal<byte[]> ourThreadLocalByteBuffer =
        new ThreadLocal<byte[]>()
        {
            @Override
            protected byte[] initialValue()
            {
                // 640k^H^H^H^H1Mb should be enough for anyone
                return new byte[1024 * 1024];
            }
        };

    /**
     * Sort the given Methods into a reasonable ordering. This is the
     * entry-point to the actual methodSort().
     *
     * <p>See the JavaDoc of MethodComparator for caveats.
     */
    private static void methodSort(final Method[] methods)
    {
        // Duplicate the input and sort _that duplicate_ back into the original
        // input. Hand off the recursive call directly.
        final int length = methods.length;
        methodSort(Arrays.copyOf(methods, length), methods, 0, length);
    }

    /**
     * The recursive step of methodSort(), which is a form of merge-sort.
     *
     * <p>Do not call this directly.
     */
    private static void methodSort(final Method[] src,
                                   final Method[] dst,
                                   final int      low,
                                   final int      high)
    {
        final int length = high - low;

        // If we have fewer than two elements then we're trivially done
        if (length < 2) {
            return;
        }

        // The recursion step
        final int mid = (low + high) >>> 1;
        methodSort(dst, src, low, mid);
        methodSort(dst, src, mid, high);

        // And the merge step
        for (int i = low, j = low, k = mid; i < high; i++) {
            if (k >= high ||
                j < mid && MethodComparator.INSTANCE.compare(src[j], src[k]) <= 0)
            {
                dst[i] = src[j++];
            }
            else {
                dst[i] = src[k++];
            }
        }
    }

    // ---------------------------------------------------------------------- //

    /**
     * The default collection of classes required for the basic PJRmi code to
     * function (i.e. this is what the Python code relies on). This can be used
     * as a basis for users wanting to create their own allowlists.
     *
     * <p>The idea is that the allowlist should not have classes in it which
     * would allow privilege escalation by a PJRmi client. (I.e. things which
     * let them run binaries, access files, etc.)
     *
     * <p><b>THINK VERY CAREFULLY BEFORE ADDING ANY CLASSES TO THIS LIST!</b>
     */
    private static final Collection<String> DEFAULT_CLASS_NAME_ALLOWLIST =
        Collections.unmodifiableList(
            Arrays.asList(
                // Primitive types
                "boolean",
                "byte",
                "char",
                "double",
                "float",
                "int",
                "long",
                "short",
                "void",

                // Arrays
                "[B",
                "[C",
                "[D",
                "[F",
                "[I",
                "[J",
                "[Ljava.lang.Object;",
                "[Ljava.lang.String;",
                "[S",
                "[Z",

                // Classes
                "com.deshaw.hypercube.CubeMath",
                "com.deshaw.hypercube.AbstractBooleanHypercube",
                "com.deshaw.hypercube.AbstractDoubleHypercube",
                "com.deshaw.hypercube.AbstractFloatHypercube",
                "com.deshaw.hypercube.AbstractHypercube",
                "com.deshaw.hypercube.AbstractIntegerHypercube",
                "com.deshaw.hypercube.AbstractLongHypercube",
                "com.deshaw.hypercube.AxisRolledHypercube",
                "com.deshaw.hypercube.Boolean1dWrappingHypercube",
                "com.deshaw.hypercube.Boolean2dWrappingHypercube",
                "com.deshaw.hypercube.Boolean3dWrappingHypercube",
                "com.deshaw.hypercube.Boolean4dWrappingHypercube",
                "com.deshaw.hypercube.Boolean5dWrappingHypercube",
                "com.deshaw.hypercube.BooleanAxisRolledHypercube",
                "com.deshaw.hypercube.BooleanBitSetHypercube",
                "com.deshaw.hypercube.BooleanFlatRolledHypercube",
                "com.deshaw.hypercube.BooleanHypercube",
                "com.deshaw.hypercube.BooleanSlicedHypercube",
                "com.deshaw.hypercube.BooleanWrappingHypercube",
                "com.deshaw.hypercube.Double1dWrappingHypercube",
                "com.deshaw.hypercube.Double2dWrappingHypercube",
                "com.deshaw.hypercube.Double3dWrappingHypercube",
                "com.deshaw.hypercube.Double4dWrappingHypercube",
                "com.deshaw.hypercube.Double5dWrappingHypercube",
                "com.deshaw.hypercube.DoubleArrayHypercube",
                "com.deshaw.hypercube.DoubleAxisRolledHypercube",
                "com.deshaw.hypercube.DoubleFlatRolledHypercube",
                "com.deshaw.hypercube.DoubleFromFloatHypercube",
                "com.deshaw.hypercube.DoubleFromIntegerHypercube",
                "com.deshaw.hypercube.DoubleFromLongHypercube",
                "com.deshaw.hypercube.DoubleHypercube",
                "com.deshaw.hypercube.DoubleMappedHypercube",
                "com.deshaw.hypercube.DoubleSlicedHypercube",
                "com.deshaw.hypercube.DoubleWrappingHypercube",
                "com.deshaw.hypercube.FlatRolledHypercube",
                "com.deshaw.hypercube.Float1dWrappingHypercube",
                "com.deshaw.hypercube.Float2dWrappingHypercube",
                "com.deshaw.hypercube.Float3dWrappingHypercube",
                "com.deshaw.hypercube.Float4dWrappingHypercube",
                "com.deshaw.hypercube.Float5dWrappingHypercube",
                "com.deshaw.hypercube.FloatArrayHypercube",
                "com.deshaw.hypercube.FloatAxisRolledHypercube",
                "com.deshaw.hypercube.FloatFlatRolledHypercube",
                "com.deshaw.hypercube.FloatFromDoubleHypercube",
                "com.deshaw.hypercube.FloatFromIntegerHypercube",
                "com.deshaw.hypercube.FloatFromLongHypercube",
                "com.deshaw.hypercube.FloatHypercube",
                "com.deshaw.hypercube.FloatMappedHypercube",
                "com.deshaw.hypercube.FloatSlicedHypercube",
                "com.deshaw.hypercube.FloatWrappingHypercube",
                "com.deshaw.hypercube.GenericArrayHypercube",
                "com.deshaw.hypercube.GenericHypercube",
                "com.deshaw.hypercube.GenericWrappingHypercube",
                "com.deshaw.hypercube.Hypercube",
                "com.deshaw.hypercube.Integer1dWrappingHypercube",
                "com.deshaw.hypercube.Integer2dWrappingHypercube",
                "com.deshaw.hypercube.Integer3dWrappingHypercube",
                "com.deshaw.hypercube.Integer4dWrappingHypercube",
                "com.deshaw.hypercube.Integer5dWrappingHypercube",
                "com.deshaw.hypercube.IntegerArrayHypercube",
                "com.deshaw.hypercube.IntegerAxisRolledHypercube",
                "com.deshaw.hypercube.IntegerFlatRolledHypercube",
                "com.deshaw.hypercube.IntegerFromDoubleHypercube",
                "com.deshaw.hypercube.IntegerFromFloatHypercube",
                "com.deshaw.hypercube.IntegerFromLongHypercube",
                "com.deshaw.hypercube.IntegerHypercube",
                "com.deshaw.hypercube.IntegerMappedHypercube",
                "com.deshaw.hypercube.IntegerSlicedHypercube",
                "com.deshaw.hypercube.IntegerWrappingHypercube",
                "com.deshaw.hypercube.Long1dWrappingHypercube",
                "com.deshaw.hypercube.Long2dWrappingHypercube",
                "com.deshaw.hypercube.Long3dWrappingHypercube",
                "com.deshaw.hypercube.Long4dWrappingHypercube",
                "com.deshaw.hypercube.Long5dWrappingHypercube",
                "com.deshaw.hypercube.LongArrayHypercube",
                "com.deshaw.hypercube.LongAxisRolledHypercube",
                "com.deshaw.hypercube.LongFlatRolledHypercube",
                "com.deshaw.hypercube.LongFromDoubleHypercube",
                "com.deshaw.hypercube.LongFromFloatHypercube",
                "com.deshaw.hypercube.LongFromIntegerHypercube",
                "com.deshaw.hypercube.LongHypercube",
                "com.deshaw.hypercube.LongMappedHypercube",
                "com.deshaw.hypercube.LongSlicedHypercube",
                "com.deshaw.hypercube.LongWrappingHypercube",
                "com.deshaw.hypercube.SlicedHypercube",
                "com.deshaw.hypercube.WrappingHypercube",

                "com.deshaw.pjrmi.JavaProxyBase",
                "com.deshaw.pjrmi.PythonFunction",
                "com.deshaw.pjrmi.PythonKwargsFunction",
                "com.deshaw.pjrmi.PythonObject",
                "com.deshaw.pjrmi.PythonSlice",
                "com.deshaw.util.StringUtil",

                "java.lang.AutoCloseable",
                "java.lang.Boolean",
                "java.lang.Byte",
                "java.lang.Character",
                // Not java.lang.Class since it allows access to any class in
                // the system, and the methods on it etc.
                "java.lang.ClassNotFoundException",
                "java.lang.Comparable",
                "java.lang.Double",
                "java.lang.Exception",
                "java.lang.Float",
                "java.lang.Integer",
                "java.lang.Iterable",
                "java.lang.Long",
                "java.lang.NoSuchFieldException",
                "java.lang.NoSuchMethodException",
                "java.lang.Number",
                "java.lang.Object",
                "java.lang.Short",
                "java.lang.String",

                "java.util.Collection",
                "java.util.Iterator",
                "java.util.List",
                "java.util.Map",
                "java.util.Map$Entry",
                "java.util.NoSuchElementException",
                "java.util.Set",
                "java.util.concurrent.Future",
                "java.util.function.BiFunction",
                "java.util.function.Function",
                "java.util.logging.Level",
                "java.util.logging.LogManager",

                "sun.misc.Signal"
            )
        );

    /**
     * Our default logger.
     */
    /*package*/ static final Logger LOG = Logger.getLogger("com.deshaw.pjrmi.PJRmi");

    /**
     * The handshake string, this should have the same value as the one in the
     * Python code. The major and minor version numbers should match the
     * {@code pjrmiVersion} values in {@code gradle.properties}. Typically the
     * minor version number should change whenever the wire format changes.
     */
    private static final String HELLO = "PJRMI_1.13";

    /**
     * The request ID to use for callbacks (which are unsolicited from Python's
     * point of view).
     */
    private static final int CALLBACK_REQUEST_ID = -1;

    /**
     * The methods in an Object.
     */
    private static final List<Method> OBJECT_METHODS =
        Collections.unmodifiableList(
            Arrays.asList(Object.class.getMethods())
        );

    /**
     * How much we're allowed to recurse from Java to Python to Java before we
     * decide it's too much. Note that each level of recursion is a new thread
     * so we can't make this <i>too</i> big.
     */
    private static final int MAX_CALL_DEPTH = 128;

    /**
     * The global XOR value to use then creating thread IDs. This mimics the
     * code on the Python side.
     */
    private final long THREAD_ID_XOR = new Random().nextLong() & 0x7fffffffffffffffL;

    /**
     * An empty {@code Object[]}, so we don't create pointless ones.
     */
    private static final Object[] EMPTY_OBJECTS = new Object[0];

    /**
     * Our specialisation of the MethodUtil class.
     */
    private static MethodUtil ourMethodUtil = new PythonicMethodUtil();

    /**
     * The byte buffers used to build up the output messages. Always returned
     * empty.
     */
    private static final ThreadLocal<ByteArrayDataOutputStream> ourByteOutBuffer =
        new ThreadLocalByteArrayDataOutputStream();

    /**
     * Our per-thread PythonPicklers, for converting values to pickle format.
     */
    private static final ThreadLocal<PythonPickle> ourPythonPickle =
        ThreadLocal.withInitial(PJRmiPythonPickle::new);

    // ---------------------------------------------------------------------- //

    /**
     * Where connections come in.
     */
    private final Transport.Provider myTransportProvider;

    /**
     * The name of this instance.
     */
    private final String myName;

    /**
     * How we index the connections.
     */
    private int myConnectionIndex;

    /**
     * A flag is used to notify the driver thread to stop its processing.
     */
    private final AtomicBoolean myIsClosed;

    /**
     * Our type mappings. These are shared by all connections.
     */
    private final TypeMapping myTypeMapping;

    /**
     * Our class injector.
     */
    private final ClassInjector myClassInjector;

    /**
     * Our source injector.
     */
    private final SourceInjector mySourceInjector;

    /**
     * Our lock manager instance.
     */
    private final PJRmiLockManager myLockManager;

    /**
     * The Instrumentors for this instance, keyed by MessageType ordinal.
     */
    private final Instrumentor[] myInstrumentors;

    /**
     * The instrumentor for the readObject() method.
     */
    private final Instrumentor myReadObjectInstrumentor;

    /**
     * The instrumentor for the writeObject() method.
     */
    private final Instrumentor myWriteObjectInstrumentor;

    /**
     * Whether to use SHM copying.
     */
    private final boolean myUseShmdata;

    /**
     * The collection of classes which the user is allowed to get using a
     * TYPE_REQUEST. If this is null then all classes are permitted.
     *
     * <p>The idea is that the allowlist should not have classes in it which
     * would allow privilege escalation by a PJRmi client. (I.e. things which
     * let them run binaries, access files, etc.)
     *
     * <p>By default this collection is used by the {@link #isClassPermitted(CharSequence)}
     * method in order to determine what's allowed. If that method is overridden
     * then this collection's purpose may become moot.
     *
     * <p>Likewise, if {@link isClassBlockingOn} is {@code false}, this
     * collection's purpose is moot.
     */
    private final Collection<String> myClassNameAllowlist;

    // ---------------------------------------------------------------------- //

    /**
     * CTOR.
     *
     * @param name     The name of this instance.
     * @param provider How we get connections from the outside world.
     *
     * @throws IOException if there was a problem.
     */
    public PJRmi(final String name, final Transport.Provider provider)
        throws IOException
    {
        this(name, provider, false);
    }

    /**
     * CTOR.
     *
     * @param name           The name of this instance.
     * @param provider       How we get connections from the outside world.
     * @param useGlobalLock  Whether to instantiate the global lock for all
     *                       method calls from Python etc.
     *
     * @throws IOException if there was a problem.
     */
    public PJRmi(final String             name,
                 final Transport.Provider provider,
                 final boolean            useGlobalLock)
        throws IOException
    {
        this(name, provider, useGlobalLock, null);
    }

    /**
     * CTOR.
     *
     * @param name           The name of this instance.
     * @param provider       How we get connections from the outside world.
     * @param useGlobalLock  Whether to instantiate the global lock for all
     *                       method calls from Python etc.
     * @param useShmdata     Whether to use SHM copying.
     *
     * @throws IOException if there was a problem.
     */
    public PJRmi(final String             name,
                 final Transport.Provider provider,
                 final boolean            useGlobalLock,
                 final boolean            useShmdata)
        throws IOException
    {
        this(name, provider, useGlobalLock, useShmdata, null);
    }

    /**
     * CTOR.
     *
     * @param name         The name of this instance.
     * @param provider     How we get connections from the outside world.
     * @param lockManager  The LockManager to be used for this PJRmi instance.
     *
     * @throws IOException if there was a problem.
     */
    public PJRmi(final String             name,
                 final Transport.Provider provider,
                 final PJRmiLockManager   lockManager)
        throws IOException
    {
        // See private constructor for semantics when providing
        // both a non-null lockManager and a value for useGlobalLock.
        this(name, provider, false, Objects.requireNonNull(lockManager));
    }

    /**
     * CTOR.
     *
     * SHM value passing disabled by default.
     *
     * @param name           The name of this instance.
     * @param provider       How we get connections from the outside world.
     * @param useGlobalLock  Whether to instantiate the global lock for all
     *                       method calls from Python etc.  Ignored if
     *                       lockManager is non-null (inherits the value
     *                       from the provided lockManager).
     * @param lockManager    The LockManager to use for this PJRmi instance
     *                       (null if a new lockManager should be created).
     *
     * @throws IOException if there was a problem.
     */
    private PJRmi(final String             name,
                  final Transport.Provider provider,
                  final boolean            useGlobalLock,
                  final PJRmiLockManager   lockManager)
        throws IOException
    {
        this(name, provider, useGlobalLock, false, lockManager);
    }

    /**
     * CTOR.
     *
     * Alternative PJRmi constructor to allow a lock manager
     * to be passed in so that this PJRmi instance can share a lock
     * with other instances.
     *
     * This constructor is private to hide a potentially confusing semantic
     * from users, in the case that this constructor is invoked with a
     * non-null lockManager with a value for useGlobalLock that is different
     * from the value for useGlobalLock passed into this method.  The
     * behavior would be undefined and hence is hidden so that these parameters
     * will be consistent.
     *
     * @param name           The name of this instance.
     * @param provider       How we get connections from the outside world.
     * @param useGlobalLock  Whether to instantiate the global lock for all
     *                       method calls from Python etc. Ignored if
     *                       lockManager is non-{@code null} (inherits the value
     *                       from the provided lockManager).
     * @param lockManager    The LockManager to use for this PJRmi instance
     *                       ({@code null} if a new lockManager should be
     *                       created).
     *
     * @throws IOException if there was a problem.
     */
    private PJRmi(final String             name,
                  final Transport.Provider provider,
                  final boolean            useGlobalLock,
                  final boolean            useShmdata,
                  final PJRmiLockManager   lockManager)
        throws IOException
    {
        // Set up our thread nicely
        super(name);
        setDaemon(true);

        // Very simply, create the connection and set it running; we make it
        // 'active' when start() is called.
        myTransportProvider = provider;
        myName = name + ":" + provider;
        myUseShmdata = useShmdata && JniPJRmi.isAvailable();
        myConnectionIndex = 0;
        if (lockManager == null) {
            myLockManager = new PJRmiLockManager(useGlobalLock);
        }
        else {
            myLockManager = lockManager;
        }
        myIsClosed = new AtomicBoolean(false);
        myTypeMapping = new TypeMapping();
        myClassInjector = new ClassInjector();
        mySourceInjector = new SourceInjector();

        // Keep track of how long things are taking etc.
        myReadObjectInstrumentor  = getInstrumentor(myName + ":readObject()");
        myWriteObjectInstrumentor = getInstrumentor(myName + ":writeObject()");
        final MessageType[] types = MessageType.values();
        myInstrumentors = new Instrumentor[types.length];
        for (MessageType type : types) {
            Instrumentor instr = getInstrumentor(myName + ":" + type);
            myInstrumentors[type.ordinal()] = instr;

            // Set the interval mods to reflect how much we expect things to be
            // called. For some things we expect a lot of calls, for some we
            // expect very few.
            switch (type) {
            case INSTANCE_REQUEST:
            case TYPE_REQUEST:
            case GET_CALLBACK_HANDLE:
                instr.setIntervalMod(1);
                break;

            case ARBITRARY_ITEM:
            case METHOD_CALL:
                instr.setIntervalMod(1000);
                break;

            default:
                instr.setIntervalMod(10);
                break;
            }
        }

        // Figure out the class-name allowlist
        Collection<String> allowlist = getClassNameAllowlist();
        if (allowlist != null) {
            allowlist = Collections.unmodifiableSet(new HashSet<>(allowlist));
        }
        myClassNameAllowlist = allowlist;
    }

    /**
     * Get a handle on the lock manager.
     *
     * @return the lock manager.
     */
    public PJRmiLockManager getLockManager()
    {
        return myLockManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run()
    {
        // Say we're up and running
        LOG.info(myName + " " +
                 "Listening for connections with " + myTransportProvider);

        // Keep accepting connections as they arrive and spawn a new handler
        // thread for each one
        while (!myIsClosed.get()) {
            try {
                awaitConnection();
            }
            catch (IOException|SecurityException e) {
                LOG.log(Level.INFO, "Failed to establish connection", e);

                // If the reason for the failure was that the provider has been
                // closed then we should terminate at this point
                if (myTransportProvider.isClosed()) {
                    LOG.log(Level.INFO, "Provider is closed so exiting");
                    return;
                }
            }
        }
    }

    /**
     * Get the instance by the given name.
     *
     * @param name  The name to look up for.
     *
     * @return the instance, if any.
     */
    protected abstract Object getObjectInstance(CharSequence name);

    /**
     * Whether the given <i>authenticated</i> username is permitted to access
     * this instance. If the user cannot be securely identified then the caller
     * should pass {@code null} to this method.
     *
     * @param username  The autheticated username, or {@code null} if it does
     *                  not exist.
     *
     * @return whether the user is permitted.
     */
    protected boolean isUserPermitted(CharSequence username)
    {
        // By default we allow the user who is running the process to access it
        return StringUtil.equals(System.getProperty("user.name"), username);
    }

    /**
     * Whether the host with the given internet address is permitted to access
     * this instance.
     *
     * @param address The address of the remote client. This may be {@code null}.
     *
     * @return whether the host is permitted.
     */
    protected boolean isHostPermitted(InetAddress address)
    {
        // By default we let all remote hosts connect
        return true;
    }

    /**
     * Whether class blocking is on, i.e. whether only a specific set of
     * classes is permitted or not. Subclasses can override this behavior.
     *
     * @return whether blocking is on.
     */
    protected boolean isClassBlockingOn()
    {
        // By default, class blocking is off and all classes are permitted.
        return false;
    }

    /**
     * Whether access to a class, given its canonical name, is permitted or not.
     *
     * <p>By default:<ul>
     *  <li>If {@link #isClassBlockingOn()} returns {@code false} then this
     *      method is effectively inactive and all classes are permitted.
     *  <li>If {@code true}, the result of {@link #getClassNameAllowlist()}
     *      will be used to decide whether a class is allowed or not, making
     *      this method "active".
     *  <li>Access to primitive types, and their arrays, is permitted.
     *  <li>Arrays of objects are allowed if their element's class is allowed.
     * </ul>
     * This default behaviour may not apply should the method be overridden.
     *
     * <p><b>IMPORTANT:</b> In all circumstances, if this method is "active"
     * then passing in {@code null} should return {@code false} and, if it is
     * not "active" then doing so should return {@code true}. Other logic
     * depends on these semantics.
     *
     * @param className  The name to check.
     *
     * @return Whether access to the class is allowed.
     *
     * @see getClassNameAllowlist
     */
    protected boolean isClassPermitted(final CharSequence className)
    {
        // By default, if class blocking is not on, then we allow everything
        if (!isClassBlockingOn()) {
            return true;
        }

        // Classes with a null name are not allowed. Who knows what they are?!
        if (className == null) {
            return false;
        }

        // For shredding the name
        final int len = className.length();

        // We allow all 1D and 2D primitive arrays since they don't allow
        // access to insecure methods
        if ((len == 2 && className.charAt(0) == '[') ||
            (len == 3 && className.charAt(0) == '[' &&
                         className.charAt(1) == '['))
        {
            switch (className.charAt(len - 1)) {
            case 'B':
            case 'C':
            case 'D':
            case 'F':
            case 'I':
            case 'J':
            case 'S':
            case 'Z':
                return true;
            }
        }

        //  Check the map for the actual class. We render to a String at this
        //  point since that's what the map is keyed with. Chances are that we
        //  are given a String anyhow so this will be a NOP.
        //
        // If the allow list is null, all classes are permitted.
        final String classNameStr = className.toString();
        if (myClassNameAllowlist == null ||
            myClassNameAllowlist.contains(classNameStr))
        {
            return true;
        }

        // All primitive types are allowed, including 'void'
        switch (classNameStr) {
            case "void":
            case "boolean":
            case "byte":
            case "char":
            case "double":
            case "float":
            case "int":
            case "long":
            case "short":
                return true;
        }

        // See if the className is an array. If it's an object array then we
        // only allow it if the underlying class is also allowed. We could just
        // allow arrays of anything, but subclasses can be more lax if they
        // really want to.
        if (len > 3 &&
            className.charAt(0)     == '[' &&
            className.charAt(1)     == 'L' &&
            className.charAt(len-1) == ';' &&
            isClassPermitted(className.subSequence(2, len-1)))
        {
            return true;
        }

        // Otherwise access to this class isn't allowed
        return false;
    }

    /**
     * Get the additional message to add to the {@code SecurityException} that
     * is thrown when a not-permitted class, as determined by
     * {@code isClassPermitted}, is accessed.
     *
     * @return the message.
     */
    protected String getClassNotPermittedMessage()
    {
        // No-op for the base class
        return "";
    }

    /**
     * Whether class injection is permitted in this instance.
     *
     * <p>By default this is not allowed if we have class permissioning enabled.
     * Subclasses should be very careful about overriding this method to return
     * {@code true} if this class's call returns {@code false}. (I.e. don't make
     * it more permissive.)
     *
     * @return whether injection is permitted.
     */
    protected boolean isClassInjectionPermitted()
    {
        // We defer to the semantics of isClassPermitted() for this. If we give
        // it null then it will return false if the function is active and true
        // otherwise. We should not allow class injection if we have
        // permissioning enabled since it could be used to subvert the security.
        return isClassPermitted(null);
    }

    /**
     * This method is called when a client connection is accepted. Subclasses
     * may wish to override this in order to add extra logging etc. Some or all
     * of the arguments to this function may be {@code null}.
     *
     * @param username       The username of the connection, if any.
     * @param address        The address of the client, if any.
     * @param transport      The transport used, if any.
     * @param clientCommand  The name of tha client script, if any.
     * @param pid            The PID of the client, if any.
     * @param id             The unique ID of the client, if any.
     */
    protected void connectionAccepted(final String      username,
                                      final InetAddress address,
                                      final Transport   transport,
                                      final String      clientCommand,
                                      final Integer     pid,
                                      final Long        id)
    {
        // Build the connection details
        final StringBuilder sb = new StringBuilder();
        sb.append(myName).append(' ').append("Allowed ");
        if (username != null) {
            sb.append("user \"").append(username).append("\"");
        }
        else {
            sb.append("anonymous/unauthenicated user");
        }
        if (address != null) {
            sb.append(" from ").append(address);
        }
        if (transport != null) {
            sb.append(" via ").append(transport);
        }
        if (clientCommand != null) {
            sb.append(" using client command ")
              .append("\"").append(clientCommand).append("\"");
        }
        if (pid != null) {
            sb.append(" with PID ").append(pid);
        }
        if (id != null) {
            sb.append(" with ID ").append(id);
        }

        // And log it all
        LOG.info(sb.toString());
    }

    /**
     * This method is called when a connection is refused for some reason.
     * Subclasses may wish to override this in order to add extra logging etc.
     * Some or all of the arguments to this function may be {@code null}.
     *
     * <p>Note that this method could be called repeatedly in quick succession
     * for a variety of reasons. Subclasses should be aware of this when
     * deciding how to alert.
     *
     * @param username   The username of the connection, if any.
     * @param address    The address of the client, if any.
     * @param transport  The transport used, if any.
     * @param reason     The reason the connection was refused, if any.
     */
    protected void connectionRejected(final String      username,
                                      final InetAddress address,
                                      final Transport   transport,
                                      final String      reason)
    {
        LOG.warning(
            "Rejected connection by user " +
            ((username  == null) ? "<unknown>" : username            ) + " " +
            "from host " +
            ((address   == null) ? "<unknown>" : address  .toString()) + " " +
            "via transport " +
            ((transport == null) ? "<unknown>" : transport.toString()) + " " +
            ((reason    == null) ? "" : ": " + reason)
        );
    }

    /**
     * Get the collection of classes which the user is allowed to get using a
     * TYPE_REQUEST. This returns a safe but conservative collection of classes.
     *
     * <p>This method will be called once, in the constructor, and its result
     * copied and saved.
     *
     * <p>If {@link isClassBlockingOn} is {@code false}, then the results of
     * this call are effectively ignored.
     *
     * <p><i>Note:</i> If this method returns null, then all classes are
     * permitted.
     *
     * @return the allow-list.
     */
    protected Collection<String> getClassNameAllowlist()
    {
        return DEFAULT_CLASS_NAME_ALLOWLIST;
    }

    /**
     * How many worker threads this PJRmi instance should use to field incoming
     * requests.
     *
     * <p>If this value is greater than zero then worker threads will be used;
     * else all the calls will be handled in the receiver thread. using workers
     * slower but allows for re-entrancy (i.e. Python calls Java calls Python
     * calls Java). This value determines how many worker threads to actively
     * maintain; it's possible that more threads than this will be created, if
     * required, but they will be leaked.
     *
     * <p>By default this returns {@code 0}, implying single-threaded access.
     *
     * @return the number of workers.
     */
    protected int numWorkers()
    {
        return 0;
    }

    /**
     * Whether we're using workers or not.
     *
     * @return whether to use workers.
     */
    protected final boolean useWorkers()
    {
        return (numWorkers() > 0);
    }

    /**
     * Whether or not to instrument method calls using the instrumentation
     * framework.
     *
     * @return whether to instrument calls.
     */
    protected boolean instrumentMethodCalls()
    {
        return false;
    }

    /**
     * Await a connection on our transport and, when we get one, hand it back.
     * The returned connection will be active and running.
     *
     * <p>In the event of a failure to correctly connect an error message will
     * be sent back and the connection dropped.
     *
     * @return the new connection.
     *
     * @throws IOException       if there was a problem.
     * @throws SecurityException if access was blocked by security mechanisms.
     */
    protected final Connection awaitConnection()
        throws IOException,
               SecurityException
    {
        final Transport transport = myTransportProvider.accept();
        LOG.info(
            myName + " Got connection " + transport
        );

        // Grab its streams
        final InputStream  is = transport.getInputStream();
        final OutputStream os = transport.getOutputStream();

        // Do the handshake string exchange
        final StringBuilder hello = new StringBuilder();
        for (int i=0; i < HELLO.length(); i++) {
            // Build up the hello string one char at a time, so that we don't
            // block on a malformed one
            final int c = is.read();
            hello.append((char)c);
            if (c != (int)HELLO.charAt(i)) {
                break;
            }
        }
        for (int i=0; i < HELLO.length(); i++) {
            os.write((byte)HELLO.charAt(i));
        }

        // Check to see if the HELLO string was what we expected. If not we send
        // back an error message and drop the connection.
        for (int i=0; i < HELLO.length(); i++) {
            if (hello.charAt(i) != HELLO.charAt(i)) {
                final String error =
                    "Malformed HELLO string; " +
                    "had \""      + hello + "\", " +
                    "expected \"" + HELLO + "\"";
                final int len = Math.min(127, error.length());
                os.write(-len);
                for (int j=0; j < len; j++) {
                    os.write((int)error.charAt(j));
                }
                transport.close();
                throw new IOException(error + "; dropping connection");
            }
        }

        // Read the client script name and PID
        final int count = ((Byte.toUnsignedInt((byte)is.read()) << 24) |
                           (Byte.toUnsignedInt((byte)is.read()) << 16) |
                           (Byte.toUnsignedInt((byte)is.read()) <<  8) |
                           (Byte.toUnsignedInt((byte)is.read())      ));
        final byte[] buffer = new byte[count];
        for (int i=0; i < count; i++) {
            buffer[i] = (byte)is.read();
        }
        final String argv = new String(buffer, 0, count, "UTF-16");
        final int pid   = ((      Byte.toUnsignedInt((byte)is.read()) << 24) |
                           (      Byte.toUnsignedInt((byte)is.read()) << 16) |
                           (      Byte.toUnsignedInt((byte)is.read()) <<  8) |
                           (      Byte.toUnsignedInt((byte)is.read())      ));
        final long id   = (((long)Byte.toUnsignedInt((byte)is.read()) << 56) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) << 48) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) << 40) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) << 32) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) << 24) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) << 16) |
                           ((long)Byte.toUnsignedInt((byte)is.read()) <<  8) |
                           ((long)Byte.toUnsignedInt((byte)is.read())      ));

        // Grab the host address and authenticated username and see if they are
        // allowed to connect. If not we send back an error message and drop the
        // connection.
        final String      username = transport.getUserName();
        final InetAddress address  = transport.getRemoteAddress();
        if (!isUserPermitted(username)) {
            final String error = (username != null) ?
                "User not permissioned: \"" + username + "\"" :
                "Unauthenticated users not permitted";
            final int len = Math.min(127, error.length());
            os.write(-len);
            for (int i=0; i < len; i++) {
                os.write((int)error.charAt(i));
            }

            // Close, flag and bail
            transport.close();
            connectionRejected(username, address, transport, error);
            throw new SecurityException(error);
        }
        else if (!isHostPermitted(address)) {
            final String error = (address != null) ?
                "Host address not permissioned: " + address :
                "Unknown remote host not permitted";
            final int len = Math.min(127, error.length());
            os.write(-len);
            for (int i=0; i < len; i++) {
                os.write((int)error.charAt(i));
            }

            // Close, flag and bail
            transport.close();
            connectionRejected(username, address, transport, error);
            throw new SecurityException(error);
        }
        else {
            // Otherwise we echo back our name, this may not be longer than 127
            // chars since we encode the length as a signed byte. This is done
            // to simplify the client side's error handling. (This probably
            // doesn't matter too much since it's not really used by the client
            // right now. Though I think I'm going to regret saying this.)
            final int len = Math.min(127, myName.length());
            os.write(len);
            for (int i=0; i < len; i++) {
                os.write((int)myName.charAt(i));
            }

            // Now send along some flags telling the other side about
            // us; just a byte for now
            final byte flags =
                (byte)((useWorkers() ? Flags.USE_WORKERS.value : 0));
            os.write(flags);
        }

        // Flag a successful connection
        connectionAccepted(username, address, transport, argv, pid, id);

        // Create it the handler thread and set it rolling
        final Connection c =
            new Connection(
                myName + ":" + (myConnectionIndex++) + ":" + username,
                transport,
                id
            );
        c.setDaemon(true);
        c.start();

        // Now give it back
        return c;
    }

    /**
     * Terminates the PJRmi service, rendering this instance unusable.
     *
     * @throws IOException if there was a problem.
     */
    public void close()
        throws IOException
    {
        if (myIsClosed.compareAndSet(false, true)) {
            if (myTransportProvider != null) {
                myTransportProvider.close();
            }

            // Interrupt the PJRmi thread so that (hopefully) it wakes up from
            // any blocked calls it's in and is able to terminate
            this.interrupt();
        }
    }

    /**
     * How we might instrument.
     *
     * @param name  The name of the instrumentor to get.
     *
     * @return the instrumentor.
     */
    protected Instrumentor getInstrumentor(final String name)
    {
        return Instrumentor.NULL_INSTRUMENTOR;
    }

    // ---------------------------------------------------------------------- //

    /**
     * Simple testing method which exposes a socket.
     * <pre>
         java -Djava.library.path=`python -c 'import pjrmi; print(str(pjrmi.get_config()["libpath"]))'` \
              -classpath `python -c 'import pjrmi; print(str(pjrmi.get_config()["classpath"]))'`        \
                  com.deshaw.pjrmi.PJRmi
     * </pre>
     *
     * The connect with: <pre>
     *   import pjrmi
     *   c = pjrmi.connect_to_socket('localhost', 65432)
     *</pre>
     *
     * @param args  Ignored.
     *
     * @throws Exception if there was a problem.
     */
    public static void main(String[] args)
        throws Exception
    {
        // Get our params from properties
        final String prefix    = "com.deshaw.pjrmi.PJRmi.";
        final String logLevel  = System.getProperty(prefix + "logLevel", "INFO");
        final String port      = System.getProperty(prefix + "port", "65432");
        final String storeName = System.getProperty(prefix + "storeName");
        final String storePass = System.getProperty(prefix + "storePassword");

        // Set up logging
        LOG.setLevel(Level.parse(logLevel));

        // Set up the transport
        final int portNum = Integer.parseInt(port);
        final Transport.Provider provider =
            (storeName != null && storePass != null)
                ? new SSLSocketProvider(portNum, storeName, storePass)
                : new SocketProvider   (portNum);

        // Create a simple instance which just echoes back the name it's given
        LOG.info("Waiting for connections with " + provider);
        final PJRmi pjrmi =
            new PJRmi("PJRmi", provider, true) {
                @Override
                protected boolean isUserPermitted(CharSequence username)
                {
                    return true;
                }

                @Override
                protected int numWorkers()
                {
                    return 2;
                }

                @Override
                protected Object getObjectInstance(CharSequence name)
                {
                    return name.toString().intern();
                }
            };

        // Set it rolling
        pjrmi.run();
    }
}
