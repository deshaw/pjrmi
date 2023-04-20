package com.deshaw.pjrmi;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import java.util.Comparator;
import java.util.Objects;

/**
 * MethodUtil provides utility functions for queries related to Java methods.
 */
public class MethodUtil
{
    /**
     * How we wrap a Method or Constructor so that we can use the same methods
     * in each case.
     */
    private static interface CallableWrapper
    {
        /**
         * Get the name. For a method it's the method name, for example.
         */
        public String getName();

        /**
         * Get the return type.
         */
        public Class<?> getReturnType();

        /**
         * Get the arguments.
         */
        public Class<?>[] getParameterTypes();
    }

    /**
     * The Method version of CallableWrapper.
     */
    private static class MethodWrapper
        implements CallableWrapper
    {
        /**
         * The method which we wrap.
         */
        private final Method myMethod;

        /**
         * CTOR.
         */
        public MethodWrapper(final Method method)
        {
            myMethod = method;
        }

        /**
         * Get the method.
         */
        public Method get()
        {
            return myMethod;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getName()
        {
            return myMethod.getName();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?> getReturnType()
        {
            return myMethod.getReturnType();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?>[] getParameterTypes()
        {
            return myMethod.getParameterTypes();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myMethod.toString();
        }
    }

    /**
     * The Constructor version of CallableWrapper.
     */
    private static class ConstructorWrapper
        implements CallableWrapper
    {
        /**
         * The constructor which we wrap.
         */
        private final Constructor<?> myConstructor;

        /**
         * CTOR.
         */
        public ConstructorWrapper(final Constructor<?> constructor)
        {
            myConstructor = constructor;
        }

        /**
         * Get the constructor.
         */
        public Constructor<?> get()
        {
            return myConstructor;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getName()
        {
            return myConstructor.getName();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?> getReturnType()
        {
            return myConstructor.getDeclaringClass();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?>[] getParameterTypes()
        {
            return myConstructor.getParameterTypes();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return myConstructor.toString();
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Compare two methods according to the Java compiler binding semantics.
     * Read the Java spec, referenced below, for the official definition of
     * what that means.
     *
     * <p>We only return a non-zero value if there exists a defined binding
     * ordering between the two methods.
     *
     * <p>Firstly, we check to see if the two methods overload one-another. If
     * they do not (i.e. their names differ, or they have a different number of
     * arguments) then we return zero, since they are not comparable.
     *
     * <p>At this point, we have determined that the methods overload one-
     * another, and we need to now determine whether one method has a more
     * specific binding that the other one.
     *
     * <p>For example, assume that we have three classes all linearly inheriting
     * from one another: {@code A}, {@code B} and {@code C}; where {@code A} is
     * at the root of the inheritance hierachy. Now consider we have the
     * following methods:<ul>
     *   <li>{@code void f(B x, B y)}</li>
     *   <li>{@code void f(A x, B y)}</li>
     *   <li>{@code void f(A x, A y)}</li>
     * </ul>
     * These are in the ordering of most specific to least specific. If we call
     * the function with two {@code B} instances then we want to get the first
     * one in the list, even though that call would work for the last one in the
     * list too.
     *
     * <p>Note that, per the language spec (see below) one method is only more
     * specific than another one if <i>all</i> of the former's arguments' types
     * are not less specific than the corresponding ones in the latter. I.e. if
     * we have:<ul>
     *  <li>{@code void f(Number x, Integer y, Object  z)}</li>
     *  <li>{@code void f(Object x, Integer y, Integer z)}</li>
     * </ul>
     * Then the latter method is not more specific because the {@code Number} in
     * the former is more specific than its {@code Object}. You might think that
     * calling {@code f} with 3 {@code Integer}s would be unambiguous, but it's
     * not.
     *
     * <p>Finally, if a method in one class is overridden in a subclass then
     * it's possible for it to have the same arguments but a more specific
     * return type. These latter methods are considered to be more specific
     * since the latter will shadow the former.
     *
     * <p>Users should note that this method does not conform to the contract of
     * the equivalent one in {@link Comparator} because a return of zero does
     * not imply equality. As such, this method may not be used for general
     * purpose sorting algorithms.
     *
     * @see <dl>
     *   <dt>The Java SE8 Language specification:</dt>
     *   <dd><a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.12.2.5">
     *       15.12.2.5. Choosing the Most Specific Method</a></dd>
     * </dl>
     *
     * @param m1  The first method.
     * @param m2  The second method.
     *
     * @return A negative value if {@code m1} was more specific than {@code m2}.
     *         A positive value if the converse was true. Zero if the methods
     *         have no defined ordering between them. (A zero does <b>not</b>
     *         imply equality.
     */
    public static int compareBySpecificity(final Method m1, final Method m2)
    {
        return compareBySpecificity(new MethodWrapper(m1),
                                    new MethodWrapper(m2));
    }

    /**
     * The {@link Constructor} version of {@link #compareBySpecificity(Method,Method)}.
     *
     * @param m1  The first method.
     * @param m2  The second method.
     *
     * @return the relative specificity.
     */
    public static int compareBySpecificity(final Constructor<?> m1,
                                           final Constructor<?> m2)
    {
        return compareBySpecificity(new ConstructorWrapper(m1),
                                    new ConstructorWrapper(m2));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * How we actually compare any two CallableWrappers.
     *
     * @see #compareBySpecificity(Method,Method)
     *
     * @param w1  The first wrapper.
     * @param w2  The second wrapper.
     *
     * @return the relative specificity.
     */
    private static int compareBySpecificity(final CallableWrapper w1,
                                            final CallableWrapper w2)
    {
        // Simple name compare
        final String n1 = w1.getName();
        final String n2 = w2.getName();
        if (!n1.equals(n2)) {
            return 0;
        }

        // Simple num-args compare
        final Class<?>[] a1 = w1.getParameterTypes();
        final Class<?>[] a2 = w2.getParameterTypes();
        if (a1.length != a2.length) {
            return 0;
        }

        // Now compare by specificity. If m1's arguments are more specific then
        // they are "less than" the those of m2. E.g. this is an ordering:
        //   f(Integer)
        //   f(Number)
        //   f(Object)
        //
        // Note that if there is no definite ordering then we return zero, e.g.
        // for:
        //   f(Integer, Number)
        //   f(Number, Integer)
        // neither one is greater than the other.
        //
        // Also note that, if two methods have arguments which are not in the
        // same inheritance tree then they are not comparable (i.e. we return
        // zero). Consider these interfaces:
        //   A, B-extends-A, C, D
        // and these methods:
        //   void f(A, C)
        //   void f(B, D)
        // These calls are:
        //   f(a,    null);  <-- unique and unambiguous
        //   f(b,    null);  <-- ambiguous (could bind to either)
        //   f(null, null);  <-- ambiguous (could bind to either)
        //   f(a,    c);     <-- unique and unambiguous
        //   f(b,    d);     <-- unique and unambiguous
        // So, in all cases, no specificity ordering exists.
        //
        // Finally, note that it's possible for the arguments to _exactly_ match
        // if one class overrides a method in another class but has a more
        // specific return type. As such, we need to know if that happens too
        // and tie-break on the return type.
        boolean match = true;
        int cmp = 0;
        for (int i=0; i < a1.length; i++) {
            final Class<?> c1 = a1[i];
            final Class<?> c2 = a2[i];
            if (areEquivalent(c1, c2)) {
                // Same class is not more or less specific
            }
            else if (isAssignableFrom(c1, c2)) {
                if (cmp < 0) {
                    // We have one argument which is more specific and one
                    // argument which is less specific; therefore neither method
                    // is more specific than the other. See above.
                    return 0;
                }
                else {
                    // Remember the direction of specificity, so we can check
                    // that all the arguments don't have conflicting directions.
                    // (As per the if block just above.)
                    cmp  = 1;
                    match = false;
                }
            }
            else if (isAssignableFrom(c2, c1)) {
                if (cmp > 0) {
                    // The same one-more/one-less case, like above.
                    return 0;
                }
                else {
                    // Remember the direction, like above.
                    cmp   = -1;
                    match = false;
                }
            }
            else {
                // The two types have no relation and so they are uncomparable.
                // This renders the entire method uncomparable also. Per Java
                // semantics, it can now not be more specific than another
                // method.
                return 0;
            }
        }

        // Did we get an exact match in the arguments?
        if (match) {
            // The more specific return type overrides the less specific one
            final Class<?> r1 = w1.getReturnType();
            final Class<?> r2 = w2.getReturnType();
            if (r1.equals(r2)) {
                // This can happen if we were passed two identical methods
                return 0;
            }
            else if (isAssignableFrom(r1, r2)) {
                // w1's overridden by w2 so it's after w2 in terms of
                // specificity
                return 1;
            }
            else if (isAssignableFrom(r2, r1)) {
                // w1 overrides w2 so it's before w2 in terms of specificity
                return -1;
            }
            else {
                // The two return types are different classes and, so, they are
                // not comparable
                return 0;
            }
        }
        else {
            // The signed result indicates the relative specificity of the
            // arguments
            return cmp;
        }
    }

    /**
     * Whether two classes are equivalent, with understanding of auto-boxing.
     * This is in the context of method comparison, as opposed to type equality.
     */
    private static boolean areEquivalent(final Class<?> c1, final Class<?> c2)
    {
        // If both are null or equal then they are equivalent
        if (Objects.equals(c1, c2)) {
            return true;
        }

        // If one of them is null then they can't be equivalent. (They can't
        // both be null at this point since Object.equals() will have returned
        // true above.)
        if (c1 == null || c2 == null) {
            return false;
        }

        // Handle one of the them being primitive.
        //
        // Note that we are only considering equivalence here for the purposes
        // of method binding/overriding. That is to say that, for two methods:
        //   void f(Integer i) { ... }
        //   void f(int     i) { ... }
        // there is no obvious ordering between them. If we pass in an int or an
        // Integer in Java then it's obvious which one should be bound to.
        // However, this is not the case on the PJRmi side of things, since a
        // Python integer can be both an int and an Integer; in that case it is
        // ambiguous which method we should be binding to. Or, to put it another
        // way, without knowing the arguments with which each method may be
        // called, it's unclear which is more specific. This is not the case if
        // you have, say, Number and Integer, where you have well defined
        // specificity semantics.
        if (c1.isPrimitive() && !c2.isPrimitive()) {
            if (c1.equals(Boolean  .TYPE)) return c2.equals(Boolean  .class);
            if (c1.equals(Character.TYPE)) return c2.equals(Character.class);
            if (c1.equals(Byte     .TYPE)) return c2.equals(Byte     .class);
            if (c1.equals(Short    .TYPE)) return c2.equals(Short    .class);
            if (c1.equals(Integer  .TYPE)) return c2.equals(Integer  .class);
            if (c1.equals(Long     .TYPE)) return c2.equals(Long     .class);
            if (c1.equals(Float    .TYPE)) return c2.equals(Float    .class);
            if (c1.equals(Double   .TYPE)) return c2.equals(Double   .class);
            if (c1.equals(Void     .TYPE)) return c2.equals(Void     .class);
            throw new UnsupportedOperationException(
                "Unhandled native type: " + c1
            );
        }
        else if (!c1.isPrimitive() && c2.isPrimitive()) {
            if (c2.equals(Boolean  .TYPE)) return c1.equals(Boolean  .class);
            if (c2.equals(Character.TYPE)) return c1.equals(Character.class);
            if (c2.equals(Byte     .TYPE)) return c1.equals(Byte     .class);
            if (c2.equals(Short    .TYPE)) return c1.equals(Short    .class);
            if (c2.equals(Integer  .TYPE)) return c1.equals(Integer  .class);
            if (c2.equals(Long     .TYPE)) return c1.equals(Long     .class);
            if (c2.equals(Float    .TYPE)) return c1.equals(Float    .class);
            if (c2.equals(Double   .TYPE)) return c1.equals(Double   .class);
            if (c2.equals(Void     .TYPE)) return c1.equals(Void     .class);
            throw new UnsupportedOperationException(
                "Unhandled native type: " + c2
            );
        }
        else {
            // Both were either primitive or non-primitive and so, per the
            // earlier tests, they can't be equivalent
            return false;
        }
    }

    /**
     * Whether one class is assignable from another class. This is just like the
     * standard {@link Class} version of the method, except that it will handle
     * primitives too.
     *
     * <p>Calling this method with non-primitive classes should be equivalent to
     * called the code:<pre>
     *    c1.isAssignableFrom(c2)
     * </pre>
     * Calling it with primitives will yield the same results as one would
     * expect from compiled code. For example, one may assign a {@code long} to
     * a {@code double} but not vice versa (without casting).
     *
     * <p>One important caveat here is how we handle auto-(un)boxing, since we
     * break with the Java semantics. When compiling code, then the following is
     * true:<pre>
     *   Integer a = 1; // compiles
     *   Long    b = 1; // does not compile: int cannot be converted to Long
     *   Integer c = b; // does not compile: Long cannot be converted to Integer
     *   Long    d = a; // does not compile: Integer cannot be converted to Long
     *   int     e = a; // compiles
     *   long    f = a; // compiles
     *   int     g = b; // does not compile: Long cannot be converted to int
     *   long    h = b; // compiles
     * </pre>. This means that we have asymmetry between what happens in the
     * {@code Object} world vs in the primitive one. While this is soluble when
     * binding methods at compile time, it's not so when comparing methods like
     * we do above. (This asymmetry means that<pre>
     *   compare(m1, m2) != -compare(m2, m1)
     * </pre>.) As such, we simply say that a boxable type is only assignable to
     * and from its equivalent.
     *
     * <p>If either class is {@code null} then this method will return
     * {@code false}.
     */
    private static boolean isAssignableFrom(final Class<?> c1, final Class<?> c2)
    {
        // You can never assign to or from a null class
        if (c1 == null || c2 == null) {
            return false;
        }

        // Primitives or non-primitives?
        if (!c1.isPrimitive() && !c2.isPrimitive()) {
            // Class semantics
            return c1.isAssignableFrom(c2);
        }
        else if (c1.isPrimitive() && c2.isPrimitive()) {
            // Primitive semantics. We disallow any lossy conversions or ones
            // which simply aren't allowed.
            if (c1.equals(byte.class)) {
                return (c2.equals(byte   .class));
            }

            if (c1.equals(short.class)) {
                return (c2.equals(short  .class) ||
                        c2.equals(byte   .class));
            }

            if (c1.equals(int.class)) {
                return (c2.equals(int    .class) ||
                        c2.equals(short  .class) ||
                        c2.equals(byte   .class) ||
                        c2.equals(char   .class));
            }

            if (c1.equals(long.class)) {
                return (c2.equals(long   .class) ||
                        c2.equals(int    .class) ||
                        c2.equals(short  .class) ||
                        c2.equals(byte   .class) ||
                        c2.equals(char   .class));
            }

            if (c1.equals(float.class)) {
                return (c2.equals(float  .class) ||
                        c2.equals(long   .class) ||
                        c2.equals(int    .class) ||
                        c2.equals(short  .class) ||
                        c2.equals(byte   .class) ||
                        c2.equals(char   .class));
            }

            if (c1.equals(double.class)) {
                return (c2.equals(double .class) ||
                        c2.equals(float  .class) ||
                        c2.equals(long   .class) ||
                        c2.equals(int    .class) ||
                        c2.equals(short  .class) ||
                        c2.equals(byte   .class) ||
                        c2.equals(char   .class));
            }

            if (c1.equals(char.class)) {
                return (c2.equals(char   .class));
            }

            if (c1.equals(boolean.class)) {
                return (c2.equals(boolean.class));
            }

            if (c1.equals(void.class)) {
                return false;
            }

            // If the if()s above are canonical then we should never get here
            throw new UnsupportedOperationException(
                "Unhandled native type: " + c1
            );
        }
        else if (c1.isPrimitive() && !c2.isPrimitive()) {
            // We dictate that a primitive is only assignable from its boxed
            // Object type
            if (c1.equals(Boolean  .TYPE)) return c2.equals(Boolean  .class);
            if (c1.equals(Character.TYPE)) return c2.equals(Character.class);
            if (c1.equals(Byte     .TYPE)) return c2.equals(Byte     .class);
            if (c1.equals(Short    .TYPE)) return c2.equals(Short    .class);
            if (c1.equals(Integer  .TYPE)) return c2.equals(Integer  .class);
            if (c1.equals(Long     .TYPE)) return c2.equals(Long     .class);
            if (c1.equals(Float    .TYPE)) return c2.equals(Float    .class);
            if (c1.equals(Double   .TYPE)) return c2.equals(Double   .class);
            if (c1.equals(Void     .TYPE)) return c2.equals(Void     .class);
            throw new UnsupportedOperationException(
                "Unhandled native type: " + c1
            );
        }
        else if (!c1.isPrimitive() && c2.isPrimitive()) {
            // We dictate that a boxed Object type is only assignable from its
            // primitive type. Note that we keep the same canonical list of
            // types in the if statements, even if it looks a little weird to
            // order things this way.
            if (c2.equals(Boolean  .TYPE)) return c1.equals(Boolean  .class);
            if (c2.equals(Character.TYPE)) return c1.equals(Character.class);
            if (c2.equals(Byte     .TYPE)) return c1.equals(Byte     .class);
            if (c2.equals(Short    .TYPE)) return c1.equals(Short    .class);
            if (c2.equals(Integer  .TYPE)) return c1.equals(Integer  .class);
            if (c2.equals(Long     .TYPE)) return c1.equals(Long     .class);
            if (c2.equals(Float    .TYPE)) return c1.equals(Float    .class);
            if (c2.equals(Double   .TYPE)) return c1.equals(Double   .class);
            if (c2.equals(Void     .TYPE)) return c1.equals(Void     .class);
            throw new UnsupportedOperationException(
                "Unhandled native type: " + c2
            );
        }
        else {
            throw new IllegalStateException("Unreachable statement");
        }
    }
}
