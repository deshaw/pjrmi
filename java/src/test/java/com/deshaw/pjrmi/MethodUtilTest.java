package com.deshaw.pjrmi;

import com.deshaw.pjrmi.MethodUtil;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import static com.deshaw.pjrmi.MethodUtil.compareBySpecificity;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the {@link MethodUtil} class.
 */
public class MethodUtilTest
{
    // Helper classes

    /**
     * Root interface.
     */
    public static interface A
    {
        // Nothing
    }

    /**
     * Sub-interface of {@link A}.
     */
    public static interface B
        extends A
    {
        // Nothing
    }

    /**
     * Sub-interface of {@link B}.
     */
    public static interface C
        extends B
    {
        // Nothing
    }

    /**
     * A class with Methods.
     */
    public static class Methods
    {
        public static final Method[] METHODS = Methods.class.getMethods();

        public static final Method o_f_aa;
        public static final Method o_f_ba;
        public static final Method o_f_ab;

        public static final Method o_f_ion;
        public static final Method o_f_nio;
        public static final Method o_f_oni;

        public static final Method o_f_isn;
        public static final Method o_f_nis;
        public static final Method o_f_sni;

        public static final Method v_g_n;
        public static final Method v_g_b;
        public static final Method v_g_s;
        public static final Method v_g_i;
        public static final Method v_g_l;
        public static final Method v_g_f;
        public static final Method v_g_d;

        public static final Method v_h_i;
        public static final Method v_h_I;

        static {
            try {
                o_f_aa = Methods.class.getMethod("f", A.class, A.class);
                o_f_ba = Methods.class.getMethod("f", B.class, A.class);
                o_f_ab = Methods.class.getMethod("f", A.class, B.class);

                o_f_ion = Methods.class.getMethod("f",
                                                  Integer.class,
                                                  Object .class,
                                                  Number .class);
                o_f_nio = Methods.class.getMethod("f",
                                                  Number .class,
                                                  Integer.class,
                                                  Object .class);
                o_f_oni = Methods.class.getMethod("f",
                                                  Object .class,
                                                  Number .class,
                                                  Integer.class);

                o_f_isn = Methods.class.getMethod("f",
                                                  Integer.class,
                                                  String .class,
                                                  Number .class);
                o_f_nis = Methods.class.getMethod("f",
                                                  Number .class,
                                                  Integer.class,
                                                  String .class);
                o_f_sni = Methods.class.getMethod("f",
                                                  String .class,
                                                  Number .class,
                                                  Integer.class);

                v_g_n = Methods.class.getMethod("g", Number.class);
                v_g_b = Methods.class.getMethod("g", byte  .class);
                v_g_s = Methods.class.getMethod("g", Short .class);
                v_g_i = Methods.class.getMethod("g", int   .class);
                v_g_l = Methods.class.getMethod("g", Long  .class);
                v_g_f = Methods.class.getMethod("g", float .class);
                v_g_d = Methods.class.getMethod("g", Double.class);

                v_h_i = Methods.class.getMethod("h", int    .class);
                v_h_I = Methods.class.getMethod("h", Integer.class);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            assertNotNull(o_f_aa);
            assertNotNull(o_f_ab);
            assertNotNull(o_f_ba);

            assertNotNull(o_f_ion);
            assertNotNull(o_f_nio);
            assertNotNull(o_f_oni);

            assertNotNull(o_f_isn);
            assertNotNull(o_f_nis);
            assertNotNull(o_f_sni);

            assertNotNull(v_g_n);
            assertNotNull(v_g_b);
            assertNotNull(v_g_s);
            assertNotNull(v_g_i);
            assertNotNull(v_g_l);
            assertNotNull(v_g_f);
            assertNotNull(v_g_d);

            assertNotNull(v_h_i);
            assertNotNull(v_h_I);
        }

        // Least to most specific
        public Object f(A x, A y) { return "o_f_aa"; }
        public Object f(B x, A y) { return "o_f_ba"; }
        public Object f(A x, B y) { return "o_f_ab"; }


        // Methods with what looks like a circular hierachy, but are actually
        // all incomparable
        public Object f(Integer x, String  y, Number  z) { return "o_f_isn"; }
        public Object f(Number  x, Integer y, String  z) { return "o_f_nis"; }
        public Object f(String  x, Number  y, Integer z) { return "o_f_sni"; }

        // Ditto, but with inheritance between all the arguments
        public Object f(Integer x, Object  y, Number  z) { return "o_f_ion"; }
        public Object f(Number  x, Integer y, Object  z) { return "o_f_nio"; }
        public Object f(Object  x, Number  y, Integer z) { return "o_f_oni"; }

        // Mixing primitives and non-primitives
        public void g(Number  x) { }
        public void g(byte    x) { }
        public void g(Short   x) { }
        public void g(int     x) { }
        public void g(Long    x) { }
        public void g(float   x) { }
        public void g(Double  x) { }

        // These are effectively equal for the purposes of specificity
        public void h(int     x) { }
        public void h(Integer x) { }
    }

    /**
     * A class with more methods, some which which override those in the parent
     * class.
     */
    public static class MoreMethods
        extends Methods
    {
        public final Method[] METHODS = MoreMethods.class.getMethods();

        public static final Method s_f_aa;
        static {
            try {
                s_f_aa = MoreMethods.class.getMethod("f", A.class, A.class);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            assertNotNull(s_f_aa);
        }

        /** Overloaded return type */
        public String f(A x, A y) { return "s_f_aa"; }
    }

    // ----------------------------------------------------------------------

    /**
     * Ensure that {@link MethodUtil#compareBySpecificity(Method,Method)} works.
     */
    @Test
    public void testCompare()
        throws Exception
    {
        // Comparing with self
        assertTrue(compareBySpecificity(Methods    .o_f_aa,     Methods.o_f_aa) == 0);
        assertTrue(compareBySpecificity(Methods    .o_f_ab,     Methods.o_f_ab) == 0);
        assertTrue(compareBySpecificity(Methods    .o_f_ba,     Methods.o_f_ba) == 0);
        assertTrue(compareBySpecificity(MoreMethods.s_f_aa, MoreMethods.s_f_aa) == 0);

        // Comparing with others
        assertTrue(compareBySpecificity(Methods    .o_f_aa, Methods    .o_f_ab)  > 0);
        assertTrue(compareBySpecificity(Methods    .o_f_ab, Methods    .o_f_aa)  < 0);
        assertTrue(compareBySpecificity(Methods    .o_f_ba, Methods    .o_f_ab) == 0);
        assertTrue(compareBySpecificity(Methods    .o_f_ab, Methods    .o_f_ba) == 0);
        assertTrue(compareBySpecificity(Methods    .o_f_aa, MoreMethods.s_f_aa)  > 0);
        assertTrue(compareBySpecificity(MoreMethods.s_f_aa, Methods    .o_f_aa)  < 0);

        // Comparing circular-or-not. None of these should be considered
        // comparable with one another.
        assertTrue(compareBySpecificity(Methods.o_f_ion, Methods.o_f_nio) == 0);
        assertTrue(compareBySpecificity(Methods.o_f_nio, Methods.o_f_oni) == 0);
        assertTrue(compareBySpecificity(Methods.o_f_oni, Methods.o_f_ion) == 0);
        assertTrue(compareBySpecificity(Methods.o_f_isn, Methods.o_f_nis) == 0);
        assertTrue(compareBySpecificity(Methods.o_f_nis, Methods.o_f_sni) == 0);
        assertTrue(compareBySpecificity(Methods.o_f_sni, Methods.o_f_isn) == 0);

        // Compare primitives and Object types. Remember that we have more
        // restrictive semantics when it comes to boxed types.
        //
        // First, see how everything compares against g(int), in both
        // directions.
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_n) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_b)  > 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_s) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_l) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_f)  < 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_d) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_n, Methods.v_g_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_b, Methods.v_g_i)  < 0);
        assertTrue(compareBySpecificity(Methods.v_g_s, Methods.v_g_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_i, Methods.v_g_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_l, Methods.v_g_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_g_f, Methods.v_g_i)  > 0);
        assertTrue(compareBySpecificity(Methods.v_g_d, Methods.v_g_i) == 0);
        // Now some other things, not covered by the above, which we expect to
        // be true.
        assertTrue(compareBySpecificity(Methods.v_g_s, Methods.v_g_n)  < 0);
        assertTrue(compareBySpecificity(Methods.v_g_n, Methods.v_g_s)  > 0);
        assertTrue(compareBySpecificity(Methods.v_g_d, Methods.v_g_n)  < 0);
        assertTrue(compareBySpecificity(Methods.v_g_n, Methods.v_g_d)  > 0);
        assertTrue(compareBySpecificity(Methods.v_h_I, Methods.v_h_i) == 0);
        assertTrue(compareBySpecificity(Methods.v_h_i, Methods.v_h_I) == 0);
    }
}
