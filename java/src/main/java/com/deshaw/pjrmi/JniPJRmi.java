package com.deshaw.pjrmi;

import java.io.IOException;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * C extensions for the {@link PJRmi} Java code.
 */
public class JniPJRmi
{
    /**
     * The name of the library.
     */
    /*package*/ static final String LIBRARY_NAME = "pjrmijni";

    /**
     * Standard logger.
     */
    private static final Logger LOG = Logger.getLogger("com.deshaw.pjrmi.JniPJRmi");

    /**
     * Whether we successfully loaded the native library.
     */
    private static final boolean ourLoadedLibrary;
    static {
        // Attempt to load in the JNI code
        boolean loaded = false;
        try {
            System.loadLibrary(LIBRARY_NAME);
            loaded = true;
        }
        catch (SecurityException | UnsatisfiedLinkError e) {
            final String libraryPath =
                ManagementFactory.getRuntimeMXBean().getLibraryPath();
            LOG.warning("Unable to load library " + LIBRARY_NAME + " " +
                        "using library path " + libraryPath + ": " + e);
        }

        // And assign
        ourLoadedLibrary = loaded;
    }

    /**
     * Private constructor to prevent instantiation of a JniPJRmi object.
     */
    private JniPJRmi()
    {
        // Nothing
    }

    /**
     * The handle for the input and output methods.
     */
    public static class ArrayHandle
    {
        /**
         * Path to memory location containing the array.
         */
        public String filename;

        /**
         * Number of elements in the array.
         * Initialized to an invalid value.
         */
        public int numElems = -1;

        /**
         * Type of array.
         */
        public char type;

        /**
         * Class constructor.
         */
        private ArrayHandle() {}

        /**
         * Class non-default constructor.
         *
         * @param  inFilename  The input filename.
         * @param  inNumElems  The number of expected elements in the returned
         *                     array.
         * @param  inType      The expected type of the returned array.
         */
        private ArrayHandle(String inFilename, int inNumElems, char inType)
        {
            filename = inFilename;
            numElems = inNumElems;
            type = inType;
        }
    }

    // -----------------------------------------------------------------------

    /**
     * Whether the library is available for use.
     *
     * @return yes or no.
     */
    public static boolean isAvailable()
    {
        return ourLoadedLibrary;
    }

    /**
     * Writes the input array into a file and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final boolean[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutBooleanArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final byte[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutByteArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final short[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutShortArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final int[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutIntegerArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final long[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutLongArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final float[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutFloatArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Writes the input array into memory and returns an object containing the
     * file information.
     *
     * @param  array  The primitive array we are writing into the file.
     *
     * @return        The file information.
     *
     * @throws  IllegalArgumentException  If there is an error in the creating
     *                                    the filename.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     * @throws  OutOfMemoryError          If the file size is insufficient.
     */
    public static ArrayHandle writeArray(final double[] array)
        throws IllegalArgumentException,
               IOException,
               OutOfMemoryError
    {
        if (ourLoadedLibrary) {
            ArrayHandle result = new JniPJRmi.ArrayHandle();
            nativePutDoubleArray(array, result);
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * Create an ArrayHandle and call readArray() on it.
     * This is to keep users from interacting with the ArrayHandle class.
     *
     * @param  inFilename  The input filename.
     * @param  inNumElems  The number of expected elements in the returned
     *                     array.
     * @param  inType      The expected type of the returned array.
     *
     * @return             The populated array Object, or NULL if an invalid
     *                     case was given or an exception occurred.
     *
     * @throws  IllegalArgumentException  If the input is invalid.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     */
    public static Object readArray(String inFilename,
                                   int inNumElems,
                                   char inType)
        throws IllegalArgumentException,
               IOException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "Input type to readArray() " + inType);
        }

        // Create the object for communicating with the JNI.
        ArrayHandle input = new ArrayHandle(inFilename,
                                            inNumElems,
                                            inType);

        // Read from the corresponding file.
        return readArray(input);
    }

    /**
     * Given the location and size of a file, this function populates an array
     * with the contents of the file. It performs sanity checks to ensure that
     * the file contains an array of the appropriate type.
     *
     * @param  input  The information of the size, location, and type of file.
     *
     * @return        The populated array Object, or NULL if an invalid case
     *                was given or an exception occurred.
     *
     * @throws  IllegalArgumentException  If the input is invalid.
     * @throws  IOException               If there is an error in using the
     *                                    file.
     */
    public static Object readArray(ArrayHandle input)
        throws IllegalArgumentException,
               IOException
    {
        if (ourLoadedLibrary) {
            // Are our inputs valid?
            if (input == null) {
                return null;
            }

            final int numElems = input.numElems;
            final String filename = input.filename;
            if (numElems < 0 || filename == null) {
                throw new IllegalArgumentException("Invalid input value");
            }

            // JNI uses char identifiers for each type.
            if (nativeIsBooleanArrayType(input.type)) {
                boolean[] resultBoolean = new boolean[numElems];
                nativeGetBooleanArray(filename, resultBoolean, numElems);
                return resultBoolean;
            }
            else if (nativeIsByteArrayType(input.type)) {
                byte[] resultByte = new byte[numElems];
                nativeGetByteArray(filename, resultByte, numElems);
                return resultByte;
            }
            else if (nativeIsShortArrayType(input.type)) {
                short[] resultShort = new short[numElems];
                nativeGetShortArray(filename, resultShort, numElems);
                return resultShort;
            }
            else if (nativeIsIntegerArrayType(input.type)) {
                int[] resultInt = new int[numElems];
                nativeGetIntegerArray(filename, resultInt, numElems);
                return resultInt;
            }
            else if (nativeIsLongArrayType(input.type)) {
                long[] resultLong = new long[numElems];
                nativeGetLongArray(filename, resultLong, numElems);
                return resultLong;
            }
            else if (nativeIsFloatArrayType(input.type)) {
                float[] resultFloat = new float[numElems];
                nativeGetFloatArray(filename, resultFloat, numElems);
                return resultFloat;
            }
            else if (nativeIsDoubleArrayType(input.type)) {
                double[] resultDouble = new double[numElems];
                nativeGetDoubleArray(filename, resultDouble, numElems);
                return resultDouble;
            }
            else {
                throw new IllegalArgumentException("Invalid input type");
            }
        }
        return null;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    /*
     * Given a character, returns whether it represents the PJRmi library
     * enumerated type.
     */
    private native static boolean nativeIsBooleanArrayType (char type);
    private native static boolean nativeIsByteArrayType    (char type);
    private native static boolean nativeIsShortArrayType   (char type);
    private native static boolean nativeIsIntegerArrayType (char type);
    private native static boolean nativeIsLongArrayType    (char type);
    private native static boolean nativeIsFloatArrayType   (char type);
    private native static boolean nativeIsDoubleArrayType  (char type);

    /*
     * Writes the array into a /dev/shm file and returns the file information.
     */
    private native static void nativePutBooleanArray (boolean[] array, ArrayHandle result);
    private native static void nativePutByteArray    (byte   [] array, ArrayHandle result);
    private native static void nativePutShortArray   (short  [] array, ArrayHandle result);
    private native static void nativePutIntegerArray (int    [] array, ArrayHandle result);
    private native static void nativePutLongArray    (long   [] array, ArrayHandle result);
    private native static void nativePutFloatArray   (float  [] array, ArrayHandle result);
    private native static void nativePutDoubleArray  (double [] array, ArrayHandle result);

    /*
     * Given the file information, returns the contents of the file as an array.
     */
    private native static void nativeGetBooleanArray (String filename, boolean[] result, int numElems);
    private native static void nativeGetByteArray    (String filename, byte   [] result, int numElems);
    private native static void nativeGetShortArray   (String filename, short  [] result, int numElems);
    private native static void nativeGetIntegerArray (String filename, int    [] result, int numElems);
    private native static void nativeGetLongArray    (String filename, long   [] result, int numElems);
    private native static void nativeGetFloatArray   (String filename, float  [] result, int numElems);
    private native static void nativeGetDoubleArray  (String filename, double [] result, int numElems);

    // ---------------------------------------------------------------------- //

    /**
     * Simple test method which prints out calls from this class.
     *
     * @param args  Ignored.
     */
    public static void main(String[] args)
    {
        // Have we loaded ourselves okay?
        LOG.info("Loaded library successfully: " + ourLoadedLibrary);

        // For testing native function calls.
        ArrayHandle result = new ArrayHandle();

        // Sanity check tests: if we write into the file and then read out the
        // results, do they match?
        boolean[] testBoolean = new boolean[]{false, true, false};
        try {
            result = writeArray(testBoolean);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjBoolean = readArray(result.filename,
                                                result.numElems,
                                                result.type);
            if (resultObjBoolean != null) {
                boolean[] resultBoolean = (boolean[])resultObjBoolean;
                if (Arrays.equals(testBoolean, resultBoolean)) {
                    LOG.info("main_boolean success");
                }
                else {
                    LOG.info("main_boolean incorrect array values");
                }
            }
            else {
                LOG.info("main_boolean returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        byte[] testByte = new byte[]{69, 121, 101, 45, 62, 118};
        try {
            result = writeArray(testByte);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjByte = readArray(result.filename,
                                             result.numElems,
                                             result.type);
            if (resultObjByte != null) {
                byte[] resultByte = (byte[])resultObjByte;
                if (Arrays.equals(testByte, resultByte)) {
                    LOG.info("main_byte success");
                }
                else {
                    LOG.info("main_byte incorrect array values");
                }
            }
            else {
                LOG.info("main_byte returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        short[] testShort = new short[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        try {
            result = writeArray(testShort);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjShort = readArray(result.filename,
                                              result.numElems,
                                              result.type);
            if (resultObjShort != null) {
                short[] resultShort = (short[])resultObjShort;
                if (Arrays.equals(testShort, resultShort)) {
                    LOG.info("main_short success");
                }
                else {
                    LOG.info("main_short incorrect array values");
                }
            }
            else {
                LOG.info("main_short returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        int[] testInt = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        try {
            result = writeArray(testInt);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjInt = readArray(result.filename,
                                            result.numElems,
                                            result.type);
            if (resultObjInt != null) {
                int[] resultInt = (int[])resultObjInt;
                if (Arrays.equals(testInt, resultInt)) {
                    LOG.info("main_int success");
                }
                else {
                    LOG.info("main_int incorrect array values");
                }
            }
            else {
                LOG.info("main_int returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        long[] testLong = new long[]{1, 2, 3, 4, 5};
        try {
            result = writeArray(testLong);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjLong = readArray(result.filename,
                                             result.numElems,
                                             result.type);
            if (resultObjLong != null) {
                long[] resultLong = (long[])resultObjLong;
                if (Arrays.equals(testLong, resultLong)) {
                    LOG.info("main_long success");
                }
                else {
                    LOG.info("main_long incorrect array values");
                }
            }
            else {
                LOG.info("main_long returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        float[] testFloat = new float[]{1, 2, 3, 4, 5, 6};
        try {
            result = writeArray(testFloat);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjFloat = readArray(result.filename,
                                              result.numElems,
                                              result.type);
            if (resultObjFloat != null) {
                float[] resultFloat = (float[])resultObjFloat;
                if (Arrays.equals(testFloat, resultFloat)) {
                    LOG.info("main_float success");
                }
                else {
                    LOG.info("main_float incorrect array values");
                }
            }
            else {
                LOG.info("main_float returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        double[] testDouble = new double[]{1, 2, 3, 4, 5};
        try {
            result = writeArray(testDouble);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }
        try {
            Object resultObjDouble = readArray(result.filename,
                                               result.numElems,
                                               result.type);
            if (resultObjDouble != null) {
                double[] resultDouble = (double[])resultObjDouble;
                if (Arrays.equals(testDouble, resultDouble)) {
                    LOG.info("main_double success");
                }
                else {
                    LOG.info("main_double incorrect array values");
                }
            }
            else {
                LOG.info("main_double returned a null object.");
            }
        }
        catch (IOException e) {
            LOG.info("readArray() failed with " + e);
        }

        // Test whether overwriting a returned ArrayHandle produces graceful
        // error messages. We don't want it to core dump.
        byte[] testError = new byte[]{3, 1, 4, 15, 92};
        try {
            result = writeArray(testError);
        }
        catch (IOException e) {
            LOG.info("writeArray() failed with " + e);
        }

        // Set result to erroneous values.
        // Preserving the original filename, set filename.
        String originalFilename = result.filename;
        result.filename = "Bad filename";

        try {
            readArray(result.filename, result.numElems, result.type);
        }
        catch (IOException e) {
            LOG.info("bad_filename failed correctly with " + e);
        }
        catch (Exception e) {
            LOG.info("bad_filename unexpectedly threw " + e);
        }

        // Revert original filename.
        result.filename = originalFilename;

        // Preserving the original numElems, set numElems.
        int originalNumElems = result.numElems;
        result.numElems = -1;

        try {
            readArray(result.filename, result.numElems, result.type);
        }
        catch (IllegalArgumentException e) {
            LOG.info("bad_num_elems failed correctly with " + e);
        }
        catch (Exception e) {
            LOG.info("bad_num_elems unexpectedly threw " + e);
        }

        // Revert original numElems.
        result.numElems = originalNumElems;

        // Preserving the original type, set type.
        // 'h' is an invalid type as it does not represent a Java primitive.
        int originalType = result.type;
        result.type = 'h';

        try {
            readArray(result.filename, result.numElems, result.type);
        }
        catch (IllegalArgumentException e) {
            LOG.info("bad_type failed correctly with " + e);
        }
        catch (Exception e) {
            LOG.info("bad_type unexpectedly threw " + e);
        }

        // Revert original type.
        result.numElems = originalType;
    }
}
