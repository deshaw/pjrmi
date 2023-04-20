/**
 * @file com_deshaw_pjrmi_JniPJRmi.cpp C extensions for the PJRmi Java code.
 *
 * @since 2020/06/04
 */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <iostream>
#include <string>

#include <pjrmi.h>
#include <com_deshaw_pjrmi_JniPJRmi.h>

using std::cerr;
using std::endl;
using std::string;

/**
 * Throws an exception to Java.
 */
static void throw_java_exception(JNIEnv* env,
                                 const char* exception_class,
                                 const string& error_message)
{
    jclass ex = env->FindClass(exception_class);
    if (ex == NULL) {
        string msg = "Cannot find the ";
        msg += exception_class;
        msg += " class for reporting " + error_message;
        env->FatalError(msg.c_str());
    }
    else {
        env->ThrowNew(ex, error_message.c_str());
    }
}

/**
 * Throws an IllegalArgumentException to Java.
 */
static void pjrmi_exception_handle(JNIEnv* env, des::pjrmi::exception::illegal_argument& e)
{
    throw_java_exception(
        env,
        "java/lang/IllegalArgumentException",
        e.what()
    );
}

/**
 * Throws an IOException to Java.
 */
static void pjrmi_exception_handle(JNIEnv* env, des::pjrmi::exception::io& e)
{
    throw_java_exception(
        env,
        "java/io/IOException",
        e.what()
    );
}

/**
 * Throws an OutOfMemoryError to Java.
 */
static void pjrmi_exception_handle(JNIEnv* env, des::pjrmi::exception::out_of_memory& e)
{
    throw_java_exception(
        env,
        "java/lang/OutOfMemoryError",
        e.what()
    );
}

/**
 * Throws a general exception to Java.
 * We shouldn't have to use it, but this will catch anything unexpected.
 */
static void pjrmi_exception_handle(JNIEnv* env, std::exception& e)
{
    throw_java_exception(
        env,
        "java/lang/Exception",
        e.what()
    );
}

/**
 * Check the inputs from the get() native function calls.
 *
 * @param result                     The object passed from Java.
 * @param filename                   The name of the file.
 * @param num_elems                  The number of elements in the object.
 *
 * @return                           True if inputs are valid, false otherwise.
 *
 * @throws IllegalArgumentException  If inputs are bad.
 */
static bool error_check_for_get(JNIEnv* env, jstring filename,
                                jobject result, jint num_elems)
{
    if (filename == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Filename input to JNI read method is NULL"
        );
        return false;
    }

    if (strlen(env->GetStringUTFChars(filename, NULL)) == 0) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Filename input to JNI read method is empty"
        );
        return false;
    }

    if (result == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Result object input to JNI read method is NULL"
        );
        return false;
    }

    if (num_elems < 0) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Number of elements input to JNI read method is negative"
        );
        return false;
    }

    return true;
}

/**
 * Takes a bag o' bytes, writes the bytes to memory, and sets the Java object
 * with information on the file.
 *
 * @param result     A NativePJRmi$ArrayHandle instance in which to store the
 *                   result.
 * @param lambda     A pointer to the function we will use to write.
 *                   The function takes the write destination as a parameter.
 * @param num_elems  The number of elements in the array.
 * @param elem_size  The size, in bytes, of one element in the array.
 * @param type       The type of the array.
 *
 * @throws IllegalArgumentException If there is an error in filename creation
 *                                  or in arguments.
 * @throws IOException              If there is an error getting or setting
 *                                  object fields.
 * @throws OutOfMemoryError         If there is insufficient memory for the
 *                                  file.
 */
static void pjrmi_array_to_shm(JNIEnv* env,
                               jobject result,
                               std::function<void(void*)> const& lambda,
                               const des::pjrmi::ArrayType type,
                               const int num_elems, const size_t elem_size)
{
    // Argument check
    if (result == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Result argument is NULL"
        );
        return;
    }

    if (num_elems < 0) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Num_elems is negative"
        );
        return;
    }

    if (elem_size == 0) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Elem_size is zero"
        );
        return;
    }

    // Get the object fields to be returned
    jclass javaClass = env->GetObjectClass(result);

    // The filename is of type String
    jfieldID obj_filename = env->GetFieldID(javaClass,
                                "filename", "Ljava/lang/String;");
    if (obj_filename == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Given result object is missing the 'filename' field"
        );
        return;
    }

    // The array type is of type char
    jfieldID obj_type = env->GetFieldID(javaClass, "type", "C");
    if (obj_type == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Given result object is missing the 'type' field"
        );
        return;
    }

    // The number of elements is of type integer
    jfieldID obj_num_elems = env->GetFieldID(javaClass, "numElems", "I");
    if (obj_num_elems == NULL) {
        throw_java_exception(
            env,
            "java/lang/IllegalArgumentException",
            "Given result object is missing the 'numElems' field"
        );
        return;
    }

    // Calculate the size, in bytes, of the array we are writing.
    // We rely on the caller to check these elements.
    const uint64_t array_bytes = num_elems * elem_size;

    // Write the data to memory
    std::string returned_filename;
    try {
        returned_filename = des::pjrmi::write_bytes_to_shm(lambda,
                                                           array_bytes,
                                                           type);
    }
    catch (des::pjrmi::exception::illegal_argument& e) {
        pjrmi_exception_handle(env, e);
        return;
    }
    catch (des::pjrmi::exception::io& e) {
        pjrmi_exception_handle(env, e);
        return;
    }
    catch (des::pjrmi::exception::out_of_memory& e) {
        pjrmi_exception_handle(env, e);
        return;
    }
    catch (std::exception& e) {
        pjrmi_exception_handle(env, e);
        return;
    }

    // Convert it so it can be written to the object
    jstring filename = env->NewStringUTF(returned_filename.c_str());

    // Set the object fields
    env->SetObjectField(result, obj_filename,  filename);
    env->SetCharField(  result, obj_type,      (char)type);
    env->SetIntField(   result, obj_num_elems, num_elems);
}

/**
 * Given a filename, returns a pointer to the file information.
 *
 * @param filename         The name of the file.
 * @param array_bytes      The number of bytes we expect the file to contain.
 * @param type             The type of the array we expect the file to contain.
 *
 * @return                 The file information buffer.
 *
 * @throws IOException     If there is an error in using the file.
 */
static void* pjrmi_array_from_shm(JNIEnv* env,
                                  jstring filename,
                                  const uint64_t array_bytes,
                                  const des::pjrmi::ArrayType type)
{
    // The below functions need to use a char*
    const char* file = env->GetStringUTFChars(filename, NULL);

    // Make sure the filename is not null
    if (file == NULL) {
        throw_java_exception(
            env,
            "java/io/IOException",
            "Given filename is null"
        );
        return NULL;
    }

    // Get the pointer from memory
    try {
        return des::pjrmi::mmap_bytes_from_shm(file, array_bytes, type);
    }
    catch (des::pjrmi::exception::io& e) {
        pjrmi_exception_handle(env, e);
        return NULL;
    }
}

/**
 * Performs cleanup after a pjrmi_array_from_shm() call.
 *
 * @param filename         The input given to pjrmi_array_from_shm().
 * @param array_bytes      The input given to pjrmi_array_from_shm().
 * @param type             The input given to pjrmi_array_from_shm().
 * @param result           The returned value from pjrmi_array_from_shm().
 *
 * @throws IOException     If there is an error in using the file.
 */
static void pjrmi_array_from_shm_cleanup(JNIEnv* env,
                                         jstring filename,
                                         const uint64_t array_bytes,
                                         const des::pjrmi::ArrayType type,
                                         void* result)
{
    // The below functions need to use a char*
    const char* file = env->GetStringUTFChars(filename, NULL);

    // Make sure the filename is not null
    if (file == NULL) {
        throw_java_exception(
            env,
            "java/io/IOException",
            "Given filename is null"
        );
    }

    try {
        des::pjrmi::munmap_bytes_from_shm(file, array_bytes, type, result);
    }
    catch (des::pjrmi::exception::io& e) {
        pjrmi_exception_handle(env, e);
    }
}

// ------------------------------------------------------------------------- //

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsBooleanArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'z';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsByteArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'b';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsShortArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 's';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsIntegerArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'i';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsLongArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'j';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsFloatArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'f';
}

JNIEXPORT jboolean JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeIsDoubleArrayType
 (JNIEnv* env, jclass cls, jchar type)
{
    return char(type) == 'd';
}

/*
 * Native functions to write Java array data to a shared memory file.
 * See nativeBooleanArray() for code explanation.
 *
 * Note: char arrays are not currently supported as they are 2 bytes in Java
 *       but 1 byte in C++.
 */
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutBooleanArray
  (JNIEnv* env, jclass cls, jbooleanArray data, jobject result)
{
    // Get the number of elements in the array
    const int num_elems = env->GetArrayLength(data);

    // Wrap GetArrayRegion() in a lambda function that captures the Java array,
    // the number of elements, and the env variable. These are all parameters
    // that GetArrayRegion() requires.
    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetBooleanArrayRegion(data, 0, num_elems, (jboolean*)addr);
        };

    // Write data to memory
    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_BOOLEAN,
                       num_elems, sizeof(int8_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutByteArray
  (JNIEnv* env, jclass cls, jbyteArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetByteArrayRegion(data, 0, num_elems, (jbyte*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_BYTE,
                       num_elems, sizeof(int8_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutShortArray
  (JNIEnv* env, jclass cls, jshortArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetShortArrayRegion(data, 0, num_elems, (jshort*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_SHORT,
                       num_elems, sizeof(int16_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutIntegerArray
  (JNIEnv* env, jclass cls, jintArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetIntArrayRegion(data, 0, num_elems, (jint*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_INTEGER,
                       num_elems, sizeof(int32_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutLongArray
  (JNIEnv* env, jclass cls, jlongArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetLongArrayRegion(data, 0, num_elems, (jlong*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_LONG,
                       num_elems, sizeof(int64_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutFloatArray
  (JNIEnv* env, jclass cls, jfloatArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetFloatArrayRegion(data, 0, num_elems, (jfloat*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_FLOAT,
                       num_elems, sizeof(int32_t));
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutDoubleArray
  (JNIEnv* env, jclass cls, jdoubleArray data, jobject result)
{
    const int num_elems = env->GetArrayLength(data);

    std::function<void(void*)> lambda =
        [data, num_elems, env](void* addr) {
            env->GetDoubleArrayRegion(data, 0, num_elems, (jdouble*)addr);
        };

    pjrmi_array_to_shm(env,
                       result,
                       lambda, des::pjrmi::ArrayType::TYPE_DOUBLE,
                       num_elems, sizeof(int64_t));
}

// ------------------------------------------------------------------------- //

/*
 * Native functions to read Java array data from a shared memory file.
 * See nativeGetBooleanArray() for code explanation.
 */
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetBooleanArray
  (JNIEnv* env, jclass cls, jstring filename, jbooleanArray result, jint num_elems)
{
    // Check the inputs
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }

    // Calculate the size, in bytes, of the array we are reading
    // sizeof(Boolean) = 1 byte = sizeof(int8_t) in Java, Python, and C!
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(int8_t);

    // Obtain a pointer to the bytes in the file
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_BOOLEAN);

    // If data is NULL, an exception should have been set
    if (data != NULL) {
        // Set the elements of the array we received from Java
        env->SetBooleanArrayRegion(result, 0, num_elems, (jboolean *)data);

        // Cleanup after we are done using the data
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_BOOLEAN,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetByteArray
  (JNIEnv* env, jclass cls, jstring filename, jbyteArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(int8_t);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_BYTE);

    if (data != NULL) {
        env->SetByteArrayRegion(result, 0, num_elems, (jbyte *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_BYTE,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetShortArray
  (JNIEnv* env, jclass cls, jstring filename, jshortArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(int16_t);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_SHORT);

    if (data != NULL) {
        env->SetShortArrayRegion(result, 0, num_elems, (jshort *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_SHORT,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetIntegerArray
  (JNIEnv* env, jclass cls, jstring filename, jintArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(int32_t);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_INTEGER);

    if (data != NULL) {
        env->SetIntArrayRegion(result, 0, num_elems, (jint *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_INTEGER,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetLongArray
  (JNIEnv* env, jclass cls, jstring filename, jlongArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(int64_t);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_LONG);

    if (data != NULL) {
        env->SetLongArrayRegion(result, 0, num_elems, (jlong *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_LONG,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetFloatArray
  (JNIEnv* env, jclass cls, jstring filename, jfloatArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    // We specify float here because its type is not overly ambiguous
    const uint64_t array_bytes = (uint64_t)num_elems * sizeof(float);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_FLOAT);

    if (data != NULL) {
        env->SetFloatArrayRegion(result, 0, num_elems, (jfloat *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_FLOAT,
                                     data);
    }
}

JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetDoubleArray
  (JNIEnv* env, jclass cls, jstring filename, jdoubleArray result, jint num_elems)
{
    if (!error_check_for_get(env, filename, result, num_elems)) {
        return;
    }
    // We specify double here because its type is not overly ambiguous
    const uint64_t array_bytes = num_elems * sizeof(double);
    void* data = pjrmi_array_from_shm(env,
                                      filename,
                                      array_bytes,
                                      des::pjrmi::ArrayType::TYPE_DOUBLE);

    if (data != NULL) {
        env->SetDoubleArrayRegion(result, 0, num_elems, (jdouble *)data);
        pjrmi_array_from_shm_cleanup(env,
                                     filename,
                                     array_bytes,
                                     des::pjrmi::ArrayType::TYPE_DOUBLE,
                                     data);
    }
}
