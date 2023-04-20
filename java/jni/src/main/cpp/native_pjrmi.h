#ifndef _LIBJNI_NATIVE_PJRMI_JNI_H_
#define _LIBJNI_NATIVE_PJRMI_JNI_H_

#include <jni.h>
#include <des/pjrmi.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     com_deshaw_pjrmi_JniPJRmi_nativeIsTypeMethods
 * Method:    NativePJRmi
 * Signature: ()Z
 */
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
 * Class:     com_deshaw_pjrmi_JniPJRmi_nativeArrayMethods
 * Method:    NativePJRmi
 * Signature: ()
 */
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutBooleanArray
  (JNIEnv* env, jclass cls, jbooleanArray data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutByteArray
  (JNIEnv* env, jclass cls, jbyteArray    data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutShortArray
  (JNIEnv* env, jclass cls, jshortArray   data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutIntegerArray
  (JNIEnv* env, jclass cls, jintArray     data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutLongArray
  (JNIEnv* env, jclass cls, jlongArray    data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutFloatArray
  (JNIEnv* env, jclass cls, jfloatArray   data, jobject result);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativePutDoubleArray
  (JNIEnv* env, jclass cls, jdoubleArray  data, jobject result);

/*
 * Class:     com_deshaw_pjrmi_JniPJRmi_nativeGetArrayMethods
 * Method:    NativePJRmi
 * Signature: ()
 */
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetBooleanArray
  (JNIEnv* env, jclass cls, jstring filename, jbooleanArray result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetByteArray
  (JNIEnv* env, jclass cls, jstring filename, jbyteArray    result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetShortArray
  (JNIEnv* env, jclass cls, jstring filename, jshortArray   result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetIntegerArray
  (JNIEnv* env, jclass cls, jstring filename, jintArray     result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetLongArray
  (JNIEnv* env, jclass cls, jstring filename, jlongArray    result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetFloatArray
  (JNIEnv* env, jclass cls, jstring filename, jfloatArray   result, jint numElems);
JNIEXPORT void JNICALL Java_com_deshaw_pjrmi_JniPJRmi_nativeGetDoubleArray
  (JNIEnv* env, jclass cls, jstring filename, jdoubleArray  result, jint numElems);

#ifdef __cplusplus
}
#endif

#endif /* _LIBJNI_NATIVE_ARRAY_JNI_H_ */
