//----------------------------------------------------------------------------
//
// Extension module pjrmi.extension
//
// This provides special methods for PJRmi to things which require C magic.
//
//----------------------------------------------------------------------------

#include <Python.h>

// Ignore compiler warnings -- this is a known issue in NumPy, as per
// https://github.com/scikit-learn/scikit-learn/issues/1250
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>

#include <assert.h>
#include <py3c.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include <jni.h>
#include <pjrmi.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

const char* const doc_string =
"Extension module for L{pjrmi}.\n"
"\n"
"This is currently alpha quality and should not be considered ready for prime time.\n"
"The next step is to allow for multiple connections to the same JVM.\n";

//----------------------------------------------------------------------------

/**
 * Class for managing the transport to the Java VM as well as creating it.
 */
class PJRmiPipe
{
public:
    /**
     * Constructor.
     */
    PJRmiPipe(const std::vector<std::string>& classpath,
              const std::vector<std::string>& jvm_args,
              const std::vector<std::string>& args)
        noexcept(false);

    /**
     * Destructor.
     */
    ~PJRmiPipe();

    /**
     * Connect to the JVM.
     */
    void connect() noexcept(false);

    /**
     * Disconnect from the JVM.
     */
    void disconnect() noexcept(false);

    /**
     * Read a byte from the pipe.
     *
     * @return -1 on EOF
     */
    int read() noexcept(false);

    /**
     * Write a byte to the pipe.
     */
    void write(int b) noexcept(false);

private:
    /**
     * Check for a Java Exception.
     */
    void check_exception(const char* when) noexcept(false);

    /**
     * Whether we have an instance already.
     */
    static bool _have_instance;

    /**
     * Our JVM.
     */
    JavaVM* _jvm;

    /**
     * Our JVM environment.
     */
    JNIEnv* _env;

    /**
     * Who we ask for a connection from.
     */
    jobject _provider;

    /**
     * How we ask for a connection.
     */
    jmethodID _new_connection;

    /**
     * Our actual connection.
     */
    jobject _pipe;

    /**
     * The handle on the pipe's read method.
     */
    jmethodID _read;

    /**
     * The handle on the pipe's write method.
     */
    jmethodID _write;
};

// ================================================================================

bool PJRmiPipe::_have_instance = false;

/**
 * Constructor.
 */
PJRmiPipe::PJRmiPipe(const std::vector<std::string>& classpath,
                     const std::vector<std::string>& jvm_args,
                     const std::vector<std::string>& args)
    noexcept(false) :
    _jvm(NULL),
    _env(NULL),
    _pipe(NULL),
    _read(NULL),
    _write(NULL)
{
    // We can only ever create one instance since multiple JVMs are not
    // supported in the same process
    if (_have_instance) {
        throw std::runtime_error(
            "Only one JVM instance may be created per process -- sorry!"
        );
    }
    _have_instance = true;

    // Set up the JVM's options, stage them in strings for ease first
    std::vector<std::string> options;

    // Start with the classpath
    std::string cp = "-Djava.class.path=";
    for (size_t i=0; i < classpath.size(); i++) {
        if (i > 0) {
            cp += ":";
        }
        cp += classpath[i];
    }
    options.push_back(cp);

    // And add the other options
    for (size_t i=0; i < jvm_args.size(); i++) {
        options.push_back(jvm_args[i]);
    }

    // Now turn the options into what the JVM wants to see
    JavaVMOption* jvm_options = new JavaVMOption[options.size()];
    for (size_t i=0; i < options.size(); i++) {
        jvm_options[i].optionString = (char*)options[i].c_str();
    }

    // How we start the JVM
    JavaVMInitArgs vm_args;
    vm_args.version            = JNI_VERSION_1_6;
    vm_args.nOptions           = options.size();
    vm_args.options            = jvm_options;
    vm_args.ignoreUnrecognized = 1;

    // Construct a VM
    jint res = JNI_CreateJavaVM(&_jvm, (void **)&_env, &vm_args);
    if (res != JNI_OK) {
        std::stringstream err;
        err << "Failed to create Java VM: ";
        switch (res) {
        case JNI_EDETACHED: err << "Thread detached from the VM"; break;
        case JNI_EVERSION:  err << "JNI version error"; break;
        case JNI_ENOMEM:    err << "Not enough memory"; break;
        case JNI_EEXIST:    err << "VM already created"; break;
        case JNI_EINVAL:    err << "Invalid arguments"; break;
        case JNI_ERR:
        default:            err << "Unknown error"; break;
        }
        err << " (" << res << ")";
        throw std::runtime_error(err.str());
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    // Create the arguments to call into the PipedPJRmi CTOR
    jobjectArray pjrmi_args =
        (jobjectArray)_env->NewObjectArray(args.size(),
                                           _env->FindClass("java/lang/String"),
                                           nullptr);
    for (size_t i=0; i < args.size(); i++) {
        _env->SetObjectArrayElement(pjrmi_args,
                                    i,
                                    _env->NewStringUTF(args[i].c_str()));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    // Get the handles on the classes which we want
    jclass PipedPJRmi = _env->FindClass("com/deshaw/pjrmi/PipedProvider$PipedPJRmi");
    if (PipedPJRmi == NULL) {
        throw std::runtime_error("Failed to find PipedPJRmi class");
    }
    jclass PipedProvider = _env->FindClass("com/deshaw/pjrmi/PipedProvider");
    if (PipedProvider == NULL) {
        throw std::runtime_error("Failed to find PipedProvider class");
    }
    jclass BidirectionalPipe =
        _env->FindClass("com/deshaw/pjrmi/PipedProvider$BidirectionalPipe");
    if (BidirectionalPipe == NULL) {
        throw std::runtime_error("Failed to find BidirectionalPipe class");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    // Get the handle on the methods we want
    jmethodID pjrmi_ctor =
        _env->GetMethodID(PipedPJRmi,
                          "<init>",
                          "(Lcom/deshaw/pjrmi/PipedProvider;[Ljava/lang/String;)V");
    if (pjrmi_ctor == NULL) {
        throw std::runtime_error("Failed to find PJRmi constructor");
    }
    jmethodID provider_ctor = _env->GetMethodID(PipedProvider, "<init>", "()V");
    if (provider_ctor == NULL) {
        throw std::runtime_error("Failed to find PipedProvider constructor");
    }

    jmethodID pjrmi_start =
        _env->GetMethodID(PipedPJRmi, "start", "()V");
    if (pjrmi_start == NULL) {
        throw std::runtime_error("Failed to find PJRmi#start()");
    }
    _new_connection =
        _env->GetMethodID(PipedProvider,
                          "newConnection",
                          "()Lcom/deshaw/pjrmi/PipedProvider$BidirectionalPipe;");
    if (_new_connection == NULL) {
        throw std::runtime_error(
            "Failed to find 'BidirectionalPipe PipedProvider#newConnection()'"
        );
    }
    _read  = _env->GetMethodID(BidirectionalPipe, "read", "()I");
    if (_read == NULL) {
        throw std::runtime_error("Failed to find 'int BidirectionalPipe#read()'");
    }
    _write = _env->GetMethodID(BidirectionalPipe, "write", "(I)V");
    if (_write == NULL) {
        throw std::runtime_error("Failed to find 'void BidirectionalPipe#write(int)'");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    // Create a new Provider instance
    _provider = _env->NewGlobalRef(_env->NewObject(PipedProvider, provider_ctor));
    check_exception("constructing PipedProvider instance");
    assert(_provider != NULL);

    // And the PipedPJRmi which we'll talk to. We create a Global Reference to
    // this guy but then forget about it. We should probably clean it up in the
    // DTOR...
    jobject pjrmi = _env->NewGlobalRef(
                        _env->NewObject(PipedPJRmi, pjrmi_ctor, _provider, pjrmi_args)
                    );
    check_exception("constructing PipedPJRmi instance");
    assert(pjrmi != NULL);

    // Start the PJRmi instance
    _env->CallVoidMethod(pjrmi, pjrmi_start);
}

/**
 * Connect to the JVM.
 */
void PJRmiPipe::connect()
    noexcept(false)
{
    if (_pipe != NULL) {
        throw std::runtime_error("Already connected");
    }

    _pipe = _env->NewGlobalRef(_env->CallObjectMethod(_provider, _new_connection));
}

/**
 * Disconnect from the JVM.
 */
void PJRmiPipe::disconnect()
    noexcept(false)
{
    if (_pipe == NULL) {
        throw std::runtime_error("Not connected");
    }

    _pipe = NULL;
    _env->DeleteGlobalRef(_pipe);
}

/**
 * Destructor.
 */
PJRmiPipe::~PJRmiPipe()
{
    _jvm->DestroyJavaVM();
}

/**
 * Write a byte down the pipe.
 */
int PJRmiPipe::read()
    noexcept(false)
{
    jint result = _env->CallIntMethod(_pipe, _read);
    check_exception("reading from pipe");
    return result;
}

/**
 * Write a byte to the pipe.
 */
void PJRmiPipe::write(int b)
    noexcept(false)
{
    _env->CallVoidMethod(_pipe, _write, b);
    check_exception("writing to pipe");
}

/**
 * Check for a Java exception and throw a C++ one instead.
 */
void PJRmiPipe::check_exception(const char* when)
    noexcept(false)
{
    if (_env->ExceptionCheck() == JNI_TRUE) {
        jthrowable exceptionObj = _env->ExceptionOccurred();
        _env->ExceptionClear();

        std::string errstr("[Unknown error]");
        jclass Throwable = _env->FindClass("java/lang/Throwable");
        if (Throwable != NULL) {
            jmethodID toString = _env->GetMethodID(Throwable,
                                                   "toString",
                                                   "()Ljava/lang/String;");
            if (toString != NULL) {
                jstring err = (jstring)_env->CallObjectMethod(exceptionObj, toString);
                if (err != NULL) {
                    // Convert to a std::string; this won't respect unicode right now
                    const jchar* chars = _env->GetStringChars(err, NULL);
                    errstr = "";
                    for (int i=0; chars[i] != 0; i++) {
                        errstr += (char)chars[i];
                    }
                    _env->ReleaseStringChars(err, chars);
                }
            }
        }

        // Throw the exception
        std::stringstream msg;
        msg << "Caught Java exception when " << when << ": " << errstr;
        throw std::runtime_error(msg.str());
    }
}

//----------------------------------------------------------------------------

// The singleton instance
static PJRmiPipe* _pipe = NULL;

/*
 * Create the JVM instance
 */
static const char* _create_jvm_doc = \
"create_jvm(classpath, java_args)\n"
"\n"
"Create the JVM instance using the given class path and arguments.\n"
"\n"
"@param classpath:  a tuple of strings defining the classpath\n"
"@param java_args:  a tuple of strings comprising the arguments for the JVM.\n";
static PyObject* _create_jvm(PyObject* /*self*/, PyObject* args)
{
    // We don't want a pipe to already exist
    if (_pipe != NULL) {
        // XXX Use a cleaner exception for this
        PyErr_SetString(PyExc_RuntimeError, "JVM already exists");
        return NULL;
    }

    // Get the arguments
    PyObject* py_classpath = NULL;
    PyObject* py_java_args = NULL;
    PyObject* py_app_args  = NULL;
    if (!PyArg_ParseTuple(args, "OOO",
                          &py_classpath, &py_java_args, &py_app_args))
    {
        return NULL;
    }
    if (!PyTuple_Check(py_classpath)) {
        PyErr_SetString(PyExc_ValueError, "classpath was not a tuple");
        return NULL;
    }
    if (!PyTuple_Check(py_java_args)) {
        PyErr_SetString(PyExc_ValueError, "java_args was not a tuple");
        return NULL;
    }
    if (!PyTuple_Check(py_app_args)) {
        PyErr_SetString(PyExc_ValueError, "java_args was not a tuple");
        return NULL;
    }

    // Pull out the classpath
    std::vector<std::string> classpath;
    for (Py_ssize_t i=0, len = PyTuple_Size(py_classpath); i < len; i++) {
        PyObject* py_path = PyTuple_GetItem(py_classpath, i);
        Py_ssize_t count;
        const char *path = PyStr_AsUTF8AndSize(py_path, &count);
        if (path == NULL) {
            return NULL;
        }
        classpath.push_back(std::string(path, count));
    }

    // Pull out the java_args
    std::vector<std::string> java_args;
    for (Py_ssize_t i=0, len = PyTuple_Size(py_java_args); i < len; i++) {
        PyObject* py_arg = PyTuple_GetItem(py_java_args, i);
        Py_ssize_t count;
        const char *arg = PyStr_AsUTF8AndSize(py_arg, &count);
        if (arg == NULL) {
            return NULL;
        }
        java_args.push_back(std::string(arg, count));
    }

    // And the PJRmi arguments
    std::vector<std::string> application_args;
    for (Py_ssize_t i=0, len = PyTuple_Size(py_app_args); i < len; i++) {
        PyObject* py_arg = PyTuple_GetItem(py_app_args, i);
        Py_ssize_t count;
        const char *arg = PyStr_AsUTF8AndSize(py_arg, &count);
        if (arg == NULL) {
            return NULL;
        }
        application_args.push_back(std::string(arg, count));
    }

    // Save the current signal handler for SIGINT since the JVM installs its own
    // which we don't want. The JVM's one will cause the process to exit but we
    // really want CTRL-C to generate a KeyboardInterrupt in Python so that the
    // user still has what they expect on the command line.
    struct sigaction py_sigint;
    const int sigaction_result = sigaction(SIGINT, NULL, &py_sigint);

    // Now we can actually create the C++ class
    try {
        _pipe = new PJRmiPipe(classpath, java_args, application_args);
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Can't create JVM: " << e.what();
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        return NULL;
    }

    // Restore the python signal handler, if we had one
    if (sigaction_result == 0 && py_sigint.sa_handler != SIG_DFL) {
        sigaction(SIGINT, &py_sigint, NULL);
    }

    // Okay
    Py_RETURN_NONE;
}

/*
 * Connect to the JVM.
 */
static const char* _connect_doc =
"connect()\n"
"\n"
"Connect to the JVM.\n";
static PyObject* _connect(PyObject* /*self*/, PyObject* args)
{
    try {
        _pipe->connect();
        Py_RETURN_NONE;
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Connect failed: " << e.what();
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        return NULL;
    }
}

/*
 * Disconnect from the JVM.
 */
static const char* _disconnect_doc =
"connect()\n"
"\n"
"Disconnect from the JVM.\n";
static PyObject* _disconnect(PyObject* /*self*/, PyObject* args)
{
    if (_pipe == NULL) {
        PyErr_SetString(PyExc_ValueError, "Not connected");
        return NULL;
    }

    try {
        _pipe->disconnect();
        delete(_pipe);
        _pipe = NULL;
        Py_RETURN_NONE;
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Disconnect failed: " << e.what();
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        return NULL;
    }
}

/*
 * Read a given number of bytes from the pipe
 */
static const char* _read_doc =
"read(count)\n"
"\n"
"Read a given number of bytes from the pipe.\n"
"\n"
"@param count: the number of bytes to read"
;
static PyObject* _read(PyObject* /*self*/, PyObject* args)
{
    // We must have a pipe already
    if (_pipe == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "No JVM");
        return NULL;
    }

    PyObject* py_count = NULL;
    if (! PyArg_ParseTuple(args, "O", &py_count)) {
        return NULL;
    }

    // How much to read
    const long count = std::min(64 * 1024L, PyInt_AsLong(py_count));
    if (count <= 0) {
        // The value will be -1 and PyErr_Occurred will be set if it wasn't an
        // integer
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_ValueError, "Non-positive count");
        }
        return NULL;
    }

    try {
        // Read into here
        char result[count+1];

        // Read in the amount we want to read, or up until the EOF marker
        Py_ssize_t len;
        for (len=0; len < count; len++) {
            const int byte = _pipe->read();
            if (byte < 0) {
                break;
            }
            else {
                result[len] = byte;
            }
        }

        // Terminate
        result[len] = '\0';

        // And hand back the Pythonified version
        return PyBytes_FromStringAndSize(result, len);
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Read failed: " << e.what();
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        return NULL;
    }
}

/*
 * Write out a string.
 */
static const char* _write_doc =
"write(bytes)\n"
"\n"
"Write the given bytes into the pipe.\n"
"\n"
"@param bytes: The bytes to write\n";
static PyObject* _write(PyObject* /*self*/, PyObject* args)
{
    // We must have a pipe already
    if (_pipe == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "No JVM");
        return NULL;
    }

    PyObject* py_bytes = NULL;
    if (! PyArg_ParseTuple(args, "O", &py_bytes)) {
        return NULL;
    }

    char *bytes = PyBytes_AsString(py_bytes);
    if (bytes == NULL) {
        return NULL;
    }
    Py_ssize_t count = PyBytes_Size(py_bytes);

    try {
        for (Py_ssize_t i=0; i < count; i++) {
            _pipe->write(bytes[i]);
        }
        return Py_BuildValue("");
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Write failed: " << e.what();
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        Py_RETURN_NONE;
    }
}

/*
 * Write an one-dimensional numpy array to a memory-mapped file.
 */
static const char* _write_array_doc =
"write_array(bytes)\n"
"\n"
"Write the given array bytes into a memory-mapped file.\n"
"\n"
"@param array_bytes: The bytes to write\n";
static PyObject* _write_array(PyObject* /*self*/, PyObject* args)
{
    // We don't need a pipe as we are writing directly into memory

    // We must have an array in order to use this function
    PyArrayObject *array;
    if (!PyArg_ParseTuple(args, "O!", &PyArray_Type, &array)) {
        // All error management taken care of in ParseTuple()
        Py_RETURN_NONE;
    }

    // Get the size of the array. Since we are only working with one-
    // dimensional arrays right now, the array returned by PyArrayObject
    // dimensions only has one element.
    const int num_elems = PyArray_DIM(array, 0);

    // Sanity check elem_size
    if (num_elems < 0) {
        std::stringstream err;
        err << "Write Array failed: number of elements negative";
        PyErr_SetString(PyExc_IOError, err.str().c_str());
        Py_RETURN_NONE;
    }

    // Get the number of bytes per array element. Similar to above, this
    // function works for multi-dimensional numpy arrays as well.
    const int elem_size = PyArray_STRIDE(array, 0);

    // Sanity check elem_size
    if (elem_size < 0) {
        std::stringstream err;
        err << "Write Array failed: element size negative";
        PyErr_SetString(PyExc_IOError, err.str().c_str());
        Py_RETURN_NONE;
    }

    // Calculate the size of the array in bytes
    const uint64_t array_bytes = (uint64_t)num_elems * (uint64_t)elem_size;

    // Get a pointer to the bag o' bytes representing the array
    void* buf = PyArray_DATA(array);

    // Get the type of the array
    int type = PyArray_TYPE(array);

    // Get the corresponding ArrayType
    des::pjrmi::ArrayType pjrmi_type;
    switch(type) {
    case NPY_BOOL:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_BOOLEAN;
        break;

    case NPY_BYTE:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_BYTE;
        break;

    case NPY_SHORT:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_SHORT;
        break;

    case NPY_INT:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_INTEGER;
        break;

    case NPY_FLOAT:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_FLOAT;
        break;

    case NPY_LONG:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_LONG;
        break;

    case NPY_DOUBLE:
        pjrmi_type = des::pjrmi::ArrayType::TYPE_DOUBLE;
        break;

    default:
        std::stringstream err;
        err << "Write Array failed: Unhandled element type " << type;
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        Py_RETURN_NONE;
    }

    // Where the array is written
    std::string filename;

    // Write the array to memory
    try {
        filename = des::pjrmi::write_bytes_to_shm(buf, array_bytes, pjrmi_type);
    }
    catch (des::pjrmi::exception::pjrmi_exception& e) {
        std::stringstream err;
        err << "Write Array failed: " << e.what();
        PyErr_SetString(PyExc_IOError, err.str().c_str());
        Py_RETURN_NONE;
    }

    // We will return the filename, number of bytes, and type as a tuple
    return Py_BuildValue("slc",
                         filename.c_str(),
                         num_elems,
                         pjrmi_type);
}

/*
 * Read a given number of bytes from the pipe
 */
static const char* _read_array_doc =
"read_array(count)\n"
"\n"
"Read a given number of bytes from the memory-mapped file.\n"
"\n"
"@param filename: where we are reading from"
"\n"
"@param num_elems: the number of elements to read"
"\n"
"@param type: the type of array to read"
;
static PyObject* _read_array(PyObject* /*self*/, PyObject* args)
{
    // We extract the tuple parts to the following:
    char* filename = NULL;
    int num_elems;
    char type;

    if (! PyArg_ParseTuple(args, "sic", &filename, &num_elems, &type)) {
        Py_RETURN_NONE;
    }

    // Number of bytes to read
    long array_bytes;

    // Get the corresponding ArrayType from the input
    des::pjrmi::ArrayType pjrmi_type = des::pjrmi::char_to_array_type(type);

    // Sanity check type input
    if (pjrmi_type == des::pjrmi::ArrayType::UNKNOWN) {
        std::stringstream err;
        err << "Read Array failed: Unknown input type " << type;
        PyErr_SetString(PyExc_IOError, err.str().c_str());
        Py_RETURN_NONE;
    }

    // Get the corresponding NPY_Type
    int numpy_type;
    switch(pjrmi_type) {
    case des::pjrmi::ArrayType::TYPE_BOOLEAN:
        numpy_type = NPY_BOOL;
        // Calculate the number of elements we are reading
        // sizeof(Boolean) = 1 = sizeof(int8_t) in Java, Python, and C!
        array_bytes = num_elems * sizeof(int8_t);
        break;

    case des::pjrmi::ArrayType::TYPE_BYTE:
        numpy_type = NPY_BYTE;
        array_bytes = num_elems * sizeof(int8_t);
        break;

    case des::pjrmi::ArrayType::TYPE_SHORT:
        numpy_type = NPY_SHORT;
        array_bytes = num_elems * sizeof(int16_t);
        break;

    case des::pjrmi::ArrayType::TYPE_INTEGER:
        numpy_type = NPY_INT;
        array_bytes = num_elems * sizeof(int32_t);
        break;

    case des::pjrmi::ArrayType::TYPE_LONG:
        numpy_type = NPY_LONG;
        array_bytes = num_elems * sizeof(int64_t);
        break;

    case des::pjrmi::ArrayType::TYPE_FLOAT:
        numpy_type = NPY_FLOAT;
        // We specify float here because its type is not overly ambiguous
        array_bytes = num_elems * sizeof(float);
        break;

    case des::pjrmi::ArrayType::TYPE_DOUBLE:
        numpy_type = NPY_DOUBLE;
        array_bytes = num_elems * sizeof(double);
        break;

    default:
        std::stringstream err;
        err << "Read Array failed: Unhandled element type " << type;
        PyErr_SetString(PyExc_RuntimeError, err.str().c_str());
        Py_RETURN_NONE;
    }

    try {
        // Read from memory-mapped file
        void* data = des::pjrmi::read_bytes_from_shm(filename,
                                                     array_bytes,
                                                     pjrmi_type);

        // Currently we can only handle 1-dimensional arrays
        long dims[1] = {num_elems};

        // Create the numpy array
        PyObject* result = PyArray_SimpleNewFromData(1,
                                                     dims,
                                                     numpy_type,
                                                     data);

        // This enables numpy to free the memory allocated in result when the
        // array is garbage collected.
        PyArray_ENABLEFLAGS((PyArrayObject *)result, NPY_ARRAY_OWNDATA);

        // Hand back the array
        return result;
    }
    catch (des::pjrmi::exception::io& e) {
        std::stringstream err;
        err << "Read of "  << num_elems
            << " of type " << type
            << " from "    << filename
            << " failed.";
        PyErr_SetString(PyExc_IOError, err.str().c_str());
        return NULL;
    }
}

}  // anonymous namespace

//----------------------------------------------------------------------------

extern "C" {

// Module methods
static PyMethodDef functions[] = {
    {"create_jvm",  (PyCFunction)_create_jvm,  METH_VARARGS, _create_jvm_doc },
    {"connect",     (PyCFunction)_connect,     METH_VARARGS, _connect_doc    },
    {"disconnect",  (PyCFunction)_disconnect,  METH_VARARGS, _disconnect_doc },
    {"read",        (PyCFunction)_read,        METH_VARARGS, _read_doc       },
    {"write",       (PyCFunction)_write,       METH_VARARGS, _write_doc      },
    {"read_array",  (PyCFunction)_read_array,  METH_VARARGS, _read_array_doc },
    {"write_array", (PyCFunction)_write_array, METH_VARARGS, _write_array_doc},
    {NULL}  /* Sentinel */
};


static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, /* m_base */
    "pjrmi.extension",     /* m_name */
    PyDoc_STR(doc_string), /* m_doc */
    -1,                    /* m_size */
    functions              /* m_methods */
};

MODULE_INIT_FUNC(extension)
{
    PyObject* m;
    try {
        // To import the PyArray API
        _import_array();

        m = PyModule_Create(&moduledef);
        if (m == NULL) {
            return NULL;
        }
    }
    catch (std::exception& e) {
        std::stringstream err;
        err << "Can't initialize extension module: " << e.what();
        Py_FatalError(err.str().c_str());
        return NULL;
    }

    if (PyErr_Occurred()) {
        Py_FatalError("Can't initialize extension module");
        return NULL;
    }
    return m;
}


}  // extern "C"
