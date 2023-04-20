#ifndef __DES_PJRMI_H_
#define __DES_PJRMI_H_

#include <functional>
#include <stdexcept>

namespace des {
namespace pjrmi {

namespace exception {

    /**
     * Exception class for top-level exceptions related to this library.
     */
    class pjrmi_exception : public std::runtime_error {
    public:
        /** CTOR */
        pjrmi_exception(const char* msg) : std::runtime_error(msg)
        { }
    };

    /**
     * We will make a subclass for each exception we intend to throw.
     */
    class illegal_argument : public pjrmi_exception {
    public:
        /** CTOR */
        illegal_argument(const char* msg) : pjrmi_exception(msg)
        { }
    };

    class io : public pjrmi_exception {
    public:
        /** CTOR */
        io(const char* msg) : pjrmi_exception(msg)
        { }
    };

    class out_of_memory : public pjrmi_exception {
    public:
        /** CTOR */
        out_of_memory(const char* msg) : pjrmi_exception(msg)
        { }
    };

} // namespace exception

    // JNI uses char identifiers for each type
    enum class ArrayType : char {
        TYPE_BOOLEAN = 'z',
        TYPE_BYTE    = 'b',
        TYPE_SHORT   = 's',
        TYPE_INTEGER = 'i',
        TYPE_LONG    = 'j',
        TYPE_FLOAT   = 'f',
        TYPE_DOUBLE  = 'd',
        UNKNOWN      = '\0'
    };

    /**
     * Given a character, returns the corresponding ArrayType
     */
    ArrayType char_to_array_type(char c);

    /**
     * Generate unique filename for given thread and time.
     *
     * @return           The name of the file generated.
     *
     * @throws           illegal_argument If gettimeofday() errors.
     */
    std::string create_filename();

    /**
     * Open and write to a file, unlinking the file if an error occurs.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file written will be of the form:
     *  char[8] : SANITY_BYTES
     *  char    : ArrayType
     *  void*   : Array contents
     *
     * @param data              A pointer to the buffer holding the array.
     * @param array_bytes       The number of bytes in the array.
     * @param type              The type of the array.
     *
     * @return                  The name of the file where the data is written.
     *
     * @throws illegal_argument If the generated filename is empty.
     * @throws io               If there is an error in opening or writing the
     *                          file.
     * @throws out_of_memory    If the file is not big enough to hold data.
     */
    std::string write_bytes_to_shm(const void* data,
                                   const size_t array_bytes,
                                   const ArrayType type);

     /**
     * Open and write (using the given function pointer) to a mmaped file,
     * unlinking the file if an error occurs.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file written will be of the form:
     *  char[8] : SANITY_BYTES
     *  char    : ArrayType
     *  void*   : Array contents
     *
     * @param lambda            A pointer to the function we will use to write.
     *                          The function takes the write destination as a
     *                          parameter.
     * @param array_bytes       The number of bytes in the array.
     * @param type              The type of the array.
     *
     * @return                  The name of the file where the data was mmaped.
     *
     * @throws illegal_argument If the generated filename is empty.
     * @throws io               If there is an error in opening, mmaping, or
     *                          writing the file.
     * @throws out_of_memory    If the file is not big enough to hold data.
     */
    std::string write_bytes_to_shm(std::function<void(void*)> const& lambda,
                                   const size_t array_bytes,
                                   const ArrayType type);

    /**
     * Open a mmaped file and return a pointer to the start of the array in the
     * file (after SANITY_BYTES and ArrayType have been read).
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file read should be of the form:
     *  char[8] : SANITY_BYTES
     *  char    : ArrayType
     *  void*   : Array contents
     *
     * @param file               The name of the mmaped file.
     * @param array_bytes        The number of bytes we expect in the file.
     * @param type               The type of the array we expect in the file.
     *
     * @throws io                If there is an error in opening, mmaping, or
     *                           writing the file.
     */
    void* mmap_bytes_from_shm(const char* file,
                              const size_t array_bytes,
                              const ArrayType type);

    /**
     * Given a pointer to the start of an array in a mmaped file, munmaps the
     * file and unlinks it.
     *
     * @param addr               The pointer in the mmaped file.
     * @param file               The name of the mmaped file.
     * @param array_bytes        The number of bytes we expect in the file.
     * @param type               The type of the array we expect in the file.
     *
     * @throws io                If there is an error in opening, mmaping, or
     *                           writing the file.
     */
    void munmap_bytes_from_shm(const char* file,
                               const size_t array_bytes,
                               const ArrayType type,
                               void* addr);

    /**
     * Open and read from mmaped file.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file read should be of the form:
     *  char[8] : SANITY_BYTES
     *  char    : ArrayType
     *  void*   : Array contents
     *
     * @param file               The name of the mmaped file.
     * @param array_bytes        The number of bytes we expect in the file.
     * @param type               The type of the array we expect in the file.
     *
     * @throws io                If there is an error in opening, mmaping, or
     *                           writing the file.
     */
    void* read_bytes_from_shm(const char* filename,
                              const size_t array_bytes,
                              const ArrayType type);

} // namespace pjrmi
} // namespace des

#endif /*  __DES_PJRMI_H_ */
