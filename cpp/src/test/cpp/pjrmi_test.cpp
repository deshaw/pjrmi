/**
 * Test harness for the C++ pjrmi code
 */

#include <pjrmi.h>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

using namespace des::pjrmi;


/** Render an exception to an ostream */
static std::ostream& operator << (std::ostream& os, const exception::pjrmi_exception& e)
{
    return os << e.what();
}

/** A function to throw an exception::illegal_argument */
void throw_illegal_argument(const char* msg)
{
    throw exception::illegal_argument(msg);
}

/** A function to throw an exception::io */
void throw_io(const char* msg)
{
    throw exception::io(msg);
}

/** A function to throw an exception::out_of_memory */
void throw_out_of_memory(const char* msg)
{
    throw exception::out_of_memory(msg);
}

/** General testing for each pjrmi exception type */
void test_pjrmi_exception()
{
    bool must_be_true = true;
    bool must_be_false = false;

    // Ensure that throwing different exceptions are caught correctly
    // illegal_argument
    const char* msg = "Hello!";
    try {
        throw_illegal_argument(msg);
        must_be_true = false;
    }
    catch (exception::illegal_argument& e) {
        std::cout << "Caught: " << e << std::endl;
        must_be_false = false;
        if (strcmp(msg, e.what()) != 0) {
            std::cerr << "Incorrect msg" << std::endl;
            assert(false);
        }
    }
    catch (exception::io& e) {
        must_be_true = false;
    }
    catch (exception::out_of_memory& e) {
        must_be_true = false;
    }
    if (!must_be_true || must_be_false) {
        std::cerr << "Incorrect boolean values" << std::endl;
        std::cerr << "must_be_true: "  << must_be_true  << " "
                  << "must_be_false: " << must_be_false << std::endl;
        assert(false);
    }

    // io
    must_be_false = true;

    try {
        throw_io(msg);
        must_be_true = false;
    }
    catch (exception::io& e) {
        std::cout << "Caught: " << e << std::endl;
        must_be_false = false;
        if (strcmp(msg, e.what()) != 0) {
            std::cerr << "Incorrect msg" << std::endl;
            assert(false);
        }
    }
    catch (exception::illegal_argument& e) {
        must_be_true = false;
    }
    catch (exception::out_of_memory& e) {
        must_be_true = false;
    }
    if (!must_be_true || must_be_false) {
        std::cerr << "Incorrect boolean values" << std::endl;
        std::cerr << "must_be_true: "  << must_be_true  << " "
                  << "must_be_false: " << must_be_false << std::endl;
        assert(false);
    }

    // out_of_memory
    must_be_false = true;

    try {
        throw_out_of_memory(msg);
        must_be_true = false;
    }
    catch (exception::out_of_memory& e) {
        std::cout << "Caught: " << e << std::endl;
        must_be_false = false;
        if (strcmp(msg, e.what()) != 0) {
            std::cerr << "Incorrect msg" << std::endl;
            assert(false);
        }
    }
    catch (exception::illegal_argument& e) {
        must_be_true = false;
    }
    catch (exception::io& e) {
        must_be_true = false;
    }
    if (!must_be_true || must_be_false) {
        std::cerr << "Incorrect boolean values" << std::endl;
        std::cerr << "must_be_true: "  << must_be_true  << " "
                  << "must_be_false: " << must_be_false << std::endl;
        assert(false);
    }
}

/**
 * Given an input array, test whether it can be written to and read from a file
 * without throwing any exceptions. Compares the read array to the input
 * and checks for equality.
 *
 * After establishing a link to the file, will check to make sure the file has
 * not been unlinked. After cleaning up, will check to make sure the file has
 * been unlinked.
 *
 * Will assert(false) on any failure.
*/
void read_and_write(const void* array_input,
                    const long array_bytes,
                    ArrayType type)
{
    std::string filename;

    // Write the array to a file in memory
    try {
        filename = write_bytes_to_shm(array_input, array_bytes, type);
    }
    catch (exception::pjrmi_exception& e) {
        std::cerr << e << std::endl;
        assert(false);
    }

    // This will hold the address to the data
    void* addr;

    // Get the address to the data
    const char* file = filename.c_str();
    try {
        addr = mmap_bytes_from_shm(file, array_bytes, type);
    }
    catch (exception::pjrmi_exception& e) {
        std::cerr << e << std::endl;
        assert(false);
    }

    // Copy the data out
    void* array_output = malloc(array_bytes);
    memcpy(array_output, addr, array_bytes);

    // Are the two arrays equal? Byte-wise comparison
    if (memcmp(array_input, array_output, size_t(array_bytes)) != 0) {
        std::cerr << "Returned arrays not equal for type: "
                  << (int)type << std::endl;

        // Don't leak on error
        free(array_output);

        assert(false);
    }

    // Don't leak in general
    free(array_output);

    // We expect the file to persist here as we haven't cleaned up.
    // Stat returns 0 on success (if file exists) and -1 otherwise.
    struct stat buffer;
    if (stat(file, &buffer) != 0) {
        std::cerr << "After reading without cleaning up, file " << filename
                  << " was already unlinked" << std::endl;
        assert(false);
    }

    // Clean up
    munmap_bytes_from_shm(file, array_bytes, type, addr);

    // The file should be gone now
    if (stat(file, &buffer) == 0) {
        std::cerr << "After reading and cleaning up, file " << filename
                  << " was not unlinked" << std::endl;
        assert(false);
    }
}

/**
 * Test the no-copy mapping path. map_bytes_from_shm() hands back a pointer
 * straight into the mapped file (no copy), unlinks the file while keeping the
 * mapping valid, and provides a privately writable (copy-on-write) view whose
 * edits do not touch the backing bytes. unmap_shm_array() then releases it.
 *
 * Will assert(false) on any failure.
 */
void map_and_unmap(const void* array_input,
                   const long array_bytes,
                   ArrayType type)
{
    // Write the array out, then take ownership of it via a private mapping.
    std::string filename = write_bytes_to_shm(array_input, array_bytes, type);
    const char* file = filename.c_str();

    void* addr;
    try {
        addr = map_bytes_from_shm(file, array_bytes, type);
    }
    catch (exception::pjrmi_exception& e) {
        std::cerr << e << std::endl;
        assert(false);
    }

    // The mapped contents must equal the input, with no intervening copy.
    if (memcmp(array_input, addr, size_t(array_bytes)) != 0) {
        std::cerr << "Mapped array not equal for type: "
                  << (int)type << std::endl;
        assert(false);
    }

    // The contents must be aligned to at least 8 bytes, so that an array
    // handed straight to numpy as a mapping is suitably aligned for any type.
    if (((uintptr_t)addr % 8) != 0) {
        std::cerr << "Mapped array is misaligned: offset "
                  << ((uintptr_t)addr % 8) << std::endl;
        assert(false);
    }

    // map_bytes_from_shm() unlinks on success, but the mapping stays valid.
    struct stat buffer;
    if (stat(file, &buffer) == 0) {
        std::cerr << "After mapping, file " << filename
                  << " should have been unlinked" << std::endl;
        assert(false);
    }

    // The mapping is copy-on-write: writing to it must succeed and must leave
    // the original input buffer untouched.
    if (array_bytes > 0) {
        unsigned char* bytes = (unsigned char*)addr;
        const unsigned char original = ((const unsigned char*)array_input)[0];
        bytes[0] = (unsigned char)(original ^ 0xFF);

        if (bytes[0] != (unsigned char)(original ^ 0xFF)) {
            std::cerr << "Private mapping was not writable" << std::endl;
            assert(false);
        }
        if (((const unsigned char*)array_input)[0] != original) {
            std::cerr << "Write to mapping leaked into the source buffer"
                      << std::endl;
            assert(false);
        }
    }

    // Release the mapping.
    try {
        unmap_shm_array(addr, array_bytes, type);
    }
    catch (exception::pjrmi_exception& e) {
        std::cerr << e << std::endl;
        assert(false);
    }
}

/**
 * Write a valid array file, corrupt its format-version byte on disk, and check
 * that mapping it is rejected rather than silently misread. This is the guard
 * against a peer built with an incompatible on-disk layout.
 *
 * Will assert(false) on any failure.
 */
void rejects_bad_version()
{
    const double input[] = {1.0, 2.0, 3.0};
    const long array_bytes = sizeof(input);

    std::string filename = write_bytes_to_shm(input, array_bytes,
                                               ArrayType::TYPE_DOUBLE);

    // The format version sits at offset sizeof("SHMARRY") + 1. Flip it to a
    // value this build will not recognise.
    int fd = open(filename.c_str(), O_RDWR);
    if (fd == -1) {
        std::cerr << "Could not reopen file to corrupt version" << std::endl;
        assert(false);
    }
    const off_t version_offset = (off_t)sizeof("SHMARRY") + 1;
    const uint8_t bad_version = 0xFF;
    if (pwrite(fd, &bad_version, 1, version_offset) != 1) {
        std::cerr << "Could not corrupt version byte" << std::endl;
        assert(false);
    }
    close(fd);

    bool threw = false;
    try {
        map_bytes_from_shm(filename.c_str(), array_bytes,
                           ArrayType::TYPE_DOUBLE);
    }
    catch (exception::io& e) {
        threw = true;
        std::cout << "Correctly rejected bad version: " << e << std::endl;
    }
    if (!threw) {
        std::cerr << "A file with a bad format version was not rejected"
                  << std::endl;
        assert(false);
    }
}

int main()
{
    std::cout << "Testing PJRmi library" << std::endl;

    std::cout << "Testing pjrmi_exception class" << std::endl;

    test_pjrmi_exception();

    std::cout << "Testing create_filename()..." << std::endl;

    // Does it begin with /dev/shm?
    std::string filename;
    try {
        filename = create_filename();
        std::string begin = "/dev/shm";
        if (filename.compare(0, begin.length(), begin) != 0) {
            std::cerr << "Incorrect filename returned: "
                      << filename << std::endl;
            assert(false);
        }
    }
    catch (exception::pjrmi_exception& e) {
        std::cerr << e << std::endl;
        assert(false);
    }

    std::cout << "Testing write_bytes_to_shm() and read_bytes_to_shm()..."
              << std::endl;

    // Test with bool array
    const bool bool_input[] = {true, false, false, true, false};

    // Write and read the array to and from memory
    read_and_write(bool_input,
                   sizeof(bool_input),
                   ArrayType::TYPE_BOOLEAN);

    // Test with int array
    const int int_input[] = {1, 3, 5, 7, 9};

    // Write and read the array to and from memory
    read_and_write(int_input,
                   sizeof(int_input),
                   ArrayType::TYPE_INTEGER);

    std::cout << "Testing map_bytes_from_shm() and unmap_shm_array()..."
              << std::endl;

    // The no-copy mapping path, exercised over a few array types. The double
    // case also guards the 8-byte alignment of the mapped contents.
    map_and_unmap(bool_input,
                  sizeof(bool_input),
                  ArrayType::TYPE_BOOLEAN);
    map_and_unmap(int_input,
                  sizeof(int_input),
                  ArrayType::TYPE_INTEGER);

    const double double_input[] = {0.0, 1.5, -2.5, 3.25, 4.5};
    map_and_unmap(double_input,
                  sizeof(double_input),
                  ArrayType::TYPE_DOUBLE);

    std::cout << "Testing format version checking..." << std::endl;

    rejects_bad_version();

    std::cout << "All tests passed okay!" << std::endl;
    exit(EXIT_SUCCESS);
}
