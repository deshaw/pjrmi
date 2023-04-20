/**
 * Test harness for the C++ pjrmi code
 */

#include <pjrmi.h>
#include <cassert>
#include <iostream>
#include <string.h>
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

    std::cout << "All tests passed okay!" << std::endl;
    exit(EXIT_SUCCESS);
}
