/**
 * @file pjrmi.cpp Implementations of useful array methods for PJRmi.
 */

#include <iostream>
#include <cstdint>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "pjrmi.h"

using std::cerr;
using std::endl;

// Very simplist debugging to stderr may be enabled by compiling with
// PJRMI_DEBUG defined. We don't do anything smarter at this point since it's
// mainly useful in development and/or debugging when you're messing with the
// code directly anyhow,

namespace des {
namespace pjrmi {

    // Header bytes, used to check file "health" when reading and writing the
    // mmaped file.
    static const char HEADER_BYTES[] = "SHMARRY";

    /**
     * Given a character, returns the corresponding ArrayType
     */
    ArrayType char_to_array_type(char c)
    {
        switch(c) {
        case 'z':
            return ArrayType::TYPE_BOOLEAN;
        case 'b':
            return ArrayType::TYPE_BYTE;
        case 's':
            return ArrayType::TYPE_SHORT;
        case 'i':
            return ArrayType::TYPE_INTEGER;
        case 'j':
            return ArrayType::TYPE_LONG;
        case 'f':
            return ArrayType::TYPE_FLOAT;
        case 'd':
            return ArrayType::TYPE_DOUBLE;
        default:
            return ArrayType::UNKNOWN;
        }
    }

    /**
     * Concatenate the error message details for the given errno to the given
     * preamble string. If an invalid errno is given, return the preamble.
     *
     * @param preamble The string to use as the result's prefix. If NULL then
     *                 it is ignored.
     * @param errnum   The errno to append the details of.
     *
     * @return         The modified string with the errno.
     */
    static std::string format_error(const char* preamble, int errnum)
    {
        // Where we'll store the error message after it is converted from
        // the errno. This "huge" size should encompass the entire message.
        char buf[256];
        buf[0] = '\0';

        // Convert the errno
        strerror_r(errnum, buf, sizeof(buf));

        std::string result;
        if (preamble != NULL) {
            result += preamble;
        }

        // If the errno was converted successfully, we append the corresponding
        // error message.
        if (buf[0] != '\0') {
            result += ": [" + std::to_string(errnum) + "] ";
            result += buf;
        }

        return result;
    }

    /**
     * Generate unique filename for given thread and time.
     *
     * @return           The name of the file generated.
     *
     * @throws           illegal_argument If gettimeofday() errors.
     */
    std::string create_filename()
    {
        // Get time of day with seconds and microseconds
        struct timeval tv;
        if (::gettimeofday(&tv, NULL) != 0) {
            throw exception::illegal_argument(
                "create_filename(): Error in ::gettimeofday()"
            );
        }

        // Append time in microseconds to guarantee uniqueness of filename
        long time = tv.tv_sec;
        time *= 1000000;
        time += tv.tv_usec;

        // Get threadid
        pid_t tid = syscall(SYS_gettid);

        // Create unique filename for future mmaping.
        // Uniqueness is necessary to ensure that files are not overwritten.
        std::string filename = "/dev/shm/";
        filename += std::to_string(time) + '.' + std::to_string(tid);

        // Some processes may call this function twice in the same microsecond,
        // so we add a rand to ensure uniqueness.
        filename += '.' + std::to_string(rand());
        return filename;
    }

    /**
     * Open a file for writing and check to make sure it has sufficient
     * available space.
     *
     * @param file              The filename.
     * @param bytes_to_write    The amount of space we need in the file.
     *
     * @return                  The file descriptor of the opened file.
     *
     * @throws illegal_argument If the filename is empty.
     * @throws io               If there is an error opening the file.
     * @throws out_of_memory    If the file does not have sufficient space.
     */
    int open_file_for_write(const char* file,
                            const size_t bytes_to_write)
    {
        // Make sure we have a nonempty filename
        if (strcmp(file, "") == 0) {
            throw exception::illegal_argument(
                "open_file_for_write(): Empty filename received"
            );
        }

        // Open a file for writing.
        //  - Creating the file if it doesn't exist.
        //  - Truncating it to 0 size if it already exists. (Not really needed.)
        //
        // Note: "O_WRONLY" mode is not sufficient when mmapping, which we do
        //       in the lambda function version of write_bytes_to_shm().
        int fd = open(file, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
        if (fd == -1) {
            throw exception::io(
                format_error(
                    "open_file_for_write(): Could not open file for writing",
                     errno
                ).c_str()
            );
        }

        // For saving the errno when we print errors
        int errnum;

        // Check to make sure there is enough space in the file
        struct statvfs buf;
        if (fstatvfs(fd, &buf) == -1) {
            // Save a copy of the errno as close/unlink will overwrite it
            errnum = errno;

            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "open_file_for_write(): Could not check file with fstatvfs()",
                     errnum
                ).c_str()
            );
        }

        // Check the available space in the file, which equals the product of
        // the available blocks and the block size.
        // If this is less than the length of data, we shouldn't write.
        // We cast the unsigned longs to size_t to compare.
        if ((size_t)buf.f_bsize * (size_t)buf.f_bavail < bytes_to_write) {
            close(fd);
            unlink(file);
            throw exception::out_of_memory(
                "open_file_for_write(): Insufficient available space in file"
            );
        }

        // The caller is responsible for closing the file!
        return fd;
    }

    /**
     * Open and write to a file, unlinking the file if an error occurs.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file written will be of the form:
     *  char[8] : HEADER_BYTES
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
                                   const ArrayType type)
    {
#ifdef PJRMI_DEBUG
        cerr << __FILE__ << ":" << __LINE__ << ": "
             << "write_bytes_to_shm() begin contents "
             << std::endl;
        for (size_t i=0; i < array_bytes; i++) {
            cerr << " " << (unsigned int)(*(((char*)data) + i) & 0xFF);
        }
        cerr << std::endl;
#endif

        // Generate the filename
        std::string generated_filename;
        try {
            generated_filename = create_filename();
        }
        // create_filename() can only throw an illegal_argument
        catch (exception::illegal_argument& e) {
            throw exception::illegal_argument(e.what());
        }

        // The below C functions want the filename as a c string
        const char* file = generated_filename.c_str();

        // Total number of bytes we need to write,
        // as we are writing HEADER_BYTES<ArrayType> before the array contents
        const size_t bytes_to_write = array_bytes + sizeof(HEADER_BYTES)
                                                  + sizeof(ArrayType);

        int fd = -1;
        try {
            fd = open_file_for_write(file, bytes_to_write);
        }
        catch (exception::illegal_argument& e) {
            throw exception::illegal_argument(e.what());
        }
        catch (exception::io& e) {
            throw exception::io(e.what());
        }
        catch (exception::out_of_memory& e) {
            throw exception::out_of_memory(e.what());
        }

        // Write the HEADER_BYTES, the type of the array, and the data to the
        // file. We explicitly create a variable for the type as write() takes
        // a void pointer.
        write(fd, HEADER_BYTES, sizeof(HEADER_BYTES));
        char casted_type = char(type);
        write(fd, &casted_type, sizeof(casted_type));

        // Write it out. This might possibly need multiple calls if array_bytes
        // is larger than can be represented by an int.
        size_t position = 0;
        while (position < array_bytes) {
            // Write out what we can. We need to do the pointer arithmetic in
            // "byte" space.
            const ssize_t written = write(fd,
                                          (void*)((uint8_t*)data + position),
                                          array_bytes - position);
            if (written < 0) {
                // This will be handled below by the size check
                break;
            }
            else {
                position += written;
            }
        }

        // For saving the errno when we print errors
        int errnum;

        // Check the size of the allocated file
        struct stat buffer;
        fstat(fd, &buffer);
        if ((size_t)buffer.st_size != bytes_to_write) {
            errnum = errno;
            close(fd);
            unlink(file);
            std::string msg = "write_bytes_to_shm(): Allocated file size incorrect; got ";
            msg += std::to_string((size_t)buffer.st_size);
            msg += " bytes but was expecting ";
            msg += std::to_string(bytes_to_write);
            msg += " bytes";
            throw exception::io(
                format_error(
                    msg.c_str(),
                    errnum
                ).c_str()
            );
        }

        // Clean up
        close(fd);

        // It's up to the reader to remove the file!
        return generated_filename;
    }

    /**
     * Open and write (using the given function pointer) to a mmaped file,
     * unlinking the file if an error occurs.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file written will be of the form:
     *  char[8] : HEADER_BYTES
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
                                   const ArrayType type)
    {
        // Generate the filename
        std::string generated_filename;
        try {
            generated_filename = create_filename();
        }
        // create_filename() can only throw an illegal_argument
        catch (exception::illegal_argument& e) {
            throw exception::illegal_argument(e.what());
        }

        // The below C functions want the filename as a c string
        const char* file = generated_filename.c_str();

        // Total number of bytes we need to write,
        // as we are writing HEADER_BYTES<ArrayType> before the array contents
        const size_t bytes_to_write = array_bytes + sizeof(HEADER_BYTES)
                                                  + sizeof(ArrayType);

        int fd = -1;
        try {
            fd = open_file_for_write(file, bytes_to_write);
        }
        catch (exception::illegal_argument& e) {
            throw exception::illegal_argument(e.what());
        }
        catch (exception::io& e) {
            throw exception::io(e.what());
        }
        catch (exception::out_of_memory& e) {
            throw exception::out_of_memory(e.what());
        }

        // For saving the errno when we print errors
        int errnum;

        // Stretch the file size to equal the number of bytes we need to write
        if (lseek(fd, bytes_to_write, SEEK_SET) == -1) {
            errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Could not stretch file with lseek()",
                     errnum
                ).c_str()
            );
        }

        // Something needs to be written at the end of the file to
        // have the file actually have the new size.
        // Just writing an empty string at the current file position will do.
        //
        // Note:
        //  - The current position in the file is at the end of the stretched
        //    file due to the call to lseek().
        //  - An empty string is actually a single '\0' character, so a zero-
        //    byte will be written at the last byte of the file.
        if (write(fd, "", 1) != 1) {
            errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Could not write empty char",
                     errnum
                ).c_str()
            );
        }

        // Check the size of the allocated file
        struct stat buffer;
        fstat(fd, &buffer);
        if ((size_t)buffer.st_size != bytes_to_write + 1) {
            errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Allocated file size incorrect",
                     errnum
                ).c_str()
            );
        }

        // Now the file is ready to be mmapped.
        // We pass MAP_SHARED to both read and write mmaps() for efficiency.
        void* addr = mmap(NULL, bytes_to_write,
                          PROT_READ | PROT_WRITE, MAP_SHARED,
                          fd, 0);
        if (addr == MAP_FAILED) {
            errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Error in mmaping the file",
                     errnum
                ).c_str()
            );
        }

        // This copy of addr is a pointer to our current address to write.
        // We cast it as we're preparing to write bytes to the file.
        uint8_t* file_addr = (uint8_t*)addr;

        // Clean up; mmap() is still valid on a closed file
        close(fd);

        // Currently we are memcpy-ing the buffer to the file.
        // First, we write the HEADER_BYTES to the file.
        memcpy(file_addr, HEADER_BYTES, sizeof(HEADER_BYTES));

        // Move the pointer to our address accordingly
        file_addr += sizeof(HEADER_BYTES);

        // We also write the type of the array
        const char char_type = char(type);
        *file_addr = char_type;

        // Move the pointer to our address accordingly
        file_addr++;

        // Copy the data in
        lambda(file_addr);

        // Clean up by un-mapping the file
        if (munmap(addr, bytes_to_write) == -1) {
            errnum = errno;
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Error in munmaping the file",
                     errnum
                ).c_str()
            );
        }

        // It's up to the reader to remove the file!
        return generated_filename;
    }

    /**
     * Open a mmaped file and return a pointer to the start of the array in the
     * file (after HEADER_BYTES and ArrayType have been read).
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file read should be of the form:
     *  char[8] : HEADER_BYTES
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
                              const ArrayType type)
    {
        // Make sure we have a nonempty and non-NULL filename
        if (file == NULL || *file == '\0') {
            throw exception::io(
                "mmap_bytes_from_shm(): Empty filename received"
            );
        }

        // Total number of bytes we need to read, as we are reading
        // HEADER_BYTES and the ArrayType char before the array contents.
        const size_t bytes_to_read = array_bytes + sizeof(HEADER_BYTES)
                                                 + sizeof(type);

        // Open a file for reading
        int fd = open(file, O_RDWR);
        if (fd == -1) {
            throw exception::io(
                format_error(
                    "mmap_bytes_from_shm(): Could not open file for reading",
                     errno
                ).c_str()
            );
        }

        // Check the size of the file
        struct stat s;
        fstat(fd, & s);

        // If size is smaller than the beginning bytes HEADER_BYTES<ArrayType>
        // we're probably in trouble.
        if ((size_t)s.st_size < (sizeof(HEADER_BYTES) + 1)) {
            close(fd);
            throw exception::io(
                "mmap_bytes_from_shm(): File size is insufficient for reading"
            );
        }

        // Now the file is ready to be mmapped.
        // We pass MAP_SHARED to both read and write mmap()s for efficiency.
        uint8_t* addr = (uint8_t*)mmap(NULL, bytes_to_read,
                                       PROT_READ, MAP_SHARED,
                                       fd, 0);
        if (addr == MAP_FAILED) {
            // Save a copy of the errno as close/unlink will overwrite it
            int errnum = errno;

            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "mmap_bytes_from_shm(): Error in mmaping the file",
                     errnum
                ).c_str()
            );
        }

        // Clean up; mmap() is still valid on a closed file
        close(fd);

        // First, we check that this file is meant for this purpose.
        // Are the first bytes of the file the header bytes?
        if (strncmp((const char*)addr, HEADER_BYTES, sizeof(HEADER_BYTES)) != 0) {
            unlink(file);

            // For printing out the unmatching bytes
            std::string wrong_bytes((const char*)addr, (const char*)addr + sizeof(HEADER_BYTES));
            std::string message = "mmap_bytes_from_shm(): The magic bytes in this file: " +
                                  wrong_bytes;
            message            += " do not match the expected magic bytes: " +
            message            += HEADER_BYTES;
            message            += " in file " +
            message            += file;

            throw exception::io(message.c_str());
        }

        // Move the pointer to our address accordingly
        addr += sizeof(HEADER_BYTES);

        // Next, we check that the array is the same type as we are expecting
        const char file_array_type = *((char*)addr);
        if (file_array_type != (char)type) {
            unlink(file);

            // For printing out the unmatching bytes
            std::string message = "mmap_bytes_from_shm(): The read type is: " +
                                  (char)file_array_type;
            message            += " but the expected type is " +
                                  (char)type;
            message            += " in file " +
            message            += file;

            throw exception::io(message.c_str());
        }

        // Move the pointer to our address accordingly -- it now points to the
        // start of the array.
        addr++;

        return addr;
    }

    /**
     * Given a pointer to the start of an array in a mmaped file, munmaps the
     * file and unlinks it.
     *
     * @param file               The name of the mmaped file.
     * @param array_bytes        The number of bytes we expect in the file.
     * @param type               The type of the array we expect in the file.
     * @param addr               The pointer in the mmaped file.
     *
     * @throws io                If there is an error in opening, mmaping, or
     *                           writing the file.
     */
    void munmap_bytes_from_shm(const char* file,
                               const size_t array_bytes,
                               const ArrayType type,
                               void* addr)
    {
        // We need to do pointer arithmetic on the addr pointer, so we retype it
        uint8_t* file_addr = (uint8_t*)addr;

        // Since we're given the pointer to the beginning of an array in the
        // mmaped file, we need to go back to the beginning of the file. As
        // we are doing operations on a "safe" file, this just means we need to
        // subtract the size of HEADER_BYTES plus the ArrayType.
        file_addr -= (sizeof(HEADER_BYTES) + sizeof(type));

        // Total number of bytes of relevant information in the file (with the
        // HEADER_BYTES and the ArrayType char before the array contents).
        const size_t file_bytes = array_bytes + sizeof(HEADER_BYTES)
                                              + sizeof(type);

        // Here's what we actually came to do
        if (munmap(file_addr, file_bytes) == -1) {
            // Save a copy of the errno as unlink will overwrite it
            int errnum = errno;

            unlink(file);
            throw exception::io(
                format_error(
                    "munmap_bytes_from_shm(): Error in munmaping the file",
                     errnum
                ).c_str()
            );
        }

        // We're done with the file now
        unlink(file);
    }

    /**
     * Open and read from mmaped file, unlinking the file afterwards. This
     * function will allocated space in memory for the file data.
     *
     * To guarantee that we are reading the correct type of array from a "safe"
     * file intended for this purpose, the file read should be of the form:
     *  char[8] : HEADER_BYTES
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
    void* read_bytes_from_shm(const char* file,
                              const size_t array_bytes,
                              const ArrayType type)
    {
        // The pointer to the start of the data in the file
        void* addr = mmap_bytes_from_shm(file, array_bytes, type);

        // We have the data from disk, create a place to put a copy of it
        void* data = malloc(array_bytes);
        if (data == NULL) {
            // Save a copy of the errno as close/unlink will overwrite it
            int errnum = errno;

            unlink(file);
            throw exception::io(
                format_error(
                    "read_bytes_from_shm(): malloc() failed",
                     errnum
                ).c_str()
            );
        }

        // Copy the data out
        memcpy(data, addr, array_bytes);

        // Clean up
        munmap_bytes_from_shm(file, array_bytes, type, addr);

#ifdef PJRMI_DEBUG
        cerr << __FILE__ << ":" << __LINE__ << ": "
             << "read_bytes_from_shm() end contents "
             << std::endl;
        for (size_t i = 0; i < array_bytes; i++) {
            cerr << " " << (unsigned int)(*(((char*)data) + i) & 0xFF);
        }
        cerr << std::endl;
#endif

        return data;
    }

} // namespace pjrmi
} // namespace des
