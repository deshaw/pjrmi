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

    // The file format version, written directly after the ArrayType char and
    // checked on read. The on-disk layout (notably ARRAY_DATA_OFFSET) is not
    // covered by the PJRmi protocol handshake, which only compares the major
    // and minor version. Bumping this whenever the layout changes lets a reader
    // reject a file written by an incompatible peer with a clear error rather
    // than silently misreading it.
    static const uint8_t FORMAT_VERSION = 1;

    // Byte offsets within the file. The ArrayType char and the format version
    // sit directly after the header; the array contents follow at an aligned
    // offset.
    static const size_t TYPE_OFFSET           = sizeof(HEADER_BYTES);
    static const size_t FORMAT_VERSION_OFFSET = TYPE_OFFSET + sizeof(char);

    // The array contents live at this byte offset within the file, past the
    // header, the ArrayType char and the format version. It is a multiple of
    // the largest primitive type's alignment; since mmap() hands back a page-
    // aligned base address, this makes the mapped array contents aligned too,
    // so an array returned to numpy as a direct mapping is suitably aligned for
    // any type.
    static const size_t ARRAY_DATA_OFFSET = 64;

    // The data must start past the header, the ArrayType char and the version.
    static_assert(ARRAY_DATA_OFFSET > FORMAT_VERSION_OFFSET,
                  "ARRAY_DATA_OFFSET must leave room for the file header");

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

        // Total number of bytes we need to write. The array contents start at
        // ARRAY_DATA_OFFSET, past the HEADER_BYTES and ArrayType char, so that
        // the contents land on an aligned boundary within the mapped file.
        const size_t bytes_to_write = ARRAY_DATA_OFFSET + array_bytes;

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

        // Size the file up front so that the padding between the header and the
        // aligned data offset is present and zeroed, regardless of array_bytes.
        if (ftruncate(fd, bytes_to_write) == -1) {
            int errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Could not size file with ftruncate()",
                     errnum
                ).c_str()
            );
        }

        // Write the HEADER_BYTES, the type of the array and the format version.
        // We explicitly create variables for these as write() takes a void
        // pointer.
        write(fd, HEADER_BYTES, sizeof(HEADER_BYTES));
        const char casted_type = char(type);
        write(fd, &casted_type, sizeof(casted_type));
        const uint8_t version = FORMAT_VERSION;
        write(fd, &version, sizeof(version));

        // Seek to the aligned data offset, leaving the intervening padding
        // bytes as the zeroes written by ftruncate() above.
        if (lseek(fd, ARRAY_DATA_OFFSET, SEEK_SET) == -1) {
            int errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Could not seek to data offset",
                     errnum
                ).c_str()
            );
        }

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

        // Total number of bytes we need to write. The array contents start at
        // ARRAY_DATA_OFFSET, past the HEADER_BYTES and ArrayType char, so that
        // the contents land on an aligned boundary within the mapped file.
        const size_t bytes_to_write = ARRAY_DATA_OFFSET + array_bytes;

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

        // Size the file to exactly the bytes we need. ftruncate() zero-fills,
        // which also gives us the padding between the header and the data.
        if (ftruncate(fd, bytes_to_write) == -1) {
            errnum = errno;
            close(fd);
            unlink(file);
            throw exception::io(
                format_error(
                    "write_bytes_to_shm(): Could not size file with ftruncate()",
                     errnum
                ).c_str()
            );
        }

        // Check the size of the allocated file
        struct stat buffer;
        fstat(fd, &buffer);
        if ((size_t)buffer.st_size != bytes_to_write) {
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

        // We also write the type of the array and the format version, directly
        // after the header.
        file_addr[TYPE_OFFSET]           = char(type);
        file_addr[FORMAT_VERSION_OFFSET] = FORMAT_VERSION;

        // Copy the data in at the aligned data offset, past the header, type,
        // version and padding bytes.
        lambda(file_addr + ARRAY_DATA_OFFSET);

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
    namespace {
        /**
         * Shared implementation for the shm mapping functions. Opens, maps and
         * validates a "safe" shm file, returning a pointer to the start of its
         * array contents. The HEADER_BYTES and ArrayType header is verified and
         * skipped over before returning.
         *
         * @param who                 The caller's name, for error messages.
         * @param file                The name of the mmaped file.
         * @param array_bytes         The number of array bytes in the file.
         * @param type                The expected array type.
         * @param mmap_prot           The protection to pass to mmap() (e.g.
         *                            PROT_READ or PROT_READ | PROT_WRITE).
         * @param mmap_flags          The flags to pass to mmap() (e.g.
         *                            MAP_SHARED or MAP_PRIVATE).
         * @param unlink_on_success   Whether to unlink the file once mapped.
         *                            The mapping remains valid afterwards.
         */
        void* open_and_map_shm(const char* who,
                               const char* file,
                               const size_t array_bytes,
                               const ArrayType type,
                               const int mmap_prot,
                               const int mmap_flags,
                               const bool unlink_on_success)
        {
            // Make sure we have a nonempty and non-NULL filename
            if (file == NULL || *file == '\0') {
                throw exception::io(
                    (std::string(who) + "(): Empty filename received").c_str()
                );
            }

            // Total number of bytes we need to read. The array contents start
            // at ARRAY_DATA_OFFSET, past the HEADER_BYTES, ArrayType char and
            // alignment padding.
            const size_t bytes_to_read = ARRAY_DATA_OFFSET + array_bytes;

            // Open a file for reading
            int fd = open(file, O_RDWR);
            if (fd == -1) {
                throw exception::io(
                    format_error(
                        (std::string(who) +
                         "(): Could not open file for reading").c_str(),
                        errno
                    ).c_str()
                );
            }

            // Check the size of the file. It must be large enough to hold the
            // header, type char, padding and the full array contents, else the
            // mapping would extend past end-of-file and touching it would fault.
            struct stat s;
            fstat(fd, & s);
            if ((size_t)s.st_size < bytes_to_read) {
                close(fd);
                throw exception::io(
                    (std::string(who) +
                     "(): File size is insufficient for reading").c_str()
                );
            }

            // Now the file is ready to be mmapped.
            uint8_t* addr = (uint8_t*)mmap(NULL, bytes_to_read,
                                           mmap_prot, mmap_flags,
                                           fd, 0);
            if (addr == MAP_FAILED) {
                // Save a copy of the errno as close/unlink will overwrite it
                int errnum = errno;

                close(fd);
                unlink(file);
                throw exception::io(
                    format_error(
                        (std::string(who) + "(): Error in mmaping the file").c_str(),
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
                std::string message = std::string(who) +
                    "(): The magic bytes in this file: " + wrong_bytes;
                message            += " do not match the expected magic bytes: " +
                message            += HEADER_BYTES;
                message            += " in file " +
                message            += file;

                throw exception::io(message.c_str());
            }

            // Next, we check that the array is the same type as we are
            // expecting. The type char sits directly after the header.
            const char file_array_type = (char)addr[TYPE_OFFSET];
            if (file_array_type != (char)type) {
                unlink(file);

                // For printing out the unmatching bytes
                std::string message = std::string(who) +
                    "(): The read type is: " + (char)file_array_type;
                message            += " but the expected type is " +
                                      (char)type;
                message            += " in file " +
                message            += file;

                throw exception::io(message.c_str());
            }

            // Then we check the format version. This guards against a peer
            // built with an incompatible on-disk layout, which the PJRmi
            // handshake does not catch as it only checks the major and minor
            // version.
            const uint8_t file_version = addr[FORMAT_VERSION_OFFSET];
            if (file_version != FORMAT_VERSION) {
                unlink(file);
                throw exception::io(
                    (std::string(who) + "(): The file format version is " +
                     std::to_string((int)file_version) +
                     " but this build expects version " +
                     std::to_string((int)FORMAT_VERSION) +
                     "; the writer and reader are incompatible").c_str()
                );
            }

            // The header checked out, so the file is ours to claim. Drop it
            // from the filesystem now if asked; the mapping outlives the link.
            if (unlink_on_success) {
                unlink(file);
            }

            // The array contents begin at the aligned data offset.
            return addr + ARRAY_DATA_OFFSET;
        }
    } // anonymous namespace

    void* mmap_bytes_from_shm(const char* file,
                              const size_t array_bytes,
                              const ArrayType type)
    {
        return open_and_map_shm("mmap_bytes_from_shm",
                                file, array_bytes, type,
                                PROT_READ, MAP_SHARED,
                                /*unlink_on_success=*/false);
    }

    void* map_bytes_from_shm(const char* file,
                             const size_t array_bytes,
                             const ArrayType type)
    {
        return open_and_map_shm("map_bytes_from_shm",
                                file, array_bytes, type,
                                PROT_READ | PROT_WRITE, MAP_PRIVATE,
                                /*unlink_on_success=*/true);
    }

    void unmap_shm_array(void* array,
                         const size_t array_bytes,
                         const ArrayType type)
    {
        (void)type; // The data offset is fixed, so the type is not needed here.

        // Rewind from the array contents to the start of the mapping, past the
        // header, type char and alignment padding which precede them.
        uint8_t* base = (uint8_t*)array - ARRAY_DATA_OFFSET;
        const size_t mapped_bytes = ARRAY_DATA_OFFSET + array_bytes;
        if (munmap(base, mapped_bytes) == -1) {
            throw exception::io(
                format_error(
                    "unmap_shm_array(): Error in munmaping the array",
                    errno
                ).c_str()
            );
        }
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
        (void)type; // The data offset is fixed, so the type is not needed here.

        // We need to do pointer arithmetic on the addr pointer, so we retype it
        uint8_t* file_addr = (uint8_t*)addr;

        // Since we're given the pointer to the beginning of an array in the
        // mmaped file, we need to go back to the beginning of the file. The
        // contents start at the fixed, aligned data offset.
        file_addr -= ARRAY_DATA_OFFSET;

        // Total number of bytes of relevant information in the file (the array
        // contents plus the preceding header, type char and padding).
        const size_t file_bytes = ARRAY_DATA_OFFSET + array_bytes;

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
