#!/usr/bin/env python3
"""
The script starts a PJRmi connection and then forks a Python child process. And
tests that the child process exits peacefully. Indirectly, this verifies that
the child process does not do connection cleanup because if the child process
tries to do this cleanup, it would get stuck while trying to close some of our
resources.
"""
from   tests.pjrmi_tests  import get_pjrmi
import os
import sys

def main():
    """
    Main body of the script.
    """
    args = sys.argv[1:]
    output_file = args[0]

    pjrmi = get_pjrmi()
    # Get the Java process PID that we can write in file, so that the unit test
    # process can make sure that we clean up properly.
    ProcessHandle = pjrmi.class_for_name('java.lang.ProcessHandle')
    java_pid = ProcessHandle.current().pid()
    python_child_pid = os.fork()
    if python_child_pid == 0:
        # Python child process
        return
    else:
        # Write both child PIDs to the file, so that the unit test can read it
        # later. To make it atomic for the reader, we will write it in tmp file
        # and then rename it.
        tmp_file = output_file + ".tmp"
        f = open(tmp_file, "w")
        f.write(str(java_pid) + " " + str(python_child_pid))
        f.close()
        os.rename(tmp_file, output_file)

        # Let's wait for the child to cleanup fine.
        os.waitpid(python_child_pid, 0)


if __name__ == "__main__":
    main()
