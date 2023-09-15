#!/usr/bin/env python3
"""
A simple array server which uses Hypercubes to act as backing storage for
ndarrays in Python clients.

This is done all in a single script for the purposes of this example, but the
general principles are there.
"""

from   threading  import Thread

import numpy
import pjrmi
import socket
import time

# ------------------------------------------------------------------------------

# Choose an arbitrary size for our 3D cubes/ndarrays
SIZE = 512

# ------------------------------------------------------------------------------

# Create the Java server and get handles on all the classes which we'll need. In
# a real world scenario this connection would be a network-based one, not a
# child-based one. Nothing else changes though.
c = pjrmi.connect_to_child_jvm(stdout=None, stderr=None)

Dimension            = c.class_for_name('com.deshaw.hypercube.Dimension')
BufferedInputStream  = c.class_for_name('java.io.BufferedInputStream')
BufferedOutputStream = c.class_for_name('java.io.BufferedOutputStream')
DataInputStream      = c.class_for_name('java.io.DataInputStream')
DataOutputStream     = c.class_for_name('java.io.DataOutputStream')
DoubleArrayHypercube = c.class_for_name('com.deshaw.hypercube.DoubleArrayHypercube')
ServerSocket         = c.class_for_name('java.net.ServerSocket')
Socket               = c.class_for_name('java.net.Socket')

# ------------------------------------------------------------------------------

# The methods which the client calls to interact with the server. Notice that
# the client effectively builds everything which it needs in each case; the
# server is completely passive and doesn't actually have any specialised
# networking code etc.

def unflatten(src, dst, chunk_size=4096):
    """
    Copy the doubles from the Python-side ``src`` array into the Java-side
    ``dst`` hypercube.

    The ``chunk_size`` is the number of array elements which we push up together
    in a single ``send`` call.
    """
    # We are going to do this by setting up a back channel to the Java process
    # and having the hypercube populate itself directly by reading bytes in off
    # the wire. This means that the values don't need to be buffered (much) in
    # the Java server as they are read in and then copied into the hypercube.
    #
    # We could also do this by calling the Hypercube.unflatten() method and
    # passing in an ndarray, but that requires the Java server to read a copy of
    # the full array before it can push the values into the hypercube instance.
    #
    # Finally, since hypercubes are thread-safe, this method doesn't need to
    # worry about what other clients might be doing. There still exist race
    # conditions, if two clients write to the same location, but no undefined
    # behaviour will result outside of that.

    # Note that the socket is unsecured. One way to address this is by having a
    # more complex listener on the Java side and sending a shared secret (e.g. a
    # random 64bit value) as the handshake. That would prevent a bad actor from
    # using the Java socket in the window between its creation and the
    # connection from the Python client.

    # This method realies on the source and destination matching in shape
    if tuple(src.shape) != tuple(dst.shape):
        raise ValueError(
            "Source shape, %s, does not match the destination shape, %s" % (
                tuple(src.shape),
                tuple(dst.shape)
            )
        )

    # Set up a listener on the Java side
    ss   = ServerSocket(0)
    host = 'localhost'
    port = ss.getLocalPort()

    # Start listening in the Java server. We use a Python thread to drive this.
    def read():
        dst.fromFlattened(
            DataInputStream(
                BufferedInputStream(
                    ss.accept().getInputStream()
                )
            )
        )
    thread = Thread(target=read)
    thread.daemon = True
    thread.start()

    # Set up the writer on the Python side
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # And send all the data, in chunks
    sz   = max(1, chunk_size)
    flat = src.reshape((src.size,))
    end  = len(flat)
    for idx in range(0, end, sz):
        e = idx + sz
        if e > end:
            e = end
        sock.sendall(flat[idx:e].tobytes('C'))

    # Now close the connection on each side, since we're done
    try:
        ss.close()
        sock.close()
    except Exception:
        pass


def flatten(src, dst, chunk_size=4096):
    """
    Copy the doubles from the Java-side ``src`` hypercube into the Python-side
    ``dst`` ndarray.

    The ``chunk_size`` is the number of array elements which we pull down together
    in a single ``recv`` call.
    """
    # Much the same logic as in unflatten() above, but going in the other
    # direction. The same observations apply.

    # This method realies on the source and destination matching in shape
    if tuple(src.shape) != tuple(dst.shape):
        raise ValueError(
            "Source shape, %s, does not match the destination shape, %s" % (
                tuple(src.shape),
                tuple(dst.shape)
            )
        )

    # Set up a listener on the Java side
    ss   = ServerSocket(0)
    host = 'localhost'
    port = ss.getLocalPort()

    # Start sending from the Java server, this will block under the Python
    # client connects
    def write():
        try:
            src.toFlattened(
                DataOutputStream(
                    BufferedOutputStream(
                        ss.accept().getOutputStream()
                    )
                )
            )
        except Exception as e:
            print("Failed: %s" % e)
    thread = Thread(target=write)
    thread.daemon = True
    thread.start()

    # Set up the writer on the Python side
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # And read all the into the ndarray data, in chunks
    sz    = max(1, chunk_size)
    count = sz
    flat  = dst.reshape((src.size,))
    end   = len(flat)
    for idx in range(0, end, sz):
        e = idx + sz
        if e > end:
            e = end
            count = e - idx
        data = b''
        while len(data) < count * 8:
            data += sock.recv(count * 8 - len(data))
        flat[idx:e] = numpy.frombuffer(data, dtype='float64', count=count)

    try:
        ss.close()
        sock.close()
    except Exception:
        pass

# ------------------------------------------------------------------------------

# Now the main body of the example. We're going to create the source and
# destination cube/array, populate the source, push it into the destination, and
# read it back again. We'll then to the same for a sliced sub-cube.

# Create the local and remote cubes
print("Creating the ndarray and cube...")
ndarray = numpy.ndarray((SIZE,SIZE,SIZE), dtype=numpy.float64)
cube    = DoubleArrayHypercube(Dimension.of(ndarray.shape))

# Fill it with something
print("Populating the ndarray...")
ndarray.reshape((ndarray.size,))[:] = numpy.arange(ndarray.size)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# And see how long it takes to push the all data in and to get it all back again
print("Unflattening up...")
s = time.time()
unflatten(ndarray, cube)
e = time.time()
t = e - s
print("  Copying a %s ndarray with %d elements into a cube took %0.2fs, %0.2fM el/s, %dMb/s" %
      ('x'.join(map(str, ndarray.shape)),
       ndarray.size,
       t,
       ndarray.size / 1000**2 / t,
       ndarray.size * 8 / 1024**2 / t))

print("Flattening down...")
s = time.time()
flatten(cube, ndarray)
e = time.time()
t = e - s
print("  Copying a %s cube with %d elements into an ndarray took %0.2fs, %0.2fM el/s, %dMb/s" %
      ('x'.join(map(str, ndarray.shape)),
       ndarray.size,
       t,
       ndarray.size / 1000**2 / t,
       ndarray.size * 8 / 1024**2 / t))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Create sliced versions of the source and destination
sub_size = SIZE >> 2
src = cube   [:sub_size,:sub_size,:sub_size]
dst = ndarray[:sub_size,:sub_size,:sub_size]

# Do a read for a sub-section of the cube/ndarray
print("Flattening down a sub-cube...")
s = time.time()
flatten(src, dst)
e = time.time()
t = e - s
print("  Copying a %s sub-cube with %d elements into a sliced ndarray took %0.2fs, %0.2fM el/s, %dMb/s" %
      ('x'.join(map(str, src.shape)),
       dst.size,
       t,
       dst.size / 1000**2 / t,
       dst.size * 8 / 1024**2 / t))
