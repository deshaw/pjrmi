"""
Directly exercise the no-copy shared-memory receive path.

value_of() on a native primitive array, over a localhost child-JVM transport
with use_shm_arg_passing=True, sends the data via /dev/shm and reads it back
through the C extension's _read_array -> map_bytes_from_shm (the no-copy path).
This checks correctness across the eligible primitive types, that the result is
independently writable (MAP_PRIVATE copy-on-write), that a large array reads
back fully, and that the materialize=True option returns an owned copy.

This is a standalone script rather than a unittest TestCase so that it can be
run directly against a freshly built tree without the wider test fixtures.
"""
import numpy
import sys

import pjrmi


c = pjrmi.connect_to_child_jvm(stdin=None, stdout=None, stderr=None,
                               use_shm_arg_passing=True)

assert c._use_shmdata and c._transport.is_localhost(), \
    "test requires the localhost shared-memory path to be active"


def make(jclass_sig, values):
    arr = c.class_for_name(jclass_sig)(len(values))
    for i, v in enumerate(values):
        arr[i] = v
    return arr


failures = 0
def check(cond, msg):
    global failures
    print(("  ok:   " if cond else "  FAIL: ") + msg)
    if not cond:
        failures += 1


specs = [('[D', [0.0, 1.5, -2.5, 3.25], numpy.float64),
         ('[I', [0, 1, -2, 2**30],      numpy.int32),
         ('[J', [0, 1, -2, 2**40],      numpy.int64),
         ('[F', [0.0, 1.5, -2.5],       numpy.float32),
         ('[B', [0, 1, -2, 127],        numpy.int8)]
for sig, vals, npdt in specs:
    out = c.value_of(make(sig, vals))
    check(isinstance(out, numpy.ndarray) and
          numpy.array_equal(out, numpy.array(vals, dtype=npdt)),
          "value_of(%s) -> correct ndarray" % sig)

# Copy-on-write: mutating the mapped result must not corrupt the Java source.
ja = make('[D', [10.0, 20.0, 30.0])
out = c.value_of(ja)
out[0] = -999.0
check(c.value_of(ja)[0] == 10.0,
      "writing the mapped result leaves the source array intact")

# Large array: full and interior reads of a multi-MiB payload.
n = 1024 * 1024
big = c.class_for_name('[D')(n)
big[0]   = 1.0
big[n-1] = 2.0
out = c.value_of(big)
check(len(out) == n and out[0] == 1.0 and out[n-1] == 2.0 and out[123456] == 0.0,
      "large (1M double) array reads back correctly")

# materialize=True must return an owned, independent copy.
ja = make('[D', [5.0, 6.0, 7.0])
owned = c.value_of(ja, materialize=True)
check(owned.flags['OWNDATA'], "materialize=True returns an OWNDATA array")
owned[0] = -1.0
check(c.value_of(ja)[0] == 5.0,
      "writing the materialized copy does not affect the source")

print("\n%s" %
      ("ALL SHM-RECEIVE TESTS PASSED" if failures == 0 else "%d FAILED" % failures))
sys.exit(1 if failures else 0)
