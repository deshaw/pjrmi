The project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and
this CHANGELOG follows the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) standard.

## [1.13.20]

### Changed
- Receiving a native array over shared memory no longer copies the data out of
  the mapped file. The array is now mapped copy-on-write and handed straight to
  numpy, which removes a full memcpy of the payload, roughly halves peak memory
  during the transfer, and only faults in the pages that are actually read.
- As a result, such arrays no longer own their memory (`ndarray.flags.OWNDATA`
  is now `False`) and hold their shared pages until garbage collected. Callers
  needing an owned array, or wanting to release the shared pages eagerly, should
  pass `materialize=True` to `value_of()` (or call `.copy()` on the result).
  The array contents are aligned, so this does not affect numerical use.

### Added
- `PJRmi.value_of()` gained a `materialize` keyword. It defaults to `False`,
  preserving the cheap copy-on-write view above; passing `True` copies the data
  into an array which owns its memory, for callers retaining many such arrays.
- The shared-memory array file now carries a format-version byte which the
  reader checks. The on-disk layout is not covered by the protocol handshake
  (which only compares the major and minor version), so this turns a layout
  mismatch between incompatible peers into a clear error rather than a silent
  misread.
