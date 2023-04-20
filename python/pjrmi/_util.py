"""
Various helper functions which support PJRmi.
"""

import numbers
import numpy

class ImpreciseRepresentationError(ValueError):
    """
    A value could not be represented precisely in the target type.
    """


def strict_int(arg):
    """
    Check that ``arg`` is an integer, and return as an ``int``.

    The input argument may be a number type (``int``/``float``/numpy type) or a
    ``str``.
      >>> strict_int(42)
      42

      >>> strict_int("42")
      42

    It may be of ``float`` type, but it must be an integer (i.e. it must not
    have a fractional part).
      >>> strict_int(42.0)
      42

      >>> strict_int(42.5)
      Traceback (most recent call last):
        ...
      TypeError: Expected an integer, but got 42.5

    Other types are not converted to int.
      >>> strict_int(Date(19950102))
      Traceback (most recent call last):
        ...
      TypeError: Expected an integer, but got Date(19950102)

    :rtype:
      ``int`` or ``long``
    :raise TypeError:
      Not an integer.
    :raise ValueError:
      Not an integer.
    """
    if type(arg) is int:
        return arg
    elif isinstance(arg, (numpy.number, numbers.Number)):
        n = int(arg)
        if n == arg:
            return n
    elif isinstance(arg, str):
        return int(arg, 10)
    raise TypeError(
        "Expected an integer, but got %r" % (arg,))


def strict_bool(arg):
    """
    Check that ``arg`` is a ``bool`` or 0 or 1, and return it as a ``bool``.

      >>> strict_bool(1)
      True

      >>> strict_bool(1.0)
      Traceback (most recent call last):
        ...
      TypeError: Expected a bool, but got a float with value 1.0

      >>> strict_bool(2)
      Traceback (most recent call last):
        ...
      ValueError: Expected False or True, but got 2

    Unlike `strict_int`, the input cannot be a string.  This is intentional
    to avoid ambiguous behavior and to so that ``strict_bool``'s behavior is a
    subset of ``bool``'s behavior.

      >>> bool("False")
      True

      >>> strict_bool("False")
      Traceback (most recent call last):
        ...
      TypeError: Expected a bool, but got a str with value 'False'

    :raise TypeError:
      Got a type other than ``bool`` or ``int``
    :raise ValueError:
      Got an integer value other than 0 or 1.
    :rtype:
      ``bool``
    """
    if isinstance(arg, bool):
        return arg
    if not isinstance(arg, (int, numpy.integer, numpy.bool8)):
        raise TypeError(
            "Expected a bool, but got a %s with value %r" %
                (type(arg).__name__, arg))
    b = bool(arg)
    if b != arg:
        raise ValueError(
            "Expected False or True, but got %r" % (arg,))
    return b


def strict_number(typ, value):
    """
    Cast the value into the given numpy type, with checking for correctness.

    This function is only designed to work with the simple numpy numeric
    types of ``intNN`` and ``floatNN``.
    """
    # NOP?
    value_type = type(value)
    if value_type is typ:
        return value

    # Strings can be "cast" to numbers so explicitly handle them. We have to
    # do this since the value constructors will take strings and turn them
    # into numbers.
    if isinstance(value, str):
        raise TypeError("String may not be cast to %s" % typ)

    # Do the cast, this may throw
    casted = typ(value)

    # NaNs are easy to check
    if numpy.isnan(casted) and numpy.isnan(value):
        return casted

    # Make sure they look the same, in a somewhat simplistic way
    if casted == value:
        # Handle the corner-case in float casting, since you have things
        # like:
        #   >>> np.float64(9007199254740993)
        #   9007199254740992.0
        #   >>> np.float64(9007199254740993) == 9007199254740993
        #   True
        # To catch this we cast the result back to the original type and
        # check that it still matches.
        if (typ not in (numpy.float32, numpy.float64) or
            type(value)(casted) == value):
            # Either it wasn't being cast to a float, or it matched when we
            # cast it back. Either way, it's safe to hand back.
            return casted

    # If we got here then we failed
    raise ImpreciseRepresentationError(
        "Value %s could not be represented as a %s" % (value, typ)
    )


def strict_array(typ, array):
    """
    Cast the values of the given array-like into the given numpy type, with
    checking for correctness. If given a non-array-like it will raise an
    exception.

    This function is only designed to work with arrays containing the simple
    `numpy` numeric types of ``intNN`` and ``floatNN``. It will also be very
    slow if given an array which is not a `numpy.array`, so don't do that.
    """
    if isinstance(array, numpy.ndarray):
        # We can be a little smarter if we have been given a numpy array

        # NOP?
        dtype = numpy.dtype(typ)
        if array.dtype is dtype:
            return array

        # If any of the dimensions of the array are zero then this is a
        # trivial operation.
        if 0 in array.shape:
            # Give back an new array of the same shape but with the new type
            return numpy.ndarray(array.shape, dtype=dtype)

        # Strings can be "cast" to numbers so explicitly handle them. We have to
        # do this since the value constructors will take strings and turn them
        # into numbers.
        if array.dtype.kind in ('U', 'S'):
            raise TypeError("String may not be cast to %s" % dtype)

        # Do the cast, this may throw
        casted = numpy.array(array, dtype=dtype)

        # Strip out the NaNs
        array_non_nans  = array [numpy.logical_not(numpy.isnan(array ))]
        casted_non_nans = casted[numpy.logical_not(numpy.isnan(casted))]

        # If we now have nothing then it was all NaNs and we can just give
        # it back directly
        if len(array_non_nans) == 0 and len(casted_non_nans) == 0:
            return casted

        # Make sure they look the same, in a somewhat simplistic way
        if numpy.all(casted_non_nans == array_non_nans):
            # Handle the corner-case in float casting, since you have things
            # like:
            #   >>> np.float64(9007199254740993)
            #   9007199254740992.0
            #   >>> np.float64(9007199254740993) == 9007199254740993
            #   True
            # To catch this we cast the result back to the original type and
            # check that it still matches.
            if (typ not in (numpy.float32, numpy.float64) or
                numpy.all(
                    numpy.array(casted_non_nans, dtype=array.dtype) == array_non_nans)
                ):
                # Either it wasn't being cast to a float, or it matched when we
                # cast it back. Either way, it's safe to hand back.
                return casted

    else:
        # We have a non-numpy array. We'll handle this by walking the
        # dimensions by hand.
        shape = numpy.shape(array)
        if len(shape) == 0:
            raise ValueError("%s was not an array" % array)

        # Like in the above, an empty dimension means that its trivial.
        if 0 in shape:
            # Give back an new array of the same shape but with the new type
            return numpy.ndarray(shape, dtype=typ)

        # Otherwise walk the dimensions by hand
        result = numpy.ndarray(shape, dtype=typ)

        # Determine how to map the elements of this dimension
        if len(shape) > 1:
            # We have multiple dimensions, recurse to handle each of them
            func = strict_array
        else:
            # We have a 1D array, just handle each element
            func = strict_number

        # This is pretty inefficient right now
        for i in range(shape[0]):
            result[i] = func(typ, array[i])

        # And give it back
        return result

    # If we got here then we failed
    raise ImpreciseRepresentationError(
        "Array could not be represented as a %s" % (typ,)
    )
