import numpy

def get_kwargs(dtype):
    if dtype == numpy.bool_:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'boolean',
            'object_type'         : 'Boolean',
            'short_object_type'   : 'Boolean',
            'default_impl'        : 'BitSet',
            'primitive_from_null' : 'false',
            'bool_from_primitive' : '',
            'bool_to_primitive'   : '',
            'num_from_primitive'  : ' ? (byte)1 : (byte)0',
            'num_to_primitive'    : ' != 0',
        }
    elif dtype == numpy.int8:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'byte',
            'object_type'         : 'Byte',
            'short_object_type'   : 'Byte',
            'default_impl'        : 'Array',
            'primitive_from_null' : '((byte)0)',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? (byte)1 : (byte)0',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    elif dtype == numpy.int16:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'short',
            'object_type'         : 'Short',
            'short_object_type'   : 'Short',
            'default_impl'        : 'Array',
            'primitive_from_null' : '((short)0)',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? (short)1 : (short)0',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    elif dtype == numpy.int32:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'int',
            'object_type'         : 'Integer',
            'short_object_type'   : 'Int',
            'default_impl'        : 'Array',
            'primitive_from_null' : '0',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? 1 : 0',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    elif dtype == numpy.int64:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'long',
            'object_type'         : 'Long',
            'short_object_type'   : 'Long',
            'default_impl'        : 'Array',
            'primitive_from_null' : '0L',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? 1L : 0L',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    elif dtype == numpy.float32:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'float',
            'object_type'         : 'Float',
            'short_object_type'   : 'Float',
            'default_impl'        : 'Array',
            'primitive_from_null' : 'Float.NaN',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? 1.0f : 0.0f',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    elif dtype == numpy.float64:
        return {
            'dtype'               : numpy.dtype(dtype),
            'primitive_type'      : 'double',
            'object_type'         : 'Double',
            'short_object_type'   : 'Double',
            'default_impl'        : 'Array',
            'primitive_from_null' : 'Double.NaN',
            'bool_from_primitive' : ' != 0',
            'bool_to_primitive'   : ' ? 1.0 : 0.0',
            'num_from_primitive'  : '',
            'num_to_primitive'    : '',
        }
    else:
        raise ValueError(f"Unhandled type: {dtype}")
