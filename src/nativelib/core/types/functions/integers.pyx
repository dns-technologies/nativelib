from decimal import Decimal


cpdef object read_int(
    object fileobj,
    int length,
    object precision,
    object scale,
    object tzinfo,
    object enumcase,
):
    """Read signed integer from Native Format."""

    cdef bytes int_value = fileobj.read(length)
    return int.from_bytes(int_value, "little", signed=True)


cpdef bytes write_int(
    object dtype_value,
    int length,
    object precision,
    object scale,
    object tzinfo,
    object enumcase,
):
    """Write signed integer into Native Format."""

    cdef object value

    if dtype_value is None:
        return bytes(length)

    if dtype_value.__class__ in (float, Decimal):
        dtype_value = round(dtype_value)

    value = int(dtype_value)

    if length == 1:
        if value < -128:
            value = -128
        elif value > 127:
            value = 127
    elif length == 2:
        if value < -32768:
            value = -32768
        elif value > 32767:
            value = 32767
    elif length == 4:
        if value < -2147483648:
            value = -2147483648
        elif value > 2147483647:
            value = 2147483647
    elif length == 8:
        if value < -9223372036854775808:
            value = -9223372036854775808
        elif value > 9223372036854775807:
            value = 9223372036854775807

    return value.to_bytes(length, "little", signed=True)


cpdef object read_uint(
    object fileobj,
    int length,
    object precision = None,
    object scale = None,
    object tzinfo = None,
    object enumcase = None,
):
    """Read unsigned integer from Native Format."""

    cdef bytes int_value = fileobj.read(length)
    return int.from_bytes(int_value, "little", signed=False)


cpdef bytes write_uint(
    object dtype_value,
    int length,
    object precision = None,
    object scale = None,
    object tzinfo = None,
    object enumcase = None,
):
    """Write unsigned integer into Native Format."""

    cdef object value

    if dtype_value is None:
        return bytes(length)

    if dtype_value.__class__ in (float, Decimal):
        dtype_value = round(dtype_value)

    value = int(dtype_value)

    if length == 1:
        if value < 0:
            value = 0
        elif value > 255:
            value = 255
    elif length == 2:
        if value < 0:
            value = 0
        elif value > 65535:
            value = 65535
    elif length == 4:
        if value < 0:
            value = 0
        elif value > 4294967295:
            value = 4294967295
    elif length == 8:
        if value < 0:
            value = 0
        elif value > 18446744073709551615:
            value = 18446744073709551615

    return value.to_bytes(length, "little", signed=False)


cdef unsigned long long r_uint(object fileobj, unsigned char length):
    """Cython read uint function (для длин 1-8 байт)."""

    cdef bytes int_value = fileobj.read(length)
    return int.from_bytes(int_value, "little", signed=False)


cdef bytes w_uint(unsigned long long dtype_value, unsigned char length):
    """Cython write uint function (для длин 1-8 байт)."""

    return dtype_value.to_bytes(length, "little", signed=False)
