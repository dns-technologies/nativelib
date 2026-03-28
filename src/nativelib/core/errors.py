class NativeLibError(Exception):
    """Base NativeLib error."""


class NativeLibValueError(NativeLibError, ValueError):
    """NativeLib value error."""
