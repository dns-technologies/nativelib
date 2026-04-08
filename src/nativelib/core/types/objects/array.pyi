from io import BufferedReader
from typing import Any

from .dtype import DType


class Array:
    """Clickhouse column array type manipulate."""

    fileobj: BufferedReader
    dtype: DType | Array
    name: str
    is_float: int
    total_rows: int
    row_elements: list
    writable_buffer: list
    pos: int

    def __init__(
        self,
        fileobj: BufferedReader,
        dtype: DType | "Array",
        total_rows: int = 0,
    ):
        """Class initialization."""

        ...

    def skip(self) -> None:
        """Skip read native column."""

        ...

    def read(self) -> list[Any]:
        """Read array values from native column."""

        ...

    def write(self, dtype_value: list[Any]) -> int:
        """Write array values into native column."""

        ...

    def tell(self) -> int:
        """Return size of write buffers."""

        ...

    def clear(self) -> bytes:
        """Get column data and clean buffers."""

        ...

    def to_bytes(self):
        """Read dtype bytes."""

        ...
