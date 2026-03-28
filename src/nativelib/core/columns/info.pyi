from io import BufferedReader

from ..types.dtypes import ClickhouseDtype
from ..types.objects import (
    Array,
    DType,
    LowCardinality,
)


class ColumnInfo:
    """Column information."""

    header: bytes
    header_length: int
    total_rows: int
    column: str
    dtype: ClickhouseDtype
    is_array: bool
    is_lowcardinality: bool
    is_nullable: bool
    length: int | None
    precision: int | None
    scale: int | None
    tzinfo: str | None
    enumcase: dict[int, str] | None
    nested: int

    def __cinit__(
        self,
        total_rows: int,
        column: str,
        dtype: str,
    ):
        """Initialize from native data."""

        ...

    def make_dtype(
        self,
        fileobj: BufferedReader,
    ) -> Array | DType | LowCardinality:
        """Make dtype object."""

        ...
