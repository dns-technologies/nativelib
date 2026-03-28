from collections.abc import Generator
from typing import (
    Any,
    Iterable,
    Iterator,
)

from .. import Size
from ..columns import Column


class BlockWriter:
    """Write block into Native format."""

    column_list: list[Column]
    max_block_size: int
    total_columns: int
    total_rows: int
    block_size: int
    headers_size: int
    data_iterator: Iterator[Any] | None

    def __init__(
        self,
        column_list: list[Column],
        max_block_size: int = Size.DEFAULT_BLOCK_SIZE,
    ) -> None:
        """Class initialization."""

        ...

    def write_row(self) -> None:
        """Write single row."""

        ...

    def clear_block(self) -> bytes:
        """Return block bytes and clear buffers."""

        ...

    def init_dataset(
        self,
        dtype_values: Iterable[Any],
    ) -> None:
        """Init dataset."""

        ...

    def write(self) -> Generator[bytes, None, None]:
        """Write from rows."""

        ...
