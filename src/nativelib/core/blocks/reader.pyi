from collections.abc import Generator
from io import BufferedReader
from typing import Any

from ..columns import Column


class BlockReader:
    """Read block from Native format."""

    fileobj: BufferedReader
    total_columns: int
    total_rows: int
    column_list: list[Column]
    columns: list[str]

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        ...

    def read_column(self) -> None:
        """Read single column."""

        ...

    def skip(self) -> int:
        """Skip block."""

        ...

    def read(self) -> Generator[tuple[Any], None, None]:
        """Read block into python rows."""

        ...

    def to_bytes(self, with_header: bool = True) -> bytes:
        """Read block as bytes."""

        ...
