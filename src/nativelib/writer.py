from collections.abc import (
    Generator,
    Iterable,
)
from io import BufferedWriter
from typing import Any

from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .core import (
    BlockWriter,
    Column,
    Size,
)
from .common import nativelib_repr


class NativeWriter:
    """Class for write data into native format."""

    metadata: list[dict[str, str]]
    fileobj: BufferedWriter | None
    block_size: int
    block_writer: BlockWriter
    num_blocks: int
    num_rows: int

    def __init__(
        self,
        metadata: list[dict[str, str]],
        fileobj: BufferedWriter | None = None,
        block_size: int = Size.DEFAULT_BLOCK_SIZE,
    ) -> None:
        """Class initialization."""

        self.metadata = metadata
        self.fileobj = fileobj
        self.block_size = block_size
        self.block_writer = BlockWriter(
            [
                Column(column, dtype)
                for column_values in self.metadata
                for column, dtype in column_values.items()
            ],
            block_size,
        )
        self.num_blocks = 0
        self.num_rows = 0

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        return [
            column
            for column_values in self.metadata
            for column, _ in column_values.items()
        ]

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        return [
            dtype
            for column_values in self.metadata
            for _, dtype in column_values.items()
        ]

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.columns)

    def from_rows(
        self,
        dtype_data: Iterable[Any],
    ) -> Generator[bytes, None, None]:
        """Convert python rows to native format."""

        self.block_writer.init_dataset(dtype_data)

        for block, num_rows in self.block_writer.write():
            self.num_rows += num_rows
            self.num_blocks += 1
            yield block

    def from_pandas(
        self,
        data_frame: PdFrame,
    ) -> Generator[bytes, None, None]:
        """Convert pandas.DataFrame to native format."""

        return self.from_rows(data_frame.itertuples(index=False))

    def from_polars(
        self,
        data_frame: PlFrame | LfFrame,
    ) -> Generator[bytes, None, None]:
        """Convert polars.DataFrame to native format."""

        if data_frame.__class__ is LfFrame:
            data_frame = data_frame.collect(engine="streaming")

        return self.from_rows(data_frame.iter_rows())

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
    ) -> int:
        """Write from any other native fileobj."""

        if self.fileobj is None:
            raise ValueError("File object not defined!")

        for block in bytes_data:
            self.fileobj.write(block)

        return self.tell()

    def write(self, rows: Iterable[list[Any] | tuple[Any, ...]]) -> None:
        """Write all rows into file."""

        bytes_data = self.from_rows(rows)
        self.from_bytes(bytes_data)

    def tell(self) -> int:
        """Return current position."""

        if self.fileobj:
            return self.fileobj.tell()

        return 0

    def close(self) -> None:
        """Close file object."""

        if self.fileobj:
            if hasattr(self.fileobj, "close"):
                self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of NativeWriter."""

        return nativelib_repr(
            self.columns,
            self.dtypes,
            self.num_columns,
            self.num_rows,
            self.num_blocks,
            "writer",
        )
