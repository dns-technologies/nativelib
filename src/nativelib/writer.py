from collections.abc import (
    Generator,
    Iterable,
)
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

    column_list: list[Column]
    block_writer: BlockWriter
    block_size: int
    total_blocks: int
    total_rows: int

    def __init__(
        self,
        column_list: list[Column],
        block_size: int = Size.DEFAULT_BLOCK_SIZE,
    ) -> None:
        """Class initialization."""

        self.column_list = column_list
        self.block_size = block_size
        self.block_writer = BlockWriter(column_list, block_size)
        self.total_blocks = 0
        self.total_rows = 0

    def from_rows(
        self,
        dtype_data: Iterable[Any],
    ) -> Generator[bytes, None, None]:
        """Convert python rows to native format."""

        self.block_writer.init_dataset(dtype_data)

        for block, total_rows in self.block_writer.write():
            self.total_rows += total_rows
            self.total_blocks += 1
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

    def __repr__(self) -> str:
        """String representation of NativeWriter."""

        return nativelib_repr(
            self.column_list,
            self.total_blocks,
            self.total_rows,
            "writer",
        )
