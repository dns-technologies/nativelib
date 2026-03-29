from io import BufferedReader
from typing import (
    Any,
    Generator,
)

from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
    Object,
)

from .common import (
    nativelib_repr,
    pandas_astype,
)
from .core import BlockReader


ISLAZY = {
    False: PlFrame,
    True: LfFrame,
}


class NativeReader:
    """Class for read data from native format."""

    fileobj: BufferedReader
    block_reader: BlockReader
    total_blocks: int
    total_rows: int

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.block_reader = BlockReader(self.fileobj)
        self.total_blocks = 0
        self.total_rows = 0

    def read_info(self) -> None:
        """Read info without reading data."""

        try:
            while 1:
                self.total_rows += self.block_reader.skip()
                self.total_blocks += 1
        except IndexError:
            """End of file."""

    def to_rows(self) -> Generator[Any, None, None]:
        """Convert to python rows."""

        try:
            while 1:
                for dtype_value in self.block_reader.read():
                    yield dtype_value
                    self.total_rows += 1
                self.total_blocks += 1
        except IndexError:
            """End of file."""

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            self.to_rows(),
            columns=self.block_reader.columns,
        ).astype(pandas_astype(self.block_reader.column_list))

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            self.to_rows(),
            schema=self.block_reader.columns,
            schema_overrides={
                col.column: Object
                for col in self.block_reader.column_list
                if col.info.is_array and col.info.dtype.name in (
                    "IPv4",
                    "IPv6",
                    "UUID",
                )
            },
            infer_schema_length=None,
        )

    def tell(self) -> int:
        """Return current position."""

        return self.fileobj.tell()

    def close(self) -> None:
        """Close file object."""

        if hasattr(self.fileobj, "close"):
            self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of NativeReader."""

        return nativelib_repr(
            self.block_reader.column_list,
            self.total_blocks,
            self.total_rows,
            "reader",
        )
