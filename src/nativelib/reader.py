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
    num_blocks: int
    num_rows: int
    _columns: list[str] | None
    _dtypes: list[str] | None
    _metadata: list[dict[str, str]]

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.block_reader = BlockReader(self.fileobj)
        self.num_blocks = 0
        self.num_rows = 0
        self._columns = None
        self._dtypes = None
        self._metadata = []

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        if not self._columns:
            self._columns = self.block_reader.columns

        return self._columns

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        if not self._dtypes:
            self._dtypes = [
                column.string_dtype
                for column in self.block_reader.column_list
            ]

        return self._dtypes

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.columns)

    @property
    def metadata(self) -> list[dict[str, str]]:
        """Generate metadata."""

        if not self._metadata:
            self._metadata = [
                {column: dtype}
                for column, dtype in zip(self.columns, self.dtypes)
            ]

        return self._metadata

    def read_info(self) -> None:
        """Read info without reading data."""

        try:
            while 1:
                self.num_rows += self.block_reader.skip()
                self.num_blocks += 1
        except IndexError:
            """End of file."""

    def read_block(self) -> tuple[Any, ...]:
        """Read single block as python values."""

        rows = tuple(self.block_reader.read())
        self.num_rows += len(rows)
        self.num_blocks += 1
        return rows

    def to_rows(self) -> Generator[Any, None, None]:
        """Convert to python rows."""

        try:
            while 1:
                for row in self.block_reader.read():
                    yield row
                    self.num_rows += 1
                self.num_blocks += 1
        except IndexError:
            """End of file."""

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            self.to_rows(),
            columns=self.columns,
        ).astype(pandas_astype(self.block_reader.column_list))

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            self.to_rows(),
            schema=self.columns,
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

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw unpacked data for block as bytes."""

        try:
            while block := self.block_reader.to_bytes():
                yield block
                self.num_rows += self.block_reader.total_rows
                self.num_blocks += 1
        except IndexError:
            """End of file."""

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
            self.columns,
            self.dtypes,
            self.num_columns,
            self.num_rows,
            self.num_blocks,
            "reader",
        )
