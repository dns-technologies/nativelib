import datetime
import io
import uuid

import pytest
import pandas as pd
import polars as pl

from nativelib import (
    Column,
    NativeReader,
    NativeWriter,
)


expected_columns = [
    "start_month",
    "start_day",
    "division_name",
    "rdc_name",
    "branch_name",
    "branch_guid",
    "category_guid",
    "category_name",
    "bonus_type",
    "category_rn",
    "tso_metric1_rn",
    "tso_metric2_rn",
    "employee_total_rn",
    "category_pcs",
    "employee_tso_metric1",
    "employee_tso_metric2",
    "employee_tso_metric3",
]
expected_pytypes = [
    "date",
    "date",
    "str",
    "str",
    "str",
    "uuid",
    "uuid",
    "str",
    "str",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
]
expected_dtypes = [
    "Date",
    "Date",
    "String",
    "String",
    "String",
    "UUID",
    "FixedString(36)",
    "LowCardinality(String)",
    "Enum8('Можно, а зачем?' = 1, 'Окак' = 2)",
    "UInt8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "UInt16",
    "LowCardinality(Int8)",
]


@pytest.fixture
def sample_columns():
    """Sample metadata for tests."""
    return [
        Column("start_month", "Date"),
        Column("start_day", "Date"),
        Column("division_name", "String"),
        Column("rdc_name", "String"),
        Column("branch_name", "String"),
        Column("branch_guid", "UUID"),
        Column("category_guid", "FixedString(36)"),
        Column("category_name", "LowCardinality(String)"),
        Column("bonus_type", "Enum8('Можно, а зачем?' = 1, 'Окак' = 2)"),
        Column("category_rn", "UInt8"),
        Column("tso_metric1_rn", "Int16"),
        Column("tso_metric2_rn", "Int32"),
        Column("employee_total_rn", "Int64"),
        Column("category_pcs", "Int128"),
        Column("employee_tso_metric1", "Int256"),
        Column("employee_tso_metric2", "UInt16"),
        Column("employee_tso_metric3", "LowCardinality(Int8)"),
    ]


@pytest.fixture
def sample_row():
    """Sample row for tests."""
    return (
        datetime.date(2026, 3, 1),
        datetime.date(2026, 3, 24),
        "Дивизион 4",
        "РРС Центр",
        "ООО Рога и копыта",
        uuid.UUID("36929e13-2a94-4810-ba49-3e41466c899f"),
        uuid.UUID("f6924c17-8c62-41e3-a10f-00155d031652"),
        "Новая Лада",
        "Можно, а зачем?",
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
    )


@pytest.fixture
def sample_data(sample_columns, sample_row):
    """Create buffer with sample data."""
    buffer = io.BytesIO()
    writer = NativeWriter(sample_columns)
    for block in writer.from_rows([sample_row]):
        buffer.write(block)
    buffer.seek(0)
    return buffer


@pytest.fixture
def reader(sample_data):
    """Create reader with sample data."""
    return NativeReader(sample_data)


@pytest.fixture
def metadata_with_arrays():
    """Metadata with list types."""
    return [
        Column("id", "Int16"),
        Column("names", "Array(String)"),
        Column("values", "Array(Int32)"),
        Column("timestamps", "Array(DateTime)"),
    ]


@pytest.fixture
def row_with_arrays():
    """Row with list data."""
    return (
        1,
        ["John", "Jane", "Bob"],
        [10, 20, 30],
        [
            datetime.datetime(2024, 10, 1, 10, 0, 0),
            datetime.datetime(2024, 10, 2, 14, 30, 0),
        ],
    )


class TestNativeWriter:
    """Тесты для NativeWriter."""

    def test_writer_basic(self, sample_columns, sample_row):
        """Test basic write operation."""

        output = io.BytesIO()
        writer = NativeWriter(sample_columns)
        blocks = list(writer.from_rows([sample_row]))

        for block in blocks:
            output.write(block)

        output.seek(0)
        reader = NativeReader(output)
        result = reader.to_pandas()
        assert "start_month" in result.columns  # noqa: S101
        assert "branch_guid" in result.columns  # noqa: S101
        assert "Дивизион 4" in result.values  # noqa: S101
        assert "Новая Лада" in result.values  # noqa: S101

    def test_writer_multiple_rows(self, sample_columns, sample_row):
        """Test writing multiple rows."""

        output = io.BytesIO()
        writer = NativeWriter(sample_columns)
        rows = [sample_row, sample_row]
        blocks = list(writer.from_rows(rows))

        for block in blocks:
            output.write(block)

        output.seek(0)
        reader = NativeReader(output)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 2  # noqa: S101

    def test_writer_empty_rows(self, sample_columns):
        """Test writing empty rows."""

        writer = NativeWriter(sample_columns)
        blocks = list(writer.from_rows([]))
        assert len(blocks) == 0  # noqa: S101

    def test_writer_columns_property(self, sample_columns):
        """Test columns property."""

        writer = NativeWriter(sample_columns)
        assert [col.column for col in writer.column_list] == expected_columns  # noqa: S101

    def test_writer_repr(self, sample_columns):
        """Test string representation."""

        writer = NativeWriter(sample_columns)
        repr_str = repr(writer)
        assert "<Clickhouse Native dump writer>" in repr_str  # noqa: S101
        assert "Total blocks: 0" in repr_str  # noqa: S101
        assert "Total rows: 0" in repr_str  # noqa: S101


class TestNativeReader:
    """Тесты для NativeReader."""

    def test_reader_columns(self, reader: NativeReader):
        """Test columns property."""

        reader.read_info()
        assert [  # noqa: S101
            col.column for col in reader.block_reader.column_list
        ] == expected_columns

    def test_reader_to_rows(self, reader: NativeReader, sample_row):
        """Test to_rows method."""

        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == sample_row[0]  # noqa: S101
        assert rows[0][2] == sample_row[2]  # noqa: S101
        assert rows[0][5] == sample_row[5]  # noqa: S101
        assert rows[0][7] == sample_row[7]  # noqa: S101

    def test_reader_to_pandas(self, reader: NativeReader):
        """Test to_pandas method."""

        df = reader.to_pandas()
        assert len(df) == 1  # noqa: S101
        assert "start_month" in df.columns  # noqa: S101
        assert df.iloc[0]["division_name"] == "Дивизион 4"  # noqa: S101

    def test_reader_to_polars(self, reader: NativeReader):
        """Test to_polars method."""

        df = reader.to_polars()
        assert len(df) == 1  # noqa: S101
        assert "start_month" in df.columns  # noqa: S101
        assert df["division_name"][0] == "Дивизион 4"  # noqa: S101

    def test_reader_to_polars_lazy(self, reader: NativeReader):
        """Test to_polars lazy mode."""

        lf = reader.to_polars(is_lazy=True)
        assert hasattr(lf, "collect")  # noqa: S101
        df = lf.collect()
        assert len(df) == 1  # noqa: S101

    def test_reader_tell(self, reader: NativeReader):
        """Test tell method."""

        reader.read_info()
        pos = reader.tell()
        assert pos > 0  # noqa: S101

    def test_reader_repr(self, reader: NativeReader):
        """Test string representation."""

        reader.read_info()
        repr_str = repr(reader)
        assert "<Clickhouse Native dump reader>" in repr_str  # noqa: S101
        assert "Total columns: 17" in repr_str  # noqa: S101
        assert "Total rows: 1" in repr_str  # noqa: S101

    def test_reader_read_info(self, sample_columns, sample_row):
        """Test read_info method."""

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns)

        for block in writer.from_rows([sample_row, sample_row]):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        reader.read_info()
        assert reader.total_rows == 2  # noqa: S101
        assert reader.total_blocks > 0  # noqa: S101

    def test_reader_multiple_blocks(self, sample_columns, sample_row):
        """Test reading data split into multiple blocks."""

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns, block_size=8192)
        rows = [sample_row for _ in range(100)]

        for block in writer.from_rows(rows):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 100  # noqa: S101


class TestNativeReaderWithArrays:
    """Тесты для работы с массивами."""

    def test_write_list(self, metadata_with_arrays, row_with_arrays):
        """Test writing rows with lists."""

        output = io.BytesIO()
        writer = NativeWriter(metadata_with_arrays)

        for block in writer.from_rows([row_with_arrays]):
            output.write(block)

        output.seek(0)
        reader = NativeReader(output)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] == ["John", "Jane", "Bob"]  # noqa: S101
        assert rows[0][2] == [10, 20, 30]  # noqa: S101
        assert rows[0][3][0] == datetime.datetime(  # noqa: S101
            2024, 10, 1, 10, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_read_write_list_roundtrip(
        self, metadata_with_arrays, row_with_arrays
    ):
        """Test roundtrip with lists."""

        buffer = io.BytesIO()
        writer = NativeWriter(metadata_with_arrays)

        for block in writer.from_rows([row_with_arrays]):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] == ["John", "Jane", "Bob"]  # noqa: S101
        assert rows[0][2] == [10, 20, 30]  # noqa: S101
        assert rows[0][3][0] == datetime.datetime(  # noqa: S101
            2024, 10, 1, 10, 0, 0, tzinfo=datetime.timezone.utc
        )
        assert rows[0][3][1] == datetime.datetime(  # noqa: S101
            2024, 10, 2, 14, 30, 0, tzinfo=datetime.timezone.utc
        )


class TestNativeEdgeCases:
    """Тесты для граничных случаев."""

    def test_empty_file(self):
        """Test reading empty file."""

        buffer = io.BytesIO()
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101

    def test_single_row_multiple_types(self, sample_columns, sample_row):
        """Test single row with multiple types."""

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns)

        for block in writer.from_rows([sample_row]):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert isinstance(rows[0][0], datetime.date)  # noqa: S101
        assert isinstance(rows[0][1], datetime.date)  # noqa: S101
        assert isinstance(rows[0][2], str)  # noqa: S101
        assert isinstance(rows[0][3], str)  # noqa: S101
        assert isinstance(rows[0][4], str)  # noqa: S101
        assert isinstance(rows[0][5], uuid.UUID)  # noqa: S101
        assert isinstance(rows[0][6], str)  # noqa: S101
        assert isinstance(rows[0][7], str)  # noqa: S101
        assert isinstance(rows[0][8], str)  # noqa: S101
        assert isinstance(rows[0][9], int)  # noqa: S101
        assert isinstance(rows[0][10], int)  # noqa: S101
        assert isinstance(rows[0][11], int)  # noqa: S101
        assert isinstance(rows[0][12], int)  # noqa: S101
        assert isinstance(rows[0][13], int)  # noqa: S101
        assert isinstance(rows[0][14], int)  # noqa: S101
        assert isinstance(rows[0][15], int)  # noqa: S101
        assert isinstance(rows[0][16], int)  # noqa: S101

    def test_large_dataset(self, sample_columns, sample_row):
        """Test with large number of rows."""

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns)
        rows = [sample_row for _ in range(1000)]

        for block in writer.from_rows(rows):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 1000  # noqa: S101

    def test_close(self, sample_data):
        """Test close method."""

        reader = NativeReader(sample_data)
        reader.close()
        # Should not raise error
        reader.close()


class TestNativeWriterFromPandas:
    """Тесты для записи из pandas."""

    def test_from_pandas(self, sample_columns):
        """Test from_pandas method."""

        df = pd.DataFrame(
            {
                "start_month": [datetime.date(2026, 3, 1)],
                "start_day": [datetime.date(2026, 3, 24)],
                "division_name": ["Дивизион 4"],
                "rdc_name": ["РРС Центр"],
                "branch_name": ["ООО Рога и копыта"],
                "branch_guid": [
                    uuid.UUID("36929e13-2a94-4810-ba49-3e41466c899f")
                ],
                "category_guid": [
                    uuid.UUID("f6924c17-8c62-41e3-a10f-00155d031652")
                ],
                "category_name": ["Новая Лада"],
                "bonus_type": ["Можно, а зачем?"],
                "category_rn": [1],
                "tso_metric1_rn": [2],
                "tso_metric2_rn": [3],
                "employee_total_rn": [4],
                "category_pcs": [5],
                "employee_tso_metric1": [6],
                "employee_tso_metric2": [7],
                "employee_tso_metric3": [8],
            }
        )

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns)

        for block in writer.from_pandas(df):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())

        assert len(rows) == 1  # noqa: S101


class TestNativeWriterFromPolars:
    """Тесты для записи из polars."""

    def test_from_polars(self, sample_columns):
        """Test from_polars method."""

        df = pl.DataFrame(
            {
                "start_month": [datetime.date(2026, 3, 1)],
                "start_day": [datetime.date(2026, 3, 24)],
                "division_name": ["Дивизион 4"],
                "rdc_name": ["РРС Центр"],
                "branch_name": ["ООО Рога и копыта"],
                "branch_guid": [
                    uuid.UUID("36929e13-2a94-4810-ba49-3e41466c899f")
                ],
                "category_guid": [
                    uuid.UUID("f6924c17-8c62-41e3-a10f-00155d031652")
                ],
                "category_name": ["Новая Лада"],
                "bonus_type": ["Можно, а зачем?"],
                "category_rn": [1],
                "tso_metric1_rn": [2],
                "tso_metric2_rn": [3],
                "employee_total_rn": [4],
                "category_pcs": [5],
                "employee_tso_metric1": [6],
                "employee_tso_metric2": [7],
                "employee_tso_metric3": [8],
            }
        )

        buffer = io.BytesIO()
        writer = NativeWriter(sample_columns)

        for block in writer.from_polars(df):
            buffer.write(block)

        buffer.seek(0)
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())

        assert len(rows) == 1  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
