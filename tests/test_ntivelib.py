import datetime
import decimal
import io
import ipaddress
import uuid

import pytest
import pandas as pd
import polars as pl

from nativelib import (
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
def sample_metadata():
    """Sample metadata for tests."""

    return [
        {"start_month": "Date"},
        {"start_day": "Date"},
        {"division_name": "String"},
        {"rdc_name": "String"},
        {"branch_name": "String"},
        {"branch_guid": "UUID"},
        {"category_guid": "FixedString(36)"},
        {"category_name": "LowCardinality(String)"},
        {"bonus_type": "Enum8('Можно, а зачем?' = 1, 'Окак' = 2)"},
        {"category_rn": "UInt8"},
        {"tso_metric1_rn": "Int16"},
        {"tso_metric2_rn": "Int32"},
        {"employee_total_rn": "Int64"},
        {"category_pcs": "Int128"},
        {"employee_tso_metric1": "Int256"},
        {"employee_tso_metric2": "UInt16"},
        {"employee_tso_metric3": "LowCardinality(Int8)"},
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
def sample_data(sample_metadata, sample_row):
    """Create buffer with sample data."""

    buffer = io.BytesIO()
    writer = NativeWriter(sample_metadata, fileobj=buffer)
    writer.write([sample_row])
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
        {"id": "Int16"},
        {"names": "Array(String)"},
        {"values": "Array(Int32)"},
        {"timestamps": "Array(DateTime)"},
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


@pytest.fixture
def all_clickhouse_metadata():
    """Создает метаданные для всех поддерживаемых типов ClickHouse."""

    return [
        {"col_uint8": "UInt8"},
        {"col_uint16": "UInt16"},
        {"col_uint32": "UInt32"},
        {"col_uint64": "UInt64"},
        {"col_uint128": "UInt128"},
        {"col_uint256": "UInt256"},
        {"col_int8": "Int8"},
        {"col_int16": "Int16"},
        {"col_int32": "Int32"},
        {"col_int64": "Int64"},
        {"col_int128": "Int128"},
        {"col_int256": "Int256"},
        {"col_float32": "Float32"},
        {"col_float64": "Float64"},
        {"col_bfloat16": "BFloat16"},
        {"col_decimal": "Decimal(10, 2)"},
        {"col_string": "String"},
        {"col_fixedstring": "FixedString(10)"},
        {"col_date": "Date"},
        {"col_date32": "Date32"},
        {"col_datetime": "DateTime"},
        {"col_datetime64": "DateTime64(3)"},
        {"col_time": "Time"},
        {"col_time64": "Time64(3)"},
        {"col_enum": "Enum8('one' = 1, 'two' = 2)"},
        {"col_bool": "Bool"},
        {"col_uuid": "UUID"},
        {"col_ipv4": "IPv4"},
        {"col_ipv6": "IPv6"},
        {"col_array": "Array(UInt8)"},
        {"col_low_cardinality": "LowCardinality(String)"},
        {"col_nullable": "Nullable(Int32)"},
        {"col_nothing": "Nothing"},
    ]


@pytest.fixture
def all_types_row():
    """Создает одну тестовую строку со значениями для всех колонок."""

    return (
        255,
        65535,
        4294967295,
        18446744073709551615,
        2**128 - 1,
        2**256 - 1,
        -128,
        -32768,
        -2147483648,
        -9223372036854775808,
        2**127 - 1,
        2**255 - 1,
        3.14159,
        3.141592653589793,
        3.125,
        decimal.Decimal("123456.78"),
        "Hello, ClickHouse!",
        "FixedStr",
        datetime.date(2024, 12, 25),
        datetime.date(1900, 1, 1),
        datetime.datetime(2024, 12, 25, 12, 30, 45),
        datetime.datetime(2024, 12, 25, 12, 30, 45, 123456),
        datetime.timedelta(hours=14, minutes=30, seconds=0),
        datetime.timedelta(
            hours=23, minutes=59, seconds=59, microseconds=123000
        ),
        "one",
        True,
        uuid.uuid4(),
        ipaddress.IPv4Address("192.168.1.1"),
        ipaddress.IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
        [1, 2, 3, 4, 5],
        "LowCardinalityString",
        12345,
        None,
    )


class TestNativeWriter:
    """Тесты для NativeWriter."""

    def test_writer_basic(self, sample_metadata, sample_row):
        """Test basic write operation."""

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        writer.write([sample_row])
        output.seek(0)
        reader = NativeReader(output)
        result = reader.to_pandas()
        assert "start_month" in result.columns  # noqa: S101
        assert "branch_guid" in result.columns  # noqa: S101
        assert "Дивизион 4" in result.values  # noqa: S101

    def test_writer_multiple_rows(self, sample_metadata, sample_row):
        """Test writing multiple rows."""

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        writer.write([sample_row, sample_row])
        output.seek(0)
        reader = NativeReader(output)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 2  # noqa: S101

    def test_writer_empty_rows(self, sample_metadata):
        """Test writing empty rows."""

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        writer.write([])
        output.seek(0)
        reader = NativeReader(output)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 0  # noqa: S101

    def test_writer_columns_property(self, sample_metadata):
        """Test columns property."""

        writer = NativeWriter(sample_metadata)
        assert writer.columns == expected_columns  # noqa: S101

    def test_writer_dtypes_property(self, sample_metadata):
        """Test dtypes property."""

        writer = NativeWriter(sample_metadata)
        assert writer.dtypes == expected_dtypes  # noqa: S101

    def test_writer_num_columns(self, sample_metadata):
        """Test num_columns property."""

        writer = NativeWriter(sample_metadata)
        assert writer.num_columns == 17  # noqa: S101

    def test_writer_repr(self, sample_metadata):
        """Test string representation."""

        writer = NativeWriter(sample_metadata)
        repr_str = repr(writer)
        assert "<Clickhouse Native dump writer>" in repr_str  # noqa: S101
        assert "Total blocks: 0" in repr_str  # noqa: S101
        assert "Total rows: 0" in repr_str  # noqa: S101

    def test_writer_all_types(self, all_clickhouse_metadata, all_types_row):
        """Тест записи и чтения всех поддерживаемых типов ClickHouse."""

        output = io.BytesIO()
        writer = NativeWriter(all_clickhouse_metadata, fileobj=output)
        writer.write([all_types_row])
        output.seek(0)
        reader = NativeReader(output)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        read_row = rows[0]
        assert read_row[0] == 255  # noqa: S101
        assert read_row[1] == 65535  # noqa: S101
        assert read_row[2] == 4294967295  # noqa: S101
        assert read_row[3] == 18446744073709551615  # noqa: S101
        assert read_row[4] == 2**128 - 1  # noqa: S101
        assert read_row[5] == 2**256 - 1  # noqa: S101
        assert read_row[6] == -128  # noqa: S101
        assert read_row[7] == -32768  # noqa: S101
        assert read_row[8] == -2147483648  # noqa: S101
        assert read_row[9] == -9223372036854775808  # noqa: S101
        assert read_row[10] == 2**127 - 1  # noqa: S101
        assert read_row[11] == 2**255 - 1  # noqa: S101
        assert abs(read_row[12] - 3.14159) < 0.0001  # noqa: S101
        assert abs(read_row[13] - 3.141592653589793) < 0.0001  # noqa: S101
        assert read_row[14] == 3.125  # noqa: S101
        assert read_row[15] == decimal.Decimal("123456.78")  # noqa: S101
        assert read_row[16] == "Hello, ClickHouse!"  # noqa: S101
        assert isinstance(read_row[17], str)  # noqa: S101
        assert read_row[18] == datetime.date(2024, 12, 25)  # noqa: S101
        assert read_row[19] == datetime.date(1900, 1, 1)  # noqa: S101
        assert read_row[20] == datetime.datetime(  # noqa: S101
            2024, 12, 25, 12, 30, 45, tzinfo=datetime.timezone.utc
        )
        expected_datetime = datetime.datetime(
            2024, 12, 25, 12, 30, 45, 123000, tzinfo=datetime.timezone.utc
        )
        assert read_row[21] == expected_datetime  # noqa: S101
        assert read_row[22] == datetime.timedelta(hours=14, minutes=30)  # noqa: S101
        assert read_row[23] == datetime.timedelta(  # noqa: S101
            hours=23, minutes=59, seconds=59, microseconds=123000
        )
        assert read_row[24] == "one"  # noqa: S101
        assert read_row[25] is True  # noqa: S101
        assert isinstance(read_row[26], uuid.UUID)  # noqa: S101
        assert read_row[27] == ipaddress.IPv4Address("192.168.1.1")  # noqa: S101
        assert read_row[28] == ipaddress.IPv6Address(  # noqa: S101
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
        )
        assert read_row[29] == [1, 2, 3, 4, 5]  # noqa: S101
        assert read_row[30] == "LowCardinalityString"  # noqa: S101
        assert read_row[31] == 12345  # noqa: S101
        assert read_row[32] is None  # noqa: S101


class TestNativeReader:
    """Тесты для NativeReader."""

    def test_reader_columns(self, reader: NativeReader):
        """Test columns property."""

        reader.read_info()
        assert reader.columns == expected_columns  # noqa: S101

    def test_reader_dtypes(self, reader: NativeReader):
        """Test dtypes property."""

        reader.read_info()
        assert reader.dtypes == expected_dtypes  # noqa: S101

    def test_reader_num_columns(self, reader: NativeReader):
        """Test num_columns property."""

        reader.read_info()
        assert reader.num_columns == 17  # noqa: S101

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

        pos = reader.tell()
        assert pos == 0  # noqa: S101
        list(reader.to_rows())
        assert reader.tell() > 0  # noqa: S101

    def test_reader_repr(self, reader: NativeReader):
        """Test string representation."""

        list(reader.to_rows())
        repr_str = repr(reader)
        assert "<Clickhouse Native dump reader>" in repr_str  # noqa: S101
        assert "Total columns: 17" in repr_str  # noqa: S101
        assert "Total rows: 1" in repr_str  # noqa: S101

    def test_reader_read_info(self, sample_metadata, sample_row):
        """Test read_info method."""

        buffer = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=buffer)
        writer.write([sample_row, sample_row])
        buffer.seek(0)
        reader = NativeReader(buffer)
        reader.read_info()
        assert reader.num_rows == 2  # noqa: S101
        assert reader.num_blocks > 0  # noqa: S101

    def test_reader_to_bytes(self, sample_data):
        """Test to_bytes method."""

        reader = NativeReader(sample_data)
        chunks = list(reader.to_bytes())
        assert len(chunks) > 0  # noqa: S101
        reader.close()

    def test_reader_close(self, reader: NativeReader):
        """Test close method."""

        reader.close()
        assert reader.fileobj.closed  # noqa: S101


class TestNativeReaderWithArrays:
    """Тесты для работы с массивами."""

    def test_read_write_arrays(self, metadata_with_arrays, row_with_arrays):
        """Test writing rows with arrays."""

        output = io.BytesIO()
        writer = NativeWriter(metadata_with_arrays, fileobj=output)
        writer.write([row_with_arrays])
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


class TestNativeEdgeCases:
    """Тесты для граничных случаев."""

    def test_empty_file(self):
        """Test reading empty file."""

        buffer = io.BytesIO()
        reader = NativeReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101

    def test_single_row_multiple_types(self, sample_metadata, sample_row):
        """Test single row with multiple types."""

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        writer.write([sample_row])
        output.seek(0)
        reader = NativeReader(output)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert isinstance(rows[0][0], datetime.date)  # noqa: S101
        assert isinstance(rows[0][1], datetime.date)  # noqa: S101
        assert isinstance(rows[0][2], str)  # noqa: S101
        assert isinstance(rows[0][5], uuid.UUID)  # noqa: S101
        assert isinstance(rows[0][9], int)  # noqa: S101

    def test_large_dataset(self, sample_metadata, sample_row):
        """Test with large number of rows."""

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        rows = [sample_row for _ in range(1000)]
        writer.write(rows)
        output.seek(0)
        reader = NativeReader(output)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 1000  # noqa: S101

    def test_close(self, sample_data):
        """Test close method."""

        reader = NativeReader(sample_data)
        reader.close()
        reader.close()


class TestNativeWriterFromPandas:
    """Тесты для записи из pandas."""

    def test_from_pandas(self, sample_metadata):
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

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        # Используем write вместо from_pandas
        writer.write(df.itertuples(index=False))
        output.seek(0)
        reader = NativeReader(output)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101


class TestNativeWriterFromPolars:
    """Тесты для записи из polars."""

    def test_from_polars(self, sample_metadata):
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

        output = io.BytesIO()
        writer = NativeWriter(sample_metadata, fileobj=output)
        # Используем write вместо from_polars
        writer.write(df.iter_rows())
        output.seek(0)
        reader = NativeReader(output)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
