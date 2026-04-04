# NativeLib

## Library for working with ClickHouse Native Format

Description of the format on the [official website](https://clickhouse.com/docs/en/interfaces/formats#native):

```quote
The most efficient format. Data is written and read by blocks in binary format.
For each block, the number of rows, number of columns, column names and types,
and parts of columns in this block are recorded one after another. In other words,
this format is “columnar” – it does not convert columns to rows.
This is the format used in the native interface for interaction between servers,
for using the command-line client, and for C++ clients.

You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS.
It does not make sense to work with this format yourself.
```

This library allows for data exchange between ClickHouse Native Format and Python objects, pandas.DataFrame, and polars.DataFrame.

## Features

- **Read/write ClickHouse Native format** with full type support
- **Stream processing** with block-based reading/writing
- **Pandas and Polars integration** for easy data manipulation
- **Memory efficient** - processes data in chunks
- **Full type support** for all basic ClickHouse types

## Supported Data Types

| ClickHouse Data Type | Read | Write | Python Data Type |
|:---------------------|:----:|:-----:|:-----------------|
| UInt8, UInt16, UInt32, UInt64, UInt128, UInt256 | + | + | int |
| Int8, Int16, Int32, Int64, Int128, Int256 | + | + | int |
| Float32, Float64, BFloat16 | + | + | float |
| Decimal(P, S) | + | + | decimal.Decimal |
| String | + | + | str |
| FixedString(N) | + | + | str |
| Date, Date32 | + | + | datetime.date |
| DateTime, DateTime64 | + | + | datetime.datetime |
| Time, Time64 | + | + | datetime.timedelta |
| Enum | + | + | str |
| Bool | + | + | bool |
| UUID | + | + | uuid.UUID |
| IPv4 | + | + | ipaddress.IPv4Address |
| IPv6 | + | + | ipaddress.IPv6Address |
| Array(T) | + | + | list[T] |
| LowCardinality(T) | + | + | T |
| Nullable(T) | + | + | Optional[T] |
| Nothing | + | + | None |

## Unsupported Data Types

The following ClickHouse types are not yet supported:

- Tuple
- Map
- Variant
- AggregateFunction
- SimpleAggregateFunction
- Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon
- Expression
- Set
- Domains
- Nested
- Dynamic
- JSON

## Installation

### From pip

```bash
pip install nativelib -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From local directory

```bash
pip install .
```

### From git

```bash
pip install git+https://github.com/dns-technologies/nativelib
```

## Usage Examples

### Basic Write Operation

```python
import datetime
import uuid
import io
from nativelib import NativeWriter

metadata = [
    {"id": "UInt32"},
    {"name": "String"},
    {"created_at": "DateTime"},
]

row = (
    1,
    "Alice",
    datetime.datetime(2024, 12, 25, 12, 30, 45),
)

buffer = io.BytesIO()
writer = NativeWriter(metadata, fileobj=buffer)
writer.write([row])
buffer.seek(0)
```

### Basic Read Operation

```python
from nativelib import NativeReader

reader = NativeReader(buffer)

# Read metadata without data
reader.read_info()
print(reader.columns)  # ['id', 'name', 'created_at']
print(reader.dtypes)   # ['UInt32', 'String', 'DateTime']

# Read all rows
for row in reader.to_rows():
    print(row)

reader.close()
```

### Convert to pandas DataFrame

```python
import pandas as pd

reader = NativeReader(buffer)
df = reader.to_pandas()
print(df.head())
reader.close()
```

### Convert to polars DataFrame

```python
import polars as pl

reader = NativeReader(buffer)
df = reader.to_polars()
print(df.head())
reader.close()

# Lazy mode for large datasets
lf = reader.to_polars(is_lazy=True)
df = lf.collect()
```

### Write from pandas DataFrame

```python
import pandas as pd
import datetime

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [
        datetime.datetime(2024, 12, 25, 12, 30, 45),
        datetime.datetime(2024, 12, 26, 10, 15, 30),
        datetime.datetime(2024, 12, 27, 9, 0, 0),
    ],
})

# Generate metadata from DataFrame columns
metadata = [{"id": "UInt32"}, {"name": "String"}, {"created_at": "DateTime"}]

buffer = io.BytesIO()
writer = NativeWriter(metadata, fileobj=buffer)
writer.write(df.itertuples(index=False))
buffer.seek(0)
```

### Write from polars DataFrame

```python
import polars as pl

pl_df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [
        datetime.datetime(2024, 12, 25, 12, 30, 45),
        datetime.datetime(2024, 12, 26, 10, 15, 30),
        datetime.datetime(2024, 12, 27, 9, 0, 0),
    ],
})

writer = NativeWriter(metadata, fileobj=buffer)
writer.write(pl_df.iter_rows())
```

### Working with Arrays

```python
metadata_with_arrays = [
    {"id": "UInt32"},
    {"tags": "Array(String)"},
    {"scores": "Array(Float64)"},
]

row_with_arrays = (
    1,
    ["python", "clickhouse", "data"],
    [95.5, 87.0, 92.3],
)

buffer = io.BytesIO()
writer = NativeWriter(metadata_with_arrays, fileobj=buffer)
writer.write([row_with_arrays])
```

### Reading Block Info

```python
reader = NativeReader(buffer)
reader.read_info()
print(f"Total rows: {reader.num_rows}")
print(f"Total blocks: {reader.num_blocks}")
```

### Streaming Read

```python
reader = NativeReader(buffer)
for chunk in reader.to_bytes():
    # Process chunk of raw bytes
    process(chunk)
reader.close()
```

## API Reference

### NativeReader

**Constructor:**
```python
NativeReader(fileobj: BufferedReader)
```

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `columns` | `list[str]` | Column names |
| `dtypes` | `list[str]` | Column data types |
| `num_columns` | `int` | Number of columns |
| `metadata` | `list[dict[str, str]]` | Column metadata |
| `num_rows` | `int` | Total rows read (after reading) |
| `num_blocks` | `int` | Total blocks read (after reading) |

**Methods:**

| Method | Description |
|--------|-------------|
| `read_info()` | Read metadata and count rows without reading data |
| `to_rows()` | Generator yielding Python rows |
| `to_pandas()` | Convert to pandas DataFrame |
| `to_polars(is_lazy=False)` | Convert to polars DataFrame or LazyFrame |
| `to_bytes()` | Generator yielding raw bytes chunks |
| `tell()` | Return current file position |
| `close()` | Close file object |

### NativeWriter

**Constructor:**
```python
NativeWriter(
    metadata: list[dict[str, str]],
    fileobj: BufferedWriter | None = None,
    block_size: int = Size.DEFAULT_BLOCK_SIZE,
)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `metadata` | `list[dict[str, str]]` | Column definitions (name and type) |
| `fileobj` | `BufferedWriter` | Optional file object for direct writing |
| `block_size` | `int` | Size of data blocks (default: 65536) |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `columns` | `list[str]` | Column names |
| `dtypes` | `list[str]` | Column data types |
| `num_columns` | `int` | Number of columns |
| `num_rows` | `int` | Total rows written (after writing) |
| `num_blocks` | `int` | Total blocks written (after writing) |

**Methods:**

| Method | Description |
|--------|-------------|
| `from_rows(dtype_data)` | Generator yielding native format bytes from Python rows |
| `from_pandas(data_frame)` | Generator yielding bytes from pandas DataFrame |
| `from_polars(data_frame)` | Generator yielding bytes from polars DataFrame |
| `from_bytes(bytes_data)` | Write bytes chunks to file (requires fileobj) |
| `write(rows)` | Write all rows to file (requires fileobj) |
| `tell()` | Return current file position |
| `close()` | Close file object |

## Metadata Format

Metadata is a list of dictionaries where each dictionary maps a column name to its ClickHouse data type:

```python
metadata = [
    {"column_name_1": "DataType1"},
    {"column_name_2": "DataType2"},
    # ...
]
```

Example:
```python
metadata = [
    {"id": "UInt32"},
    {"name": "String"},
    {"is_active": "Bool"},
    {"created_at": "DateTime"},
    {"tags": "Array(String)"},
]
```

## Requirements

- Python >= 3.10
- pandas >= 2.1.0 (optional, for DataFrame support)
- polars >= 0.20.31 (optional, for DataFrame support)

## License

MIT
