EMPTY_LINE = "├─────────────────┼─────────────────┤"
END_LINE = "└─────────────────┴─────────────────┘"
HEADER_LINES = [
    "┌─────────────────┬─────────────────┐",
    "│   Column Name   │    Data Type    │",
    "╞═════════════════╪═════════════════╡",
]


def to_col(text: str) -> str:
    """Format string element."""

    return text[:14] + "…" if len(text) > 15 else text


def table_repr(
    columns: list[str],
    dtypes: list[str],
    header: str | None = None,
    tail: list[str] | None = None,
) -> str:
    """Generate table for string representation."""

    table = [
        header,
        *HEADER_LINES,
    ] if header else HEADER_LINES

    for column, dtype in zip(columns, dtypes):
        table.extend([
            f"│ {to_col(column): <15} │ {to_col(dtype): >15} │",
            EMPTY_LINE,
        ])

    table[-1] = END_LINE

    if tail:
        table.extend(tail)

    return "\n".join(table)


def nativelib_repr(
    columns: list[str],
    dtypes: list[str],
    num_columns: int,
    num_rows: int,
    num_blocks: int,
    object_type: str,
) -> str:
    """Generate string representation for NativeReader/NativeWriter."""

    return table_repr(
        columns,
        dtypes,
        f"<Clickhouse Native dump {object_type}>",
        [
            f"Total columns: {num_columns}",
            f"Total rows: {num_rows}",
            f"Total blocks: {num_blocks}",
        ],
    )
