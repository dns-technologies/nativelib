from ..core.columns import Column

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
    column_list: list[Column],
    total_blocks: int,
    total_rows: int,
    object_type: str,
) -> str:
    """Generate string representation for NativeReader/NativeWriter."""

    return table_repr(
        [column.column for column in column_list],
        [column.string_dtype for column in column_list],
        f"<Clickhouse Native dump {object_type}>",
        [
            f"Total columns: {len(column_list)}",
            f"Total blocks: {total_blocks}",
            f"Total rows: {total_rows}",
        ],
    )
