from nativelib.core.length cimport (
    read_length,
    write_length,
)
from nativelib.core.columns.column cimport Column
from nativelib.core.types.functions.strings cimport read_string


cdef class BlockReader:
    """Read block from Native format."""

    def __init__(
        self,
        object fileobj,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.total_columns = 0
        self.total_rows = 0
        self.column_list = []
        self.columns = []

    cdef void read_column(self):
        """Read single column."""

        cdef str column = read_string(self.fileobj)
        cdef str dtype = read_string(self.fileobj)
        cdef object column_obj = Column(
            fileobj=self.fileobj,
            total_rows=self.total_rows,
            column=column,
            dtype=dtype,
        )

        column_obj.read()
        self.column_list.append(column_obj)
        self.columns.append(column_obj.column)

    cpdef unsigned long long skip(self):
        """Skip block."""

        cdef int _i
        cdef str column, dtype
        cdef object column_obj

        self.total_columns = read_length(self.fileobj)
        self.total_rows = read_length(self.fileobj)
        self.column_list.clear()
        self.columns.clear()

        for _i in range(self.total_columns):
            column = read_string(self.fileobj)
            dtype = read_string(self.fileobj)
            column_obj = Column(
                fileobj=self.fileobj,
                total_rows=self.total_rows,
                column=column,
                dtype=dtype,
            )
            column_obj.skip()
            self.column_list.append(column_obj)
            self.columns.append(column_obj.column)

        return self.total_rows

    cpdef object read(self):
        """Read block into python rows."""

        cdef int _i

        self.total_columns = read_length(self.fileobj)
        self.total_rows = read_length(self.fileobj)
        self.column_list.clear()
        self.columns.clear()

        for _i in range(self.total_columns):
            self.read_column()

        return zip(*self.column_list)

    cpdef bytes to_bytes(self, object with_header=True):
        """Read column as bytes."""

        cdef int _i
        cdef str column, dtype
        cdef bytearray block = bytearray()

        self.total_columns = read_length(self.fileobj)
        block.extend(write_length(self.total_columns))
        self.total_rows = read_length(self.fileobj)
        block.extend(write_length(self.total_rows))
        self.column_list.clear()
        self.columns.clear()

        for _i in range(self.total_columns):
            column = read_string(self.fileobj)
            dtype = read_string(self.fileobj)
            column_obj = Column(
                fileobj=self.fileobj,
                total_rows=self.total_rows,
                column=column,
                dtype=dtype,
            )
            block.extend(column_obj.to_bytes())
            self.column_list.append(column_obj)
            self.columns.append(column_obj.column)

        return bytes(block)
