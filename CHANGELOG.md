# Version History

## 0.2.5.dev0

* Developer release (not public to pip)
* Improve nativelib_repr() function
* NativeReader now ReaderType protocol compatible
* NativeWriter now ReaderType protocol compatible
* Fix BlockReader.skip() method
* Update pytest
* Update README.md

## 0.2.4.dev1

* Developer release (not public to pip)
* Improve read and write Into128, Int256, UInt128 and UInt256
* Refactor NativeReader and NativeWriter parameters detector
* Add test_writer_all_types to pytest

## 0.2.4.dev0

* Developer release (not public to pip)
* Remove Cython source code from wheel package
* Decompose project
* Add NativeLibError and NativeLibValueError classes
* Add module Size
* Add Column.string_dtype attribute
* Add pytest coverage
* Refactor *.pyi
* Change NativeReader and NativeWriter `__repr__`() method

## 0.2.3.dev4

* Developer release (not public to pip)
* Update README.md
* Fix NativeReader.to_polars() for dumps with nested objects

## 0.2.3.dev3

* Developer release (not public to pip)
* Update README.md
* Change worker

## 0.2.3.dev2

* Developer release (not public to pip)
* Speed-up NativeWriter.from_pandas() method

## 0.2.3.dev1

* Developer release (not public to pip)
* Add support for read and write polars.LazyFrame
* Add optional param is_lazy to NativeReader.to_polars() method. Default is False
* Add auto detect polars.LazyFrame to NativeWriter.from_polars() method

## 0.2.3.dev0

* Developer release (not public to pip)
* Fix LowCardinality(FixedString(N)) convert from non-string Data (UUID and other)
* Fix convert Int/Uint from float and decimal.Decimal data

## 0.2.2.6

* Change documentation link
* Change project development status to 4 - Beta
* No any changes, docs only

## 0.2.2.5

* Rollback Array
* Rollback DType
* Rollback LowCardinality
* Autoconvert NaN to None for not Float data types
* Fix compile from source on unix systems

## 0.2.2.4

* Back compile depends to cython>=0.29.33
* Make wheels for python 3.10-3.14

## 0.2.2.3

* Downgrade compile depends to cython==0.29.33
* Make wheels for python 3.10 and 3.11 only

## 0.2.2.2

* Dellocate memory for unix systems
* Disable Linux Aarch64
* Refactor Array
* Refactor DType
* Refactor LowCardinality

## 0.2.2.1

* Improve functions write_time and write_time64
for support write from datetime.time object

## 0.2.2.0

* Add Time function
* Add Time64 function
* Refactor variable precission -> precision
* Update README.md

## 0.2.1.3

* Fixed conversion to pandas for null values ​​in int columns
* Fixed an issue with converting to polars for large numeric values

## 0.2.1.2

* Update python version support to 3.10-3.14
* Change str and repr column view
* Add auto upload to pip

## 0.2.1.1

* Add wheels automake
* Delete unused imports
* Improve strings functions

## 0.2.1.0

* Add *.pyi files for cython modules descriptions
* Update MANIFEST.in
* Update depends setuptools==80.9.0

## 0.2.0.7

* Fix pandas_astype

## 0.2.0.6

* Fix write datetime function
* Fix datetime cast to pandas.DataFrame
* Delete polars_schema its interferes with correct operation to_polars() method

## 0.2.0.5

* Fix ClickhouseDtype polars compatible types
* Add cast data types to integers functions
* Add cast data types to floats functions

## 0.2.0.4

* Update MANIFEST.in
* Improve pyproject.toml license file approve
* Add CHANGELOG.md to pip package
* Add close() & tell() method to NativeReader

## 0.2.0.3

* Add MIT License
* Add MANIFEST.in
* Delete tests directory. I'll adding some autotests later

## 0.2.0.2

* Improve pandas.Timestamp write errors for date & datetime write functions
* Add date to datetime & datetime to date convert
* Refactor PANDAS_TYPE ditionary
* Fix pandas.DataFrame string dtype from object to string[python]

## 0.2.0.1

* Change Enum8/Enum16 pytype to str
* Change dtype buffers from io.BytesIO objects to list
* Improve Python data type section in README.md

## 0.2.0.0

* Refactor project
* Redistribute project directories
* Translate code to Cython
* Delete unnecessary methods
* Delete unnecessary depends from requirements.txt
* Now NativeWriter is lazy and return bytes object generator
* Now NativeReader is lazy and return python object generator
* Add LowCardinality write suppord
* Add errors handling for some Data Types
* Update README.md

## 0.0.1

First version of the library nativelib

* Create metadata from native to pgpack format
* Read native format as python rows, pandas.DataFrame, polars.DataFrame and pgcopy bynary
* Write from python rows, pandas.DataFrame, polars.DataFrame and pgpack bynary into native format
