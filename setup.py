from setuptools import (
    Extension,
    setup,
)
from Cython.Build import cythonize


extensions = [
    Extension(
        "nativelib.common.casts",
        ["src/nativelib/common/casts.pyx"],
    ),
    Extension(
        "nativelib.core.blocks.reader",
        ["src/nativelib/core/blocks/reader.pyx"],
    ),
    Extension(
        "nativelib.core.blocks.writer",
        ["src/nativelib/core/blocks/writer.pyx"],
    ),
    Extension(
        "nativelib.core.columns.column",
        ["src/nativelib/core/columns/column.pyx"],
    ),
    Extension(
        "nativelib.core.columns.info",
        ["src/nativelib/core/columns/info.pyx"],
    ),
    Extension(
        "nativelib.core.length",
        ["src/nativelib/core/length.pyx"],
    ),
    Extension(
        "nativelib.core.parse",
        ["src/nativelib/core/parse.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.booleans",
        ["src/nativelib/core/types/functions/booleans.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.dates",
        ["src/nativelib/core/types/functions/dates.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.decimals",
        ["src/nativelib/core/types/functions/decimals.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.enums",
        ["src/nativelib/core/types/functions/enums.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.floats",
        ["src/nativelib/core/types/functions/floats.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.integers",
        ["src/nativelib/core/types/functions/integers.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.ipaddrs",
        ["src/nativelib/core/types/functions/ipaddrs.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.strings",
        ["src/nativelib/core/types/functions/strings.pyx"],
    ),
    Extension(
        "nativelib.core.types.functions.uuids",
        ["src/nativelib/core/types/functions/uuids.pyx"],
    ),
    Extension(
        "nativelib.core.types.objects.array",
        ["src/nativelib/core/types/objects/array.pyx"],
    ),
    Extension(
        "nativelib.core.types.objects.dtype",
        ["src/nativelib/core/types/objects/dtype.pyx"],
    ),
    Extension(
        "nativelib.core.types.objects.lowcardinality",
        ["src/nativelib/core/types/objects/lowcardinality.pyx"],
    ),
]

setup(
    name="nativelib",
    version="0.2.5.dev2",
    package_dir={"": "src"},
    ext_modules=cythonize(extensions, language_level="3"),
    packages=[
        "nativelib",
        "nativelib.common",
        "nativelib.core",
        "nativelib.core.blocks",
        "nativelib.core.columns",
        "nativelib.core.types",
        "nativelib.core.types.functions",
        "nativelib.core.types.objects",
    ],
    package_data={
        "nativelib": [
            "**/*.pyi",
            "*.pyi",
            "*.md",
            "*.txt",
        ]
    },
    exclude_package_data={
        "": ["*.c"],
        "nativelib": ["**/*.c"],
    },
    include_package_data=True,
    setup_requires=["Cython>=0.29.33"],
    zip_safe=False,
)
