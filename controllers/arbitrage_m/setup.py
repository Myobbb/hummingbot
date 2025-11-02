"""
Setup file for compiling arbitrage_m Cython helpers.

Usage:
    python setup.py build_ext --inplace
"""

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import numpy as np

# Get the path to OrderBookEntry.cpp
import os
hummingbot_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
cpp_sources = [
    os.path.join(hummingbot_root, 'hummingbot/core/cpp/OrderBookEntry.cpp')
]

extensions = [
    Extension(
        "arbitrage_m_helpers",
        sources=[
            "arbitrage_m_helpers.pyx",
        ] + cpp_sources,
        include_dirs=[
            np.get_include(),
            os.path.join(hummingbot_root, 'hummingbot'),
        ],
        language="c++",
        extra_compile_args=["-Wno-psabi", "-std=c++11"],
        define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
    )
]

setup(
    name="arbitrage_m_helpers",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': '3',
            'cdivision': True,
            'boundscheck': False,
            'wraparound': False,
        }
    ),
)
