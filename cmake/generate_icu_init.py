#!/usr/bin/env python3
"""Run SCons' unchanged ICU-data generator with Python 3 byte/text semantics."""

import builtins
import runpy
import sys


def generator_open(path, mode="r", *args, **kwargs):
    # Keep the .dat input binary, but Python 2's generated C++ output is a str.
    if mode == "wb":
        return builtins.open(path, "w", encoding="utf-8", newline="\n")
    return builtins.open(path, mode, *args, **kwargs)


def byte_value(value):
    return value if isinstance(value, int) else ord(value)


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: generate_icu_init.py SCRIPT -i DATA -o OUTPUT")
    sys.argv = sys.argv[1:]
    runpy.run_path(sys.argv[0], run_name="__main__",
                  init_globals={"open": generator_open, "ord": byte_value})


if __name__ == "__main__":
    main()
