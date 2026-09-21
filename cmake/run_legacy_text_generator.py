#!/usr/bin/env python3
"""Run simple Python 2 text generators under Python 3.

These generators use binary output modes for strings or the removed ``U``
mode.  CMake needs Python 3, while SCons must keep executing the original
files with Python 2 and their historical I/O behavior.
"""

import builtins
import runpy
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_legacy_text_generator.py SCRIPT [ARG ...]")

    script = sys.argv[1]
    sys.argv = sys.argv[1:]
    sys.path.insert(0, str(Path(script).resolve().parent))
    original_open = builtins.open

    def text_open(path, mode="r", *args, **kwargs):
        mode = mode.replace("b", "").replace("U", "") or "r"
        return original_open(path, mode, *args, **kwargs)

    builtins.open = text_open
    runpy.run_path(script, run_name="__main__")


if __name__ == "__main__":
    main()
