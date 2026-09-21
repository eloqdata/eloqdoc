#!/usr/bin/env python3
"""Run MongoDB's Python 2 action-type generator for the CMake build."""

import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: generate_action_types.py SCRIPT [ARG ...]")

    script = Path(sys.argv[1])
    source = script.read_text(encoding="utf-8")
    replacements = {
        "            print 'Duplicate actionType %s\\n' % actionType":
            "            print('Duplicate actionType %s\\n' % actionType)",
        '        print "Usage: generate_action_types.py <path to action_types.txt> '
        '<header file path> <source file path>"':
            '        print("Usage: generate_action_types.py <path to action_types.txt> '
            '<header file path> <source file path>")',
    }
    for old, new in replacements.items():
        if source.count(old) != 1:
            raise RuntimeError("unexpected action-type generator format")
        source = source.replace(old, new)

    sys.argv = sys.argv[1:]
    globals_ = {"__file__": str(script), "__name__": "__main__"}
    exec(compile(source, str(script), "exec"), globals_)


if __name__ == "__main__":
    main()
