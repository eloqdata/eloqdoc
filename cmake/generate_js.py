#!/usr/bin/env python3
"""Execute the unchanged SCons JavaScript embedding generator using Python 3."""

import sys
from pathlib import Path


def main():
    script = Path(sys.argv[1])
    source = script.read_text(encoding="utf-8")
    replacements = {
        'print "Must specify [target] [source] "':
            'print("Must specify [target] [source] ")',
        "out.write(text)": "out.write(text.encode('utf-8'))",
    }
    for old, new in replacements.items():
        if source.count(old) != 1:
            raise RuntimeError("unexpected jstoh.py format: " + old)
        source = source.replace(old, new)
    sys.argv = sys.argv[1:]
    exec(compile(source, str(script), "exec"), {"__file__": str(script), "__name__": "__main__"})


if __name__ == "__main__":
    main()
