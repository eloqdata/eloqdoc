#!/usr/bin/env python3
"""Run MongoDB's Python 2 IDL compiler from the CMake/Python 3 build.

The historical SCons build invokes scripts/buildscripts/idl/idlc.py with
Python 2.  Keep that implementation untouched and provide the two small
Python 3 compatibility shims here so the build systems do not affect one
another.
"""

import argparse
import builtins
import itertools
import sys
from pathlib import Path


def escape_depfile_path(path):
    """Escape a path for Ninja/CMake's Make-style depfile parser."""
    return (str(path).replace("\\", "\\\\").replace("$", "$$")
            .replace("#", "\\#").replace(" ", "\\ ").replace(":", "\\:"))


def enable_depfile(compiler, depfile):
    """Record the imports actually opened by the unchanged SCons IDL compiler."""
    dependencies = set()
    original_compile = compiler.compile_idl
    original_open = compiler.CompilerImportResolver.open

    def tracking_open(self, resolved_file_name):
        dependencies.add(Path(resolved_file_name).resolve())
        return original_open(self, resolved_file_name)

    def compile_with_dependencies(args):
        dependencies.clear()
        dependencies.add(Path(args.input_file).resolve())
        if not original_compile(args):
            return False
        source = args.output_source or str(Path(args.input_file).with_suffix("")) + "_gen.cpp"
        depfile.write_text(
            escape_depfile_path(Path(source).resolve()) + ": "
            + " ".join(escape_depfile_path(path) for path in sorted(dependencies)) + "\n",
            encoding="utf-8")
        return True

    compiler.CompilerImportResolver.open = tracking_open
    compiler.compile_idl = compile_with_dependencies


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--depfile", type=Path)
    args, compiler_args = parser.parse_known_args()
    sys.argv[1:] = compiler_args

    idl_root = Path(__file__).resolve().parents[1] / "scripts" / "buildscripts" / "idl"
    sys.path.insert(0, str(idl_root))

    # binder.py uses Python 2's xrange.
    builtins.xrange = range

    import idlc  # pylint: disable=import-error,import-outside-toplevel
    from idl import compiler, syntax  # pylint: disable=import-error,import-outside-toplevel

    # syntax.py uses dict.viewitems(), which has no Python 3 equivalent.
    def item_and_type(values):
        return itertools.chain.from_iterable(
            syntax._zip_scalar(value, key) for key, value in values.items())

    syntax._item_and_type = item_and_type
    if args.depfile:
        enable_depfile(compiler, args.depfile)
    idlc.main()


if __name__ == "__main__":
    main()
