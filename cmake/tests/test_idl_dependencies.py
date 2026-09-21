#!/usr/bin/env python3
"""Exercise the real IDL compiler and Ninja's transitive dependency tracking."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generate_idl import escape_depfile_path


class IDLDependenciesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="eloqdoc idl ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.build = self.root / "build"
        self.wrapper = Path(__file__).resolve().parents[1] / "generate_idl.py"
        leaf = '''types:
    testString:
        description: A test string
        cpp_type: std::string
        bson_serialization_type: string
        deserializer: mongo::BSONElement::str
'''
        (self.root / "leaf.idl").write_text(leaf, encoding="utf-8")
        (self.root / "other.idl").write_text(leaf, encoding="utf-8")
        (self.root / "middle.idl").write_text("imports: [leaf.idl]\n", encoding="utf-8")
        for name, imported in (("first", "middle.idl"), ("second", "other.idl")):
            (self.root / (name + ".idl")).write_text(
                "imports: [" + imported + "]\n"
                "global:\n    cpp_namespace: mongo\n"
                "structs:\n    Example:\n        description: Test struct\n"
                "        fields:\n            value: testString\n", encoding="utf-8")
        (self.root / "CMakeLists.txt").write_text('''
cmake_minimum_required(VERSION 3.24)
project(IDLDependencies NONE)
foreach(name IN ITEMS first second)
    set(output "${CMAKE_CURRENT_BINARY_DIR}/${name}_gen.cpp")
    add_custom_command(
        OUTPUT "${output}" "${CMAKE_CURRENT_BINARY_DIR}/${name}_gen.h"
        COMMAND "${IDL_PYTHON}" "${IDL_WRAPPER}"
            --include "${CMAKE_CURRENT_SOURCE_DIR}"
            --base_dir "${CMAKE_CURRENT_BINARY_DIR}"
            --output "${output}"
            --header "${CMAKE_CURRENT_BINARY_DIR}/${name}_gen.h"
            --depfile "${output}.d"
            "${CMAKE_CURRENT_SOURCE_DIR}/${name}.idl"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${name}.idl" "${IDL_WRAPPER}"
        DEPFILE "${output}.d"
        VERBATIM)
    add_custom_target(${name} ALL DEPENDS "${output}")
endforeach()
''', encoding="utf-8")
        self.run_command("cmake", "-S", self.root, "-B", self.build, "-G", "Ninja",
                         "-DIDL_PYTHON=" + sys.executable, "-DIDL_WRAPPER=" + str(self.wrapper))

    def run_command(self, *args):
        result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def build_outputs(self):
        self.run_command("cmake", "--build", self.build, "-j2")
        return tuple((self.build / (name + "_gen.cpp")).stat().st_mtime_ns
                     for name in ("first", "second"))

    def test_depfile_lists_only_actual_transitive_inputs(self):
        self.build_outputs()
        depfile = (self.build / "first_gen.cpp.d").read_text(encoding="utf-8")
        for name in ("first.idl", "middle.idl", "leaf.idl"):
            self.assertIn(escape_depfile_path(self.root / name), depfile)
        self.assertNotIn("other.idl", depfile)
        self.assertNotIn("second.idl", depfile)

    def test_incremental_build_tracks_changed_and_new_imports(self):
        original = self.build_outputs()
        self.assertEqual(original, self.build_outputs(), "no-op build regenerated IDL")
        with (self.root / "leaf.idl").open("a", encoding="utf-8") as stream:
            stream.write("\n# Changed transitive dependency\n")
        changed = self.build_outputs()
        self.assertNotEqual(original[0], changed[0])
        self.assertEqual(original[1], changed[1], "unrelated output was regenerated")

        (self.root / "new.idl").write_text("global:\n    cpp_namespace: mongo\n",
                                          encoding="utf-8")
        (self.root / "middle.idl").write_text("imports: [leaf.idl, new.idl]\n",
                                             encoding="utf-8")
        imported = self.build_outputs()
        self.assertNotEqual(changed[0], imported[0])
        self.assertEqual(changed[1], imported[1])
        with (self.root / "new.idl").open("a", encoding="utf-8") as stream:
            stream.write("\n# Newly discovered dependency changed\n")
        updated = self.build_outputs()
        self.assertNotEqual(imported[0], updated[0])
        self.assertEqual(imported[1], updated[1])
        self.assertEqual(updated, self.build_outputs())

    def test_failed_compilation_does_not_publish_depfile(self):
        (self.root / "first.idl").write_text("imports: [missing.idl]\n", encoding="utf-8")
        result = subprocess.run(["cmake", "--build", str(self.build), "--target", "first", "-j2"],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.build / "first_gen.cpp.d").exists())

    def test_depfile_does_not_change_generated_cpp_or_header(self):
        self.build_outputs()
        cpp = self.build / "first_gen.cpp"
        header = self.build / "first_gen.h"
        expected = (cpp.read_bytes(), header.read_bytes())
        self.run_command(sys.executable, self.wrapper, "--include", self.root,
                         "--base_dir", self.build, "--output", cpp, "--header", header,
                         self.root / "first.idl")
        self.assertEqual(expected, (cpp.read_bytes(), header.read_bytes()))


if __name__ == "__main__":
    unittest.main()
