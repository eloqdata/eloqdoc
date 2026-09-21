#!/usr/bin/env python3
"""Check the Python 3 adapter preserves the original ICU data and initializer."""

from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


class ICUGeneratorTest(unittest.TestCase):
    def test_all_byte_values_are_embedded_unchanged(self):
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "data.dat"
            output = Path(directory) / "icu_init.cpp"
            payload = bytes(range(256)) + b"\r\n\x00\xff"
            data.write_bytes(payload)
            subprocess.run([
                sys.executable, str(root / "cmake/generate_icu_init.py"),
                str(root / "src/mongo/util/generate_icu_init_cpp.py"),
                "-i", str(data), "-o", str(output)], check=True)
            source = output.read_text()
            encoded = re.search(r"kRawData\[\] = \{([\d,]+)\};", source)
            self.assertIsNotNone(encoded)
            self.assertEqual(payload, bytes(int(value) for value in encoded.group(1).split(",")))
            self.assertIn("alignas(16)", source)
            self.assertIn("MONGO_INITIALIZER(LoadICUData)", source)
            self.assertIn("udata_setCommonData(kRawData, &status)", source)


if __name__ == "__main__":
    unittest.main()
