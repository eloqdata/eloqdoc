#!/usr/bin/env python3
"""Generate MongoDB error-code files without changing its Python 2/Cheetah generator.

MongoDB 4.0's SCons build continues to invoke src/mongo/base/generate_error_codes.py.
The CMake build uses this Python 3-only renderer for the small Cheetah directive subset present in
the checked-in error-code templates.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


DIRECTIVE = re.compile(r"^\s*//#(for|if|set)\s+(.*?)\s*$")
END_DIRECTIVE = re.compile(r"^\s*//#end\s+(for|if)\s*$")
BRACED_VALUE = re.compile(r"\$\{([^}]+)\}")
VALUE = re.compile(r"\$([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)")


class ErrorCode:
    def __init__(self, name: str, code: int, extra: str | None = None):
        self.name = name
        self.code = code
        self.extra = extra
        self.categories: List[str] = []


class ErrorClass:
    def __init__(self, name: str, codes: Sequence[str]):
        self.name = name
        self.codes = codes


def parse_definitions(path: Path) -> Tuple[List[ErrorCode], List[ErrorClass]]:
    error_codes: List[ErrorCode] = []
    error_classes: List[ErrorClass] = []
    namespace = {
        "error_code": lambda *args, **kwargs: error_codes.append(ErrorCode(*args, **kwargs)),
        "error_class": lambda *args: error_classes.append(ErrorClass(*args)),
    }
    source = path.read_text(encoding="utf-8")
    exec(compile(source, str(path), "exec"), namespace)
    error_codes.sort(key=lambda item: item.code)

    names: Dict[str, ErrorCode] = {}
    numeric_codes: Dict[int, ErrorCode] = {}
    for error_code in error_codes:
        if error_code.name in names:
            raise ValueError(f"Duplicate error-code name: {error_code.name}")
        if error_code.code in numeric_codes:
            raise ValueError(f"Duplicate numeric error code: {error_code.code}")
        names[error_code.name] = error_code
        numeric_codes[error_code.code] = error_code

    category_names = set()
    for category in error_classes:
        if category.name in category_names:
            raise ValueError(f"Duplicate error category: {category.name}")
        category_names.add(category.name)
        for error_name in category.codes:
            if error_name not in names:
                raise ValueError(
                    f"Undeclared error code {error_name} in category {category.name}"
                )
            names[error_name].categories.append(category.name)
    return error_codes, error_classes


def evaluate(expression: str, values: Dict[str, Any]) -> Any:
    expression = re.sub(r"\$([A-Za-z_]\w*)", r"\1", expression).rstrip(":")
    scope: Dict[str, Any] = {"__builtins__": {}, "enumerate": enumerate, "len": len}
    scope.update(values)
    return eval(expression, scope)


def parse_template(
    lines: Sequence[str], position: int = 0, terminator: str | None = None
) -> Tuple[List[tuple], int]:
    nodes: List[tuple] = []
    while position < len(lines):
        end_match = END_DIRECTIVE.match(lines[position])
        if end_match:
            if end_match.group(1) != terminator:
                raise ValueError(f"Unexpected template directive: {lines[position].strip()}")
            return nodes, position + 1

        match = DIRECTIVE.match(lines[position])
        if not match:
            nodes.append(("text", lines[position]))
            position += 1
            continue

        operation, argument = match.groups()
        if operation == "for":
            target, expression = argument.split(" in ", 1)
            names = [name.strip().lstrip("$") for name in target.split(",")]
            body, position = parse_template(lines, position + 1, "for")
            nodes.append(("for", names, expression, body))
        elif operation == "if":
            body, position = parse_template(lines, position + 1, "if")
            nodes.append(("if", argument, body))
        else:
            name, expression = argument.split("=", 1)
            nodes.append(("set", name.strip().lstrip("$"), expression.strip()))
            position += 1

    if terminator:
        raise ValueError(f"Missing //#end {terminator}")
    return nodes, position


def substitute(line: str, values: Dict[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        return str(evaluate(match.group(1), values))

    return VALUE.sub(replace, BRACED_VALUE.sub(replace, line))


def render_nodes(nodes: Iterable[tuple], values: Dict[str, Any]) -> List[str]:
    result: List[str] = []
    for node in nodes:
        if node[0] == "text":
            result.append(substitute(node[1], values))
        elif node[0] == "set":
            values[node[1]] = evaluate(node[2], values)
        elif node[0] == "if":
            if evaluate(node[1], values):
                result.extend(render_nodes(node[2], dict(values)))
        else:
            _, names, expression, body = node
            for item in evaluate(expression, values):
                nested = dict(values)
                unpacked = item if len(names) > 1 else (item,)
                nested.update(zip(names, unpacked))
                result.extend(render_nodes(body, nested))
    return result


def render_template(path: Path, values: Dict[str, Any]) -> str:
    nodes, _ = parse_template(path.read_text(encoding="utf-8").splitlines(keepends=True))
    return "".join(render_nodes(nodes, values))


def main(argv: Sequence[str]) -> int:
    if len(argv) < 3:
        raise SystemExit(
            "usage: generate_error_codes.py error_codes.err template=output [template=output ...]"
        )
    error_codes, error_classes = parse_definitions(Path(argv[1]))
    values = {"codes": error_codes, "categories": error_classes}
    for argument in argv[2:]:
        template, output = argument.split("=", 1)
        Path(output).write_text(render_template(Path(template), values), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
