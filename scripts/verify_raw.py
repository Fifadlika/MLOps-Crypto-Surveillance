"""Validate JSONL sidecar metadata consistency for ingestion outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _verify_tree(root: Path) -> list[str]:
    issues: list[str] = []
    if not root.exists():
        return issues

    for jsonl_path in sorted(root.rglob("*.jsonl")):
        meta_path = jsonl_path.with_suffix(".meta")
        if not meta_path.exists():
            issues.append(f"missing_meta:{jsonl_path}")
            continue

        try:
            sidecar = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            issues.append(f"invalid_meta_json:{meta_path}")
            continue

        actual_count = _count_lines(jsonl_path)
        expected_count = int(sidecar.get("record_count", -1))
        if actual_count != expected_count:
            issues.append(
                f"count_mismatch:{jsonl_path}:actual={actual_count}:expected={expected_count}"
            )

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify ingestion JSONL files against .meta sidecars."
    )
    parser.add_argument(
        "--roots",
        nargs="*",
        default=["data/raw", "data/preprocess"],
        help="Directories to scan recursively.",
    )
    args = parser.parse_args()

    all_issues: list[str] = []
    for root in args.roots:
        all_issues.extend(_verify_tree(Path(root)))

    if all_issues:
        print("VERIFY_RAW:FAIL")
        for issue in all_issues:
            print(issue)
        return 1

    print("VERIFY_RAW:OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
