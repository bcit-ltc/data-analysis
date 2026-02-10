#!/usr/bin/env python3
"""
Preprocess a CSV so quoted fields (commas, newlines, quotes) are parsed and rewritten
correctly. Use this before Spark ETL when the source CSV has column misalignment
because of unquoted or badly quoted fields.

Reads with Python's csv module (handles "", newlines in quoted fields), writes with
consistent quoting so Spark can parse columns reliably.

Usage:
  python preprocess_csv_quoted.py <input.csv> <output.csv>
  python preprocess_csv_quoted.py data/DiscussionsForum/DiscussionForums.csv data/DiscussionsForum/DiscussionForums_fixed.csv
"""
import csv
import sys
from pathlib import Path


def preprocess_csv(input_path: str, output_path: str, *, encoding: str = "utf-8") -> None:
    input_path = Path(input_path)
    output_path = Path(output_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if input_path.resolve() == output_path.resolve():
        raise ValueError("Output path must differ from input path (use a temp file then rename if needed).")

    rows_written = 0
    with open(input_path, "r", encoding=encoding, newline="", errors="replace") as f_in:
        with open(output_path, "w", encoding=encoding, newline="", errors="replace") as f_out:
            reader = csv.reader(f_in, quotechar='"', doublequote=True, delimiter=",")
            writer = csv.writer(f_out, quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            for row in reader:
                writer.writerow(row)
                rows_written += 1
                if rows_written % 50000 == 0:
                    print(f"  ... {rows_written} rows", flush=True)

    print(f"Wrote {rows_written} rows to {output_path}")


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: preprocess_csv_quoted.py <input.csv> <output.csv>", file=sys.stderr)
        return 1
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    try:
        preprocess_csv(input_path, output_path)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
