import argparse
import csv
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence


SUSPECT_NAME_PATTERNS: Sequence[str] = (
    "id",
    "name",
    "email",
    "mail",
    "phone",
    "address",
    "dob",
    "birth",
    "ssn",
    "sin",
    "student",
    "user",
    "account",
)


@dataclass
class ColumnPrivacySummary:
    name: str
    non_null_rows: int
    approx_distinct: int
    uniqueness_ratio: float
    capped_distinct: bool
    suspicious_name: bool
    risk_level: str


@dataclass
class DatasetPrivacyReport:
    dataset_name: str
    table_name: str
    rows_scanned: int
    columns: List[ColumnPrivacySummary]


def _infer_risk_level(uniqueness_ratio: float, suspicious_name: bool) -> str:
    if uniqueness_ratio >= 0.8 and suspicious_name:
        return "HIGH"
    if uniqueness_ratio >= 0.8 or suspicious_name:
        return "MEDIUM"
    return "LOW"


def _looks_suspicious(name: str) -> bool:
    lower = name.lower()
    return any(pattern in lower for pattern in SUSPECT_NAME_PATTERNS)


def analyze_csv_directory(
    data_dir: str,
    *,
    max_rows: Optional[int] = 200_000,
    distinct_cap: int = 10_000,
) -> Optional[DatasetPrivacyReport]:
    csv_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".csv")]
    if not csv_files:
        return None

    fieldnames: Optional[List[str]] = None
    distinct_values: Dict[str, set] = {}
    non_null_counts: Dict[str, int] = {}
    rows_scanned = 0

    # Iterate over all CSVs in this directory, accumulating stats.
    for fname in sorted(csv_files):
        path = os.path.join(data_dir, fname)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue

            if fieldnames is None:
                fieldnames = list(reader.fieldnames)
                distinct_values = {c: set() for c in fieldnames}
                non_null_counts = {c: 0 for c in fieldnames}

            for row in reader:
                rows_scanned += 1
                for col in fieldnames:
                    val = row.get(col)
                    if val is None or val == "":
                        continue
                    non_null_counts[col] += 1
                    values = distinct_values[col]
                    if len(values) < distinct_cap:
                        values.add(val)

                if max_rows is not None and rows_scanned >= max_rows:
                    break

        if max_rows is not None and rows_scanned >= max_rows:
            break

    if not fieldnames:
        return None

    # Infer dataset/table names from directory structure relative to input base.
    # Caller is responsible for setting these on the returned object.
    columns: List[ColumnPrivacySummary] = []
    for col in fieldnames:
        nn = non_null_counts.get(col, 0)
        approx_distinct = len(distinct_values.get(col, set()))
        uniqueness = float(approx_distinct) / nn if nn > 0 else 0.0
        suspicious = _looks_suspicious(col)
        risk = _infer_risk_level(uniqueness, suspicious)
        capped = approx_distinct >= distinct_cap
        columns.append(
            ColumnPrivacySummary(
                name=col,
                non_null_rows=nn,
                approx_distinct=approx_distinct,
                uniqueness_ratio=uniqueness,
                capped_distinct=capped,
                suspicious_name=suspicious,
                risk_level=risk,
            )
        )

    # Placeholder dataset/table; caller will override.
    return DatasetPrivacyReport(
        dataset_name="",
        table_name="",
        rows_scanned=rows_scanned,
        columns=columns,
    )


def _format_report_text(report: DatasetPrivacyReport) -> str:
    lines: List[str] = []
    label = report.dataset_name
    if report.table_name:
        label = f"{report.dataset_name}.{report.table_name}" if report.dataset_name else report.table_name

    lines.append(f"Privacy check report for: {label or 'unknown dataset'}")
    lines.append("")
    lines.append(f"Rows scanned: {report.rows_scanned}")
    lines.append(f"Columns analyzed: {len(report.columns)}")
    lines.append("")

    lines.append("Per-column privacy summary:")
    for col in report.columns:
        lines.append(f"- {col.name}")
        lines.append(f"    Non-null rows: {col.non_null_rows}")
        lines.append(f"    Approx. distinct values: {col.approx_distinct}{' (capped)' if col.capped_distinct else ''}")
        lines.append(f"    Approx. uniqueness ratio: {col.uniqueness_ratio:.3f}")
        lines.append(f"    Suspicious name: {'yes' if col.suspicious_name else 'no'}")
        lines.append(f"    Inferred risk level: {col.risk_level}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _infer_dataset_table_from_relpath(relpath: str) -> (str, str):
    parts = relpath.split(os.sep)
    # Expected ETL layout: output/{dataset}/{table}/data
    if len(parts) >= 3 and parts[2] == "data":
        dataset = parts[0]
        table = parts[1]
        return dataset, table

    if len(parts) >= 2:
        return parts[0], "/".join(parts[1:])
    if parts:
        return parts[0], "data"
    return "root", "data"


def run_privacy_checks(input_base: str, output_base: str) -> None:
    input_base = os.path.abspath(input_base)
    output_base = os.path.abspath(output_base)

    if not os.path.exists(input_base):
        raise SystemExit(f"Input directory does not exist: {input_base}")

    os.makedirs(output_base, exist_ok=True)

    any_reports = False

    for root, _dirs, files in os.walk(input_base):
        csv_files = [f for f in files if f.lower().endswith(".csv")]
        if not csv_files:
            continue

        rel = os.path.relpath(root, input_base)
        dataset_name, table_name = _infer_dataset_table_from_relpath(rel)

        report = analyze_csv_directory(root)
        if report is None:
            continue

        report.dataset_name = dataset_name
        report.table_name = table_name

        out_dir = os.path.join(output_base, dataset_name, table_name)
        os.makedirs(out_dir, exist_ok=True)
        report_path = os.path.join(out_dir, "privacy-report.txt")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(_format_report_text(report))

        print(f"[privacy-check] Wrote report for {dataset_name}.{table_name} -> {report_path}")
        any_reports = True

    if not any_reports:
        print(f"[privacy-check] No CSV files found under {input_base}; no reports generated.")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run basic privacy checks on ETL outputs. "
            "Scans CSV files under an input directory and writes reports to an output directory."
        )
    )
    parser.add_argument(
        "--input-dir",
        default="output",
        help="Directory containing ETL outputs (default: 'output').",
    )
    parser.add_argument(
        "--output-dir",
        default="/output",
        help="Directory where privacy reports will be written (default: '/output').",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_privacy_checks(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
