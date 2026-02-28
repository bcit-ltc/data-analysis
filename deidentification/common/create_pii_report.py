import os
from typing import Dict, List
from datetime import datetime


def create_pii_report(
    output_base: str,
    dataset_name: str,
    table_name: str,
    dropped_columns: List[str],
    redaction_stats: Dict[str, Dict[str, int]],
    total_records: int
):
    """
    Creates a PII report documenting columns dropped and fields redacted.
    
    Args:
        output_base: Base output directory (e.g., "output")
        dataset_name: Name of the dataset (e.g., "contentdata")
        table_name: Name of the table (e.g., "contentobjects")
        dropped_columns: List of column names that were dropped entirely
        redaction_stats: Dictionary with redaction statistics per column
                        Format: {"column_name": {"email_pattern": count, "student_id_pattern": count, "total": count}}
        total_records: Total number of records in the dataset
    
    Example:
        create_pii_report(
            "output",
            "contentdata",
            "contentobjects",
            ["created_by", "last_modified_by", "deleted_by"],
            {"title": {"email_pattern": 5, "student_id_pattern": 3, "total": 8}},
            1000
        )
    """
    # Create reports directory
    report_dir = f"{output_base}/{dataset_name}/{table_name}/reports"
    os.makedirs(report_dir, exist_ok=True)
    
    report_path = f"{report_dir}/pii_report.txt"
    
    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("PII DEIDENTIFICATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Dataset: {dataset_name}\n")
        f.write(f"Table: {table_name}\n")
        f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Records: {total_records:,}\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("COLUMNS DROPPED (Removed Entirely)\n")
        f.write("-" * 80 + "\n")
        
        if dropped_columns:
            for col in dropped_columns:
                f.write(f"  - {col}\n")
            f.write(f"\nTotal Columns Dropped: {len(dropped_columns)}\n\n")
        else:
            f.write("  No columns were dropped.\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("FIELDS REDACTED (Values Replaced with Placeholder Text)\n")
        f.write("-" * 80 + "\n\n")
        
        if redaction_stats:
            total_redacted_records = 0
            
            for column_name, stats in redaction_stats.items():
                email_count = stats.get("email_pattern", 0)
                student_id_count = stats.get("student_id_pattern", 0)
                total_count = stats.get("total", 0)
                
                f.write(f"Column: {column_name}\n")
                f.write(f"  Records with Email Pattern:      {email_count:,}\n")
                f.write(f"  Records with Student ID Pattern: {student_id_count:,}\n")
                f.write(f"  Total Records Redacted:          {total_count:,}\n")
                
                if total_records > 0:
                    percentage = (total_count / total_records) * 100
                    f.write(f"  Percentage of Dataset:           {percentage:.2f}%\n")
                
                f.write("\n")
                
                total_redacted_records = max(total_redacted_records, total_count)
            
            f.write(f"Total Fields Redacted: {len(redaction_stats)}\n\n")
        else:
            f.write("  No fields were redacted.\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("PII DETECTION PATTERNS\n")
        f.write("=" * 80 + "\n")
        f.write("Email Pattern:      [A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\n")
        f.write("Student ID Pattern: A followed by 8 digits (e.g., A00123456)\n")
        f.write("=" * 80 + "\n")
    
    print(f"PII report generated: {report_path}")
    return report_path
