from pyspark.sql.functions import col, when, lit
from pyspark.sql import Column, DataFrame
from typing import Dict, Callable


def has_email_pattern(*column_names: str) -> Column:
    """
    Detects email-like text patterns in the specified columns.
    
    Pattern: something@something.tld (case-insensitive)
    Example: john.doe@example.com
    
    Args:
        *column_names: Variable number of column names to check for email patterns
        
    Returns:
        Column: A PySpark Column boolean expression that can be used in filter()
        
    Example:
        df.filter(has_email_pattern("name", "description"))
    """
    email_regex = r"(?i)[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    
    if not column_names:
        raise ValueError("At least one column name must be provided")
    
    conditions = [col(column_name).rlike(email_regex) for column_name in column_names]
    
    result = conditions[0]
    for condition in conditions[1:]:
        result = result | condition
    
    return result


def has_student_id_pattern(*column_names: str) -> Column:
    """
    Detects student ID-style patterns in the specified columns.
    
    Pattern: Letter A followed by 8 digits
    Example: A00123456, A12345678
    
    Args:
        *column_names: Variable number of column names to check for student ID patterns
        
    Returns:
        Column: A PySpark Column boolean expression that can be used in filter()
        
    Example:
        df.filter(has_student_id_pattern("title", "location"))
    """
    student_id_regex = r"\bA\d{8}\b"
    
    if not column_names:
        raise ValueError("At least one column name must be provided")
    
    conditions = [col(column_name).rlike(student_id_regex) for column_name in column_names]
    
    result = conditions[0]
    for condition in conditions[1:]:
        result = result | condition
    
    return result


def redact_pii_fields(
    df: DataFrame, 
    field_redactions: Dict[str, str],
    detection_func: Callable[[str], Column] = None,
    track_redactions: bool = True
) -> tuple[DataFrame, Dict[str, Dict[str, int]]]:
    """
    Redacts PII in specified fields by replacing values with redaction text.
    
    Args:
        df: The DataFrame to redact
        field_redactions: Dictionary mapping column names to their redaction text
                         Example: {"name": "[PII_REDACTED_NAME]", "description": "[PII_REDACTED_DESCRIPTION]"}
        detection_func: Optional custom detection function. Defaults to has_email_pattern.
                       The function should accept a column name and return a Column condition.
        track_redactions: If True, returns tracking information about redactions
    
    Returns:
        tuple: (DataFrame with PII-containing fields redacted, Dict with redaction counts per field)
               Redaction counts format: {"column_name": {"email_pattern": count, "student_id_pattern": count, "total": count}}
        
    Example:
        df, stats = redact_pii_fields(
            df, 
            {"name": "[PII_REDACTED_NAME]", "description": "[PII_REDACTED_DESCRIPTION]"}
        )
        
        # With custom detection
        df, stats = redact_pii_fields(
            df,
            {"title": "[PII_REDACTED_TITLE]"},
            detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
        )
    """
    if detection_func is None:
        detection_func = has_email_pattern
    
    result_df = df
    redaction_stats = {}
    
    for column_name, redaction_text in field_redactions.items():
        if track_redactions:
            # Count email patterns in this column
            email_count = df.filter(has_email_pattern(column_name)).count()
            # Count student ID patterns in this column
            student_id_count = df.filter(has_student_id_pattern(column_name)).count()
            # Count total (using the detection function which may combine both)
            total_count = df.filter(detection_func(column_name)).count()
            
            redaction_stats[column_name] = {
                "email_pattern": email_count,
                "student_id_pattern": student_id_count,
                "total": total_count
            }
        
        result_df = result_df.withColumn(
            column_name,
            when(detection_func(column_name), lit(redaction_text)).otherwise(col(column_name))
        )
    
    return result_df, redaction_stats
