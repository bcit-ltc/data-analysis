# deidentification-output

This directory contains privacy and de-identification reports generated from ETL outputs.

- **Source data**: Typically produced under the `etl-output/` directory by the Spark ETL jobs.
- **Contents**: Text-based privacy and risk summaries per dataset/table (e.g., `privacy-report.txt`).
- **Git behavior**: The directory itself and this README are tracked, but individual report files are ignored via `.gitignore`.

You can safely delete and regenerate the contents of this directory without affecting version control.
