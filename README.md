# Celebal_Assignment_week_5

This repository contains three database-related Python automation tasks completed during the Celebal internship. Each assignment uses Python with MySQL to solve a real-world data engineering problem.

The dataset is taken of college and it has three tables name - student, customer and employee.

# Contents

| Assignment | Title | Description
| 1 | Export Tables to Files | Exports MySQL tables to various file formats (CSV, JSON, Excel)
| 2 | Trigger-based Export | Automatically exports a student record to CSV when a new row inserted
| 3 | Database Replication | Replicates MySQL data: Full (tables)
| 4 | Database Column Replication | Replicates MySQL data: Selective Columns

# Technology used

Python 3.12

- MySQL
- SQLAlchemy
- PyMySQL
- Pandas
- Logging
- Triggers (MySQL)
- File Formats: `.csv`, `.parquet`, `.avro`

# Assignment 1: Export Tables to Different Formats

**Files**:

- `export_files.py`
- Output: `student.csv`, `student.parquet`, `student.avro`, `export_debug.txt`

# Assignment 2: Trigger Based Export

- Scheduling Trigger
  **Files**:
- `export_files.py`
  Output: `export_debug_scheduling_trigger.txt`, `ExportOutput_scheduling_trigger`, and 3 screenshots.

- Batch Trigger
  **Files**:
- `export_files.py`, `event_trigger.py`, `trigger folder`
  Output: `export_debug_event_trigger.txt`, `ExportOutput_event_trigger`, `trigger_log.txt`

# Assignment 3: Database Replication

**Files**

- `replicate_db.py`
  Output: `log_sample.txt`
  The database of college is replicated to destination_db in MySQL.

# Assignment 4: Selective Column Replication

**Files**

- `replicate_selective_column.py`
  Output: `log_sample.txt`
  The selective column of database of college is replicated to destination_selective_column in MySQL.
