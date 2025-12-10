# These are the ETL requirements

## Excel to Parquet

1. Incremental loading
2. Maintain folder structure
3. Comprehensive Logging to postgresql database: Tracks processing statistics and maintains state
4. Data Quality:
        - Ensure excel files rows and columns tally in parquet


FROM CHATGPT

Here’s a complete **feature list** of your two scripts — **`excel_parquet_pipeline.py`** and **`file_watcher.py`** — organized by function and capability:

---

## 🧩 **excel_parquet_pipeline.py – Excel ➜ Parquet ETL Pipeline**

A full-featured, incremental ETL pipeline that processes nested Excel files into Parquet format with schema management, tracking, and logging to PostgreSQL.

### ⚙️ Core Pipeline Features

1. **Incremental Processing**

   * Detects file changes using **MD5 hash + last modified timestamp**.
   * Only reprocesses Excel files that changed.

2. **Nested Excel File Support**

   * Recursively scans directories (`.xls`, `.xlsx`, `.xlsm`, `.xlsb`).
   * Maintains input subfolder hierarchy in output.

3. **Sheet-by-Sheet Processing**

   * Reads and converts each Excel sheet to a separate Parquet file.
   * Skips empty sheets automatically.

4. **Schema Evolution & Cleaning**

   * Cleans and normalizes column names.
   * Handles duplicate columns.
   * Removes empty rows/columns.
   * Flattens nested JSON/dict/list values into JSON strings (supports schema evolution).

5. **Metadata Columns**

   * Adds `_source_sheet` and `_processed_at` columns to each Parquet dataset.

---

### 🧮 **Data Quality & Validation**

1. Counts total Excel rows (efficiently using only the first column).
2. Counts Parquet rows directly from metadata.
3. Validates row counts and logs mismatches.
4. Tracks per-file and per-run statistics in PostgreSQL.

---

### 🪵 **Logging & Monitoring**

1. **Dual Logging System**

   * Local file logs (`excel_parquet_pipeline.log`).
   * PostgreSQL database logs (`pipeline_logs` table).

2. **Structured Logging Fields**

   * Timestamp, file path, sheet name, operation type, duration, row counts, success/failure, and error message.

3. **Run Statistics Table (`processing_stats`)**

   * Tracks overall run metrics: files processed, sheets processed, total Excel vs Parquet rows, duration, and mismatches.

4. **Log Retrieval**

   * `get_logs()` fetches logs from PostgreSQL into a Pandas DataFrame.

---

### 🧠 **State Management**

1. JSON-based **processing_state.json** file stores:

   * File hash
   * Modification timestamp
   * Last processed time
   * Sheets processed
   * Parquet output paths
   * Row counts (Excel vs Parquet)
2. Supports saving and reloading of state for fault tolerance.

---

### 🧹 **Maintenance & Automation**

1. **Clean Orphaned Outputs**

   * Deletes Parquet files for Excel files that were removed.
   * Updates state accordingly.

2. **CLI Interface**

   * Rich command-line arguments:

     * `--input-dir`, `--output-dir`, `--state-file`
     * `--clean-orphaned`
     * `--log-level`
     * `--db-*` connection settings
     * `--show-logs`, `--log-limit`
   * Prints summary after each run.

3. **Automatic Database Setup**

   * Creates `pipeline_logs` and `processing_stats` tables if not found.

---

## 🧠 **file_watcher.py – Real-Time Excel Watchdog**

A Docker-integrated service that monitors a directory for new or modified Excel files and triggers the pipeline automatically.

### 🕵️‍♂️ **Core Watcher Features**

1. **Directory Monitoring**

   * Uses `watchdog` library to watch `/etl/data/input` recursively.
   * Detects new or modified Excel files in real time.

2. **Event Handling**

   * Responds to both file creation and modification events.
   * Waits 30 seconds (configurable) to ensure files are fully written before triggering.

---

### 🧩 **Pipeline Trigger via Docker**

1. Runs pipeline **inside a Docker container** (e.g., `excel-parquet-pipeline`).

2. Executes:

   ```
   python excel_parquet_pipeline.py --input-dir /etl/data/input ...
   ```

3. Supports full argument list: input/output dirs, state file, logging, and DB credentials.

4. Logs:

   * Success or failure of pipeline execution.
   * Error messages captured from Docker container output.

---

### 🪵 **Logging & Database Integration**

1. **PostgreSQL Logging**

   * Table: `file_watcher_logs`.
   * Fields: timestamp, level, message, file path, event type, container, success flag, and errors.
2. **Local Logging**

   * Uses standard Python `logging` for console output.
3. **Graceful Shutdown**

   * Captures `KeyboardInterrupt` to stop watcher cleanly and log shutdown events.

---

### ⚙️ **Configuration & Environment**

* Configurable via environment variables:

  * `WATCH_DIR`, `PIPELINE_CONTAINER`
  * `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
* Automatically creates watch directory if it doesn’t exist.

---

## 🧾 **Combined System Features**

| Category          | Feature                                                                             |
| ----------------- | ----------------------------------------------------------------------------------- |
| **Data Flow**     | Excel → Parquet (Incremental, Multi-sheet, Nested Data Flattening)                  |
| **Storage**       | Parquet (Local/ETL Output Folder)                                                   |
| **Metadata**      | JSON state tracking (`processing_state.json`)                                       |
| **Logging**       | PostgreSQL (three tables: `pipeline_logs`, `processing_stats`, `file_watcher_logs`) |
| **Orchestration** | Real-time event triggers via file watcher + Docker                                  |
| **Validation**    | Excel vs Parquet row count comparison                                               |
| **Automation**    | Self-healing cleanup (orphan removal)                                               |
| **Monitoring**    | Structured logs retrievable via SQL or Pandas                                       |
| **CLI Tools**     | Command-line flags for full pipeline control                                        |

---

Would you like me to generate a **system architecture diagram** (showing how the watcher, pipeline, PostgreSQL, and Docker interact)? It would help visualize how everything connects.
