# Healthcare Claims Data Ingestion Pipeline

This project provides a complete data ingestion pipeline for processing healthcare claims data files (XLSX, XLS, TXT) from external stages into a structured Snowflake data warehouse.

## Architecture Overview

The pipeline follows a **Bronze → Silver → Gold** medallion architecture pattern:
- **Bronze (RAW)**: Raw file storage from external sources
- **Silver**: Cleaned, structured data with standardized schemas
- **Gold**: Business-ready aggregated data (not included in this pipeline)

## File Sequence & Execution Order

Execute the scripts in the following order:

### 1. Database Setup (`1. Database Setup.sql`)
**Purpose**: Initialize the database, schemas, and stages for the ingestion pipeline.

**What it does**:
- Creates database `DB_TBOON`
- Creates schemas: `PUBLIC`, `BRONZE`, `SILVER`
- Creates stages:
  - `EXT_FILES_BUCKET` - External file landing zone
  - `RAW` - Bronze layer for raw file copies

**Run first**: This sets up the foundation for all subsequent operations.

---

### 2. Copy New Files to RAW (`2. CopyNewFilesToRaw.sql`)
**Purpose**: Copy files from the external bucket to the RAW stage, avoiding duplicates.

**What it does**:
- Compares files in `EXT_FILES_BUCKET` vs `RAW` stage
- Copies only new files that don't already exist in RAW
- Returns a result set showing status for each file (SUCCESS/SKIPPED/ERROR)
- Returns empty dataset if no new files found

**Procedure**: `copy_new_files_to_raw()`

**Usage**:
```sql
CALL copy_new_files_to_raw();
```

---

### 3a. Text File Conversion (`3a TXT Convert.sql`)
**Purpose**: Process plain text files from the RAW stage.

**What it does**:
- Reads `.txt` files from RAW stage
- Parses each line as raw text
- Loads into Bronze layer tables

**Note**: Use for unstructured text data.

---

### 3b. Excel File Conversion - Create Tables (`3b XLS Convert.sql`)
**Purpose**: Convert Excel files into individual Snowflake tables (one table per file).

**What it does**:
- Scans `EXT_FILES_BUCKET` for `.xlsx` and `.xls` files
- Reads each Excel file using pandas
- Creates a new table named after the file (with prefix `_`)
- Example: `730803_CompanyName3_TokioMarine.xlsx` → table `_730803_COMPANYNAME3_TOKIOMARINE`

**Procedure**: `read_xlsx_files(stage_name)`

**Usage**:
```sql
CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');
```

**When to use**: When you need each Excel file as a separate table for analysis.

---

### 3c. Excel File Conversion - Load to Raw Text Data (`3c XLS Convert.sql`)
**Purpose**: Convert Excel files into a centralized raw data table.

**What it does**:
- Scans `EXT_FILES_BUCKET` for `.xlsx` and `.xls` files
- Reads each Excel file row-by-row
- Converts each row to JSON format
- Inserts into `SILVER.raw_text_data` table with metadata:
  - `raw_line_content` - JSON representation of the row
  - `source_file_name` - Original file path
  - `file_row_number` - Row number within the file
  - `load_timestamp` - When the data was loaded

**Procedure**: `read_xlsx_files(stage_name)`

**Usage**:
```sql
USE SCHEMA SILVER;
CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');
```

**When to use**: When you want all Excel data in one central table for unified processing.

**Performance**: Uses batch inserts for efficiency (100-1000x faster than row-by-row).

---

### 4. Merge Mapping Script (`4. Merge Mapping Script.sql`)
**Purpose**: Map raw source data to standardized target schema and merge into production tables.

**What it does**:
- Creates target table `TARGET_CLAIMS` with standardized healthcare claims schema
- Maps source table columns to target columns:
  - Renames abbreviated columns to full names (e.g., `ACCT_NBR` → `ACCOUNT_NUMBER`)
  - Converts Unix timestamps to DATE types
  - Trims whitespace from text fields
  - Adds audit columns (`SOURCE_FILE`, `LOAD_TIMESTAMP`)
- Performs UPSERT operation (MERGE):
  - **UPDATE** existing records based on claim number + subscriber ID + service date
  - **INSERT** new records
- Includes verification queries to check results

**Source Table Example**: `DB_HACK_TBOON.PUBLIC._730803_COMPANYNAME3_TOKIOMARINE_MD_SLDET_20250101_20251031_20251104`

**Target Table**: `DB_HACK_TBOON.PUBLIC.TARGET_CLAIMS`

**Merge Keys**: 
- `CLAIM_NUMBER`
- `SUBSCRIBER_ID` 
- `SERVICE_START_DATE`

**Usage**:
```sql
-- Execute the entire script to create table and merge data
```

**When to use**: After loading raw data via 3b or 3c, run this to standardize and consolidate into production tables.

---

## Target Schema Definition

The pipeline includes a standardized healthcare claims schema (`CLAIMS_STANDARD`) with 45+ fields including:

**Key Fields** (highlighted in original requirements):
- Policy & Group Information
- Member Demographics (Insured & Claimant)
- Claim Identifiers & Dates
- Diagnosis & Procedure Codes (ICD, CPT, HCPCS)
- Financial Amounts (Billed, Allowed, Paid, Deductible, Copay, etc.)
- Provider Information
- Pharmacy Data (NDC, Rx details)
- Network Status & Claim Type

See `4. Merge Mapping Script.sql` for complete schema definition.

---

## Typical Execution Flow

### Scenario 1: New Files Arrive
```sql
-- Step 1: Copy new files from external bucket to RAW
USE SCHEMA PUBLIC;
CALL copy_new_files_to_raw();

-- Step 2: Process Excel files into raw_text_data
USE SCHEMA SILVER;
CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');

-- Step 3: Map and merge into target schema
-- Execute: 4. Merge Mapping Script.sql
```

### Scenario 2: Reprocess Specific File
```sql
-- Option A: Create separate table for analysis
USE SCHEMA BRONZE;
CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');

-- Option B: Add to centralized raw table
USE SCHEMA SILVER;
CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');
```

---

## Error Handling

All procedures return status tables with:
- `file_name` - File being processed
- `status` - SUCCESS/ERROR/SKIPPED
- `message` - Descriptive message or error details
- `row_count` - Number of rows processed (where applicable)

Always check the result set for errors before proceeding to the next step.

---

## Notes & Best Practices

1. **Idempotency**: Script 2 prevents duplicate file copies. Scripts 3b/3c will reload data on each run.
2. **Performance**: Script 3c uses batch operations for optimal performance.
3. **Schema Evolution**: Modify `4. Merge Mapping Script.sql` to add new fields or change mappings.
4. **Monitoring**: Always review the status result sets to catch errors early.
5. **File Naming**: Table names are derived from filenames - use consistent naming conventions.

---

## Prerequisites

- Snowflake account with appropriate permissions
- Python 3.11+ runtime (for stored procedures)
- Required Python packages: `snowflake-snowpark-python`, `pandas`, `openpyxl`
- Files uploaded to stages (use Snowsight UI or `PUT` command)

---

## Future Enhancements

- Add incremental loading based on file modification dates
- Implement data quality checks and validation rules
- Add Gold layer transformations for business metrics
- Create scheduling via Tasks or external orchestration
- Add support for additional file formats (CSV, JSON, Parquet)
