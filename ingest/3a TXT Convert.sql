-- This script performs three main steps:
-- 1. Creates a file format optimized for handling raw text files (.txt).
-- 2. Creates a target table to store the file contents (one row per line).
-- 3. Uses the COPY INTO command with a PATTERN to ingest only .txt files.

-- =================================================================
-- STEP 0: Define Variables (Adjust these as needed)
-- =================================================================
-- REPLACE 'YOUR_DB.YOUR_SCHEMA' with your actual database and schema.
USE DATABASE DB_TBOON;
USE SCHEMA SILVER;

-- REPLACE 'my_external_stage' with the name of your stage (e.g., S3, Azure, GCS).
-- Note: Stage MUST be pre-created and configured to point to your data location.

-- =================================================================
-- STEP 1: Create a File Format for Raw Text
-- We use TYPE = 'CSV' because it allows us to easily read line-by-line,
-- and then use the RAW data column for the entire line content.
-- =================================================================
CREATE OR REPLACE FILE FORMAT txt_file_format
    TYPE = CSV
    -- Treats the entire line as a single field, ideal for raw text files
    --FIELD_DELIMITER = ''
    -- Treats the file as one column of text (raw content)
    RECORD_DELIMITER = '\n'
    -- Ensures each line break creates a new record
    SKIP_HEADER = 0
    -- Do not skip the first line
    COMPRESSION = AUTO
    -- Auto-detect compression type
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    -- Allows flexible column counts (useful when reading only raw lines)
    EMPTY_FIELD_AS_NULL = FALSE;
    
-- =================================================================
-- STEP 2: Create the Target Table
-- This table will store the raw line content, the file name, and the line number.
-- =================================================================
CREATE TABLE IF NOT EXISTS raw_text_data (
    -- The raw line content from the .txt file
    raw_line_content VARCHAR,
    -- The source file name from the stage
    source_file_name VARCHAR,
    -- The line number within the file
    file_row_number NUMBER,
    -- (Optional) Add more columns if your text files have a known structure (e.g., ID NUMBER, VALUE VARCHAR)
    -- ...
    load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =================================================================
-- STEP 3: Load Data Using COPY INTO
-- This command reads files matching the PATTERN '.*\.txt' from the stage.
-- We use $1 to select the raw content, and metadata to get file and row info.
-- =================================================================
COPY INTO raw_text_data (raw_line_content, source_file_name, file_row_number)
FROM (
    SELECT
        -- $1 refers to the first (and only) column read by the file format
        $1,
        -- METADATA$FILENAME extracts the full path, we extract the base filename
        METADATA$FILENAME,
        -- METADATA$FILE_ROW_NUMBER extracts the line number within the file
        METADATA$FILE_ROW_NUMBER
    -- Specify the stage and file format to use
    FROM @BRONZE.RAW (FILE_FORMAT => 'txt_file_format')
)
-- Only select files ending with .txt (case-insensitive)
PATTERN = '.*\.txt'
-- Use ON_ERROR to handle errors gracefully (e.g., CONTINUE, SKIP_FILE)
ON_ERROR = 'ABORT_STATEMENT'
-- If you need to re-run the copy for the same files, set FORCE = TRUE
--\\ FORCE = TRUE;
;

-- =================================================================
-- STEP 4: Verify the Load
-- =================================================================
SELECT * FROM raw_text_data LIMIT 100 ;
SELECT COUNT(*), source_file_name FROM raw_text_data GROUP BY 2;

