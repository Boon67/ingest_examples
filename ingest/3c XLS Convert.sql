-- This script uses a Python Stored Procedure to read binary .xlsx files
-- directly from a stage, process them using pandas, and load the structured data to a table in JSON.

-- =================================================================
-- STEP 0: Define Variables and Context
-- =================================================================
USE DATABASE DB_TBOON;
USE SCHEMA SILVER;

-- =================================================================
-- STEP 1: Define Variables and Context
-- =================================================================
--DROP TABLE raw_text_data;
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


CREATE OR REPLACE PROCEDURE read_xlsx_files(stage_name string)
RETURNS TABLE(file_name VARCHAR, row_count NUMBER, status VARCHAR, message VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Row
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
import pandas as pd
import json

def run(session, stage_name):
    results = []
    
    files = session.sql(f"SELECT RELATIVE_PATH FROM DIRECTORY(@{stage_name})").collect()
    
    for file_row in files:
        file_path = file_row['RELATIVE_PATH']
        
        if not file_path.lower().endswith(('.xlsx', '.xls')):
            continue
        
        try:
            file_url = session.sql(f"SELECT BUILD_SCOPED_FILE_URL(@{stage_name}, '{file_path}')").collect()[0][0]
            
            with SnowflakeFile.open(file_url, 'rb', True) as f:
                df = pd.read_excel(f, engine='openpyxl')
            
            df['source_file_name'] = file_path
            df['file_row_number'] = range(1, len(df) + 1)
            
            df['raw_line_content'] = df.apply(
                lambda row: json.dumps({col: str(val) for col, val in row.items() 
                                       if col not in ['source_file_name', 'file_row_number']}, 
                                      default=str),
                axis=1
            )
            
            from datetime import datetime
            df['load_timestamp'] = datetime.now()
            
            insert_df = df[['raw_line_content', 'source_file_name', 'file_row_number', 'load_timestamp']]
            
            sp_df = session.create_dataframe(insert_df)
            sp_df.write.mode('append').save_as_table('raw_text_data')
            
            rows_inserted = len(df)
            
            results.append(Row(
                file_name=file_path,
                row_count=rows_inserted,
                status='SUCCESS',
                message=f'Inserted {rows_inserted} rows into raw_text_data'
            ))
        except Exception as e:
            results.append(Row(
                file_name=file_path,
                row_count=0,
                status='ERROR',
                message=str(e)[:500]
            ))
    
    schema = StructType([
        StructField("file_name", StringType()),
        StructField("row_count", IntegerType()),
        StructField("status", StringType()),
        StructField("message", StringType())
    ])
    
    return session.create_dataframe(results, schema=schema) if results else session.create_dataframe([], schema=schema)
$$;


CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');



