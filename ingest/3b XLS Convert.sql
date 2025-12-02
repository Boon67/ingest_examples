-- This script uses a Python Stored Procedure to read binary .xlsx files
-- directly from a stage, process them using pandas, and load the structured data to a table.

-- =================================================================
-- STEP 0: Define Variables and Context
-- =================================================================
USE DATABASE DB_TBOON;
USE SCHEMA BRONZE;

-- =================================================================
-- STEP 1: Define Variables and Context
-- =================================================================

CREATE OR REPLACE PROCEDURE read_xlsx_files(stage_name string)
RETURNS TABLE(file_name VARCHAR, table_name VARCHAR, row_count NUMBER, status VARCHAR, message VARCHAR)
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
            
            table_name = '_' + file_path.split('.')[0].replace(' ', '_').replace('-', '_').replace('/', '_')
            
            sp_df = session.create_dataframe(df)
            sp_df.write.mode('overwrite').save_as_table(table_name)
            
            results.append(Row(
                file_name=file_path,
                table_name=table_name,
                row_count=len(df),
                status='SUCCESS',
                message=f'Table created with {len(df)} rows'
            ))
        except Exception as e:
            results.append(Row(
                file_name=file_path,
                table_name=None,
                row_count=0,
                status='ERROR',
                message=str(e)[:500]
            ))
    
    schema = StructType([
        StructField("file_name", StringType()),
        StructField("table_name", StringType()),
        StructField("row_count", IntegerType()),
        StructField("status", StringType()),
        StructField("message", StringType())
    ])
    
    return session.create_dataframe(results, schema=schema) if results else session.create_dataframe([], schema=schema)
$$;


CALL read_xlsx_files('DB_TBOON.PUBLIC.EXT_FILES_BUCKET');



