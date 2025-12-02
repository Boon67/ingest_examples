USE DATABASE DB_TBOON;
CREATE OR REPLACE PROCEDURE copy_new_files_to_raw()
RETURNS TABLE(file_name VARCHAR, status VARCHAR, message VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session):
    from snowflake.snowpark import Row
    from snowflake.snowpark.types import StructType, StructField, StringType
    
    results = []
    
    ext_files = session.sql("SELECT RELATIVE_PATH FROM DIRECTORY(@PUBLIC.EXT_FILES_BUCKET)").collect()
    raw_files = session.sql("SELECT RELATIVE_PATH FROM DIRECTORY(@BRONZE.RAW)").collect()
    
    raw_file_set = {row['RELATIVE_PATH'] for row in raw_files}
    
    for file_row in ext_files:
        file_path = file_row['RELATIVE_PATH']
        
        if file_path not in raw_file_set:
            try:
                session.sql(f"""
                    COPY FILES INTO @BRONZE.RAW
                    FROM @PUBLIC.EXT_FILES_BUCKET
                    FILES = ('{file_path}')
                """).collect()
                results.append(Row(file_name=file_path, status='SUCCESS', message='File copied to RAW stage'))
            except Exception as e:
                results.append(Row(file_name=file_path, status='ERROR', message=str(e)[:500]))
        else:
            results.append(Row(file_name=file_path, status='SKIPPED', message='File already exists in RAW stage'))
    
    schema = StructType([
        StructField("file_name", StringType()),
        StructField("status", StringType()),
        StructField("message", StringType())
    ])
    
    return session.create_dataframe(results, schema=schema) if results else session.create_dataframe([], schema=schema)
$$;

CALL copy_new_files_to_raw();
