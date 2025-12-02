-- =================================================================
-- TARGET SCHEMA DEFINITION
-- Standard Claims Processing Table
-- =================================================================
-- This schema defines the standardized structure for claims data
-- Yellow highlighted fields indicate required/key fields
-- =================================================================

CREATE OR REPLACE DATABASE DB_TBOON;
USE DATABASE DB_TBOON;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE TABLE IF NOT EXISTS SILVER.CLAIMS_TARGET (
    POLICY_EFFECTIVE_DATE DATE,
    GROUP_NAME VARCHAR(255),
    INSURED_FIRST_NAME VARCHAR(100),
    INSURED_LAST_NAME VARCHAR(100),
    INSURED_DOB DATE,
    CLAIMANT_FIRST_NAME VARCHAR(100),
    CLAIMANT_LAST_NAME VARCHAR(100),
    CLAIMANT_DOB DATE,
    DATE_OF_ADMISSION DATE,
    CLAIM_NUMBER VARCHAR(50),
    CLAIM_CONTROL_NUMBER VARCHAR(50),
    BEGINNING_SERVICE_DATE DATE,
    ENDING_SERVICE_DATE DATE,
    PROCESSED_DATE DATE,
    PRIMARY_ICD VARCHAR(20),
    SECONDARY_ICD VARCHAR(20),
    CPT_CODE VARCHAR(20),
    HCPCS_CODE VARCHAR(20),
    SERVICE_TYPE VARCHAR(100),
    REVENUE_CODE VARCHAR(20),
    MODIFIER_CODE VARCHAR(20),
    NDC VARCHAR(50),
    RX_NAME VARCHAR(255),
    RX_QUANTITY NUMBER(18,4),
    RX_DAYS_SUPPLY NUMBER,
    RX_DATE_FILLED DATE,
    BILLED_AMOUNT NUMBER(18,2),
    COPAY_AMOUNT NUMBER(18,2),
    DEDUCTIBLE_AMOUNT NUMBER(18,2),
    COINSURANCE_AMOUNT NUMBER(18,2),
    ALLOWED_AMOUNT NUMBER(18,2),
    COB_AMOUNT NUMBER(18,2),
    INELIGIBLE_AMOUNT NUMBER(18,2),
    COB_AMOUNT_2 NUMBER(18,2),
    OTHER_REDUCED_AMOUNT NUMBER(18,2),
    MEMBER_PAID_COST_SHARE NUMBER(18,2),
    PAID_AMOUNT NUMBER(18,2),
    NETWORK_STATUS VARCHAR(50),
    CLAIM_TYPE VARCHAR(50),
    PAYEE_NAME VARCHAR(255),
    PAYEE_ID VARCHAR(50),
    PAYEE_TIN VARCHAR(20),
    SOURCE_FILE VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    );

--Stages Setup
--Files can be uploaded using the Snowsight UI or via a Streamlit application

-- Create SOURCE stage with cloud storage integration
/*CREATE OR REPLACE STAGE SOURCE
  URL = 's3://your-bucket/source/'
  CREDENTIALS = (AWS_KEY_ID = 'your_key' AWS_SECRET_KEY = 'your_secret')
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
  COMMENT = 'Stage for source data files from S3';
*/
-- Create EXT_FILES_BUCKET stage for internal file storage
USE SCHEMA PUBLIC;
CREATE STAGE IF NOT EXISTS EXT_FILES_BUCKET
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE) --This encrypts on Snowflake side
  COMMENT = 'Internal stage for raw data processing with server-side encryption';

-- Create RAW stage for internal file storage
USE SCHEMA BRONZE;
CREATE STAGE IF NOT EXISTS RAW
  FILE_FORMAT = (TYPE = 'CSV')
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE)--This encrypts on Snowflake side
  COMMENT = 'Internal stage for raw data processing';

-- List files in stages (will be empty initially)
LIST @PUBLIC.EXT_FILES_BUCKET;
LIST @BRONZE.RAW;

SHOW STAGES;

-- Show stage details
DESCRIBE STAGE PUBLIC.EXT_FILES_BUCKET;
DESCRIBE STAGE BRONZE.RAW;

