CREATE OR REPLACE PROCEDURE DATAHUB.PIPELINE_METADATA.CONVERT2HTML("USERSQL" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import pandas
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session, sql):
    return str(session.sql(sql).to_pandas().fillna(''NA'').to_html(index=False))
    
';