
USE SCHEMA ELT_STAGE;




CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG as select *
  from table(information_schema.task_history(scheduled_time_range_start=>dateadd('hours',-4,current_timestamp()), result_limit=>1000)) where STATE != 'SKIPPED' order by scheduled_time asc;





CREATE OR REPLACE STREAM ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG_STREAM
ON TABLE ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG;



CREATE OR REPLACE TABLE ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG_STREAM_DUMP
LIKE ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG;




CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_SNOWFLAKE_MERGE_HISTORY_LOG()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$



function mergeLog() {
    sqlstr = `
        MERGE INTO ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG LOG
        USING (
            SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(scheduled_time_range_start=>DATEADD(HOURS, -4, CURRENT_TIMESTAMP()), result_limit=>10000)) where DATABASE_NAME = CURRENT_DATABASE() and STATE != 'SCHEDULED' and STATE != 'SKIPPED' and STATE != 'EXECUTING'
        ) HISTORY

        ON LOG.RUN_ID = HISTORY.RUN_ID

        WHEN NOT MATCHED THEN
        INSERT (
             QUERY_ID
            ,NAME
            ,DATABASE_NAME
            ,SCHEMA_NAME
            ,QUERY_TEXT
            ,CONDITION_TEXT
            ,STATE
            ,ERROR_CODE
            ,ERROR_MESSAGE
            ,SCHEDULED_TIME
            ,QUERY_START_TIME
            ,NEXT_SCHEDULED_TIME
            ,COMPLETED_TIME
            ,ROOT_TASK_ID
            ,GRAPH_VERSION
            ,RUN_ID
            ,RETURN_VALUE
        ) VALUES (
             HISTORY.QUERY_ID
            ,HISTORY.NAME
            ,HISTORY.DATABASE_NAME
            ,HISTORY.SCHEMA_NAME
            ,HISTORY.QUERY_TEXT
            ,HISTORY.CONDITION_TEXT
            ,HISTORY.STATE
            ,HISTORY.ERROR_CODE
            ,HISTORY.ERROR_MESSAGE
            ,HISTORY.SCHEDULED_TIME
            ,HISTORY.QUERY_START_TIME
            ,HISTORY.NEXT_SCHEDULED_TIME
            ,HISTORY.COMPLETED_TIME
            ,HISTORY.ROOT_TASK_ID
            ,HISTORY.GRAPH_VERSION
            ,HISTORY.RUN_ID
            ,HISTORY.RETURN_VALUE
        );`;
    var statement = snowflake.createStatement({sqlText: sqlstr});
    var results = statement.execute();
    results.next();
    return results.getColumnValue(1);
};


function deleteOldRows() {
    sqlstr = `
    DELETE FROM ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG
    WHERE SCHEDULED_TIME < DATEADD(HOURS, -24*5, CURRENT_TIMESTAMP())
    ;`;
    var statement = snowflake.createStatement({sqlText: sqlstr});
    var results = statement.execute();
    results.next();
    return results.getColumnValue(1);
    };




mergetasklog = mergeLog();

deletecount = deleteOldRows();

return deletecount + ' rows deleted.';

$$;






CREATE OR REPLACE TASK ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG_TASK
WAREHOUSE = {{ WAREHOUSE }}_RAW_WH
SCHEDULE = '120 minute'
AS CALL ELT_STAGE.USP_SNOWFLAKE_MERGE_HISTORY_LOG();

ALTER TASK ELT_STAGE.SNOWFLAKE_MERGE_HISTORY_LOG_TASK RESUME;





