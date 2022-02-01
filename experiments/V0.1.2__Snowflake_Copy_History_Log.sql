
USE SCHEMA ELT_STAGE;




CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG
( --https://docs.snowflake.com/en/sql-reference/functions/copy_history.html
     FILE_NAME TEXT COMMENT 'Name of the source file and relative path to the file.'
    ,STAGE_LOCATION TEXT COMMENT 'Name of the stage where the source file is located.'
    ,LAST_LOAD_TIME TIMESTAMP_LTZ COMMENT 'Date and time of the load record. For bulk data loads, this is the time when the file started loading. For Snowpipe, this is the time when the file finished loading.'
    ,ROW_COUNT NUMBER COMMENT 'Number of rows loaded from the source file.'
    ,ROW_PARSED NUMBER COMMENT 'Number of rows parsed from the source file; NULL if STATUS is ‘LOAD_IN_PROGRESS’.'
    ,FILE_SIZE NUMBER COMMENT 'Size of the source file loaded.'
    ,FIRST_ERROR_MESSAGE TEXT COMMENT 'First error of the source file.'
    ,FIRST_ERROR_LINE_NUMBER NUMBER COMMENT 'Line number of the first error.'
    ,FIRST_ERROR_CHARACTER_POS NUMBER COMMENT 'Position of the first error character.'
    ,FIRST_ERROR_COLUMN_NAME TEXT COMMENT 'Column name of the first error.'
    ,ERROR_COUNT NUMBER COMMENT 'Number of error rows in the source file.'
    ,ERROR_LIMIT NUMBER COMMENT 'If the number of errors reaches this limit, then abort.'
    ,STATUS TEXT COMMENT 'Status: Load in progress, Loaded, Load failed, Partially loaded, or Load skipped.'
    ,TABLE_CATALOG_NAME TEXT COMMENT 'Name of the database in which the target table resides.'
    ,TABLE_SCHEMA_NAME TEXT COMMENT 'Name of the schema in which the target table resides.'
    ,TABLE_NAME TEXT COMMENT 'Name of the target table.'
    ,PIPE_CATALOG_NAME TEXT COMMENT 'Name of the database in which the pipe resides.'
    ,PIPE_SCHEMA_NAME TEXT COMMENT 'Name of the schema in which the pipe resides.'
    ,PIPE_NAME TEXT COMMENT 'Name of the pipe defining the load parameters; NULL for COPY statement loads.'
    ,PIPE_RECEIVED_TIME TIMESTAMP_LTZ COMMENT 'Date and time when the INSERT request for the file loaded through the pipe was received; NULL for COPY statement loads.'
    ,INTEGRATION_ID VARCHAR COMMENT 'ID key for providing uniqueness to the rows'
    ,EXECUTION_DATE TIMESTAMP_LTZ COMMENT 'CURRENT_TIMESTAMP for keeping track of table level LAST_LOAD_TIME'
);





CREATE OR REPLACE STREAM ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG_STREAM
ON TABLE ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG;



CREATE OR REPLACE TABLE ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG_STREAM_DUMP
LIKE ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG;





CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_SNOWFLAKE_COPY_HISTORY_LOG()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

function getTables() {
    sqlstr = `
        SELECT * from INFORMATION_SCHEMA.TABLES
        WHERE 1=1 
            AND TABLE_SCHEMA like '%_RAW'
            AND TABLE_NAME like '%_VARIANT'
            AND TABLE_TYPE = 'BASE TABLE'
            AND IS_TRANSIENT = 'NO'
        ;`;
    var statement = snowflake.createStatement({sqlText: sqlstr});
    var results = statement.execute();
    var tables = [];
    while (results.next()) {
        var TABLE_CATALOG = results.getColumnValue('TABLE_CATALOG');
        var TABLE_SCHEMA = results.getColumnValue('TABLE_SCHEMA');
        var TABLE_NAME = results.getColumnValue('TABLE_NAME');
        tables.push(TABLE_CATALOG + '.' + TABLE_SCHEMA + '.' + TABLE_NAME);
    }
    return tables;
};


function mergeLog(TABLE_NAME) {
    sqlstr = `
        MERGE INTO ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG LOG
        USING (
            SELECT *, 
                (CONCAT_WS(','
                    ,COALESCE(TO_VARCHAR(LAST_LOAD_TIME),'N/A')
                    ,COALESCE(TABLE_CATALOG_NAME,'N/A')
                    ,COALESCE(TABLE_SCHEMA_NAME,'N/A')
                    ,COALESCE(TABLE_NAME,'N/A')
                    )) AS INTEGRATION_ID
            FROM
                TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'` + TABLE_NAME + `', START_TIME=>DATEADD(HOURS, -24, CURRENT_TIMESTAMP())))
        ) HISTORY

        ON LOG.INTEGRATION_ID = HISTORY.INTEGRATION_ID

        WHEN NOT MATCHED THEN
        INSERT (
             FILE_NAME
            ,STAGE_LOCATION
            ,LAST_LOAD_TIME
            ,ROW_COUNT
            ,ROW_PARSED
            ,FILE_SIZE
            ,FIRST_ERROR_MESSAGE
            ,FIRST_ERROR_LINE_NUMBER
            ,FIRST_ERROR_CHARACTER_POS
            ,FIRST_ERROR_COLUMN_NAME
            ,ERROR_COUNT
            ,ERROR_LIMIT
            ,STATUS
            ,TABLE_CATALOG_NAME
            ,TABLE_SCHEMA_NAME
            ,TABLE_NAME
            ,PIPE_CATALOG_NAME
            ,PIPE_SCHEMA_NAME
            ,PIPE_NAME
            ,PIPE_RECEIVED_TIME
            ,INTEGRATION_ID
            ,EXECUTION_DATE
        ) VALUES (
             HISTORY.FILE_NAME
            ,HISTORY.STAGE_LOCATION
            ,HISTORY.LAST_LOAD_TIME
            ,HISTORY.ROW_COUNT
            ,HISTORY.ROW_PARSED
            ,HISTORY.FILE_SIZE
            ,HISTORY.FIRST_ERROR_MESSAGE
            ,HISTORY.FIRST_ERROR_LINE_NUMBER
            ,HISTORY.FIRST_ERROR_CHARACTER_POS
            ,HISTORY.FIRST_ERROR_COLUMN_NAME
            ,HISTORY.ERROR_COUNT
            ,HISTORY.ERROR_LIMIT
            ,HISTORY.STATUS
            ,HISTORY.TABLE_CATALOG_NAME
            ,HISTORY.TABLE_SCHEMA_NAME
            ,HISTORY.TABLE_NAME
            ,HISTORY.PIPE_CATALOG_NAME
            ,HISTORY.PIPE_SCHEMA_NAME
            ,HISTORY.PIPE_NAME
            ,HISTORY.PIPE_RECEIVED_TIME
            ,HISTORY.INTEGRATION_ID
            ,CURRENT_TIMESTAMP()
        );`;
    var statement = snowflake.createStatement({sqlText: sqlstr});
    var results = statement.execute();
    results.next();
    return results.getColumnValue(1);
};


function deleteOldRows() {
    sqlstr = `
    DELETE FROM ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG
    WHERE LAST_LOAD_TIME < DATEADD(HOURS, -24*5, CURRENT_TIMESTAMP())
    ;`;
    var statement = snowflake.createStatement({sqlText: sqlstr});
    var results = statement.execute();
    results.next();
    return results.getColumnValue(1);
    };


tables = getTables();

insertcount = 0
for (var i = 0; i < tables.length; i++) {
    insertcount += mergeLog(tables[i]);
    };

deletecount = deleteOldRows();

return insertcount + ' rows inserted. ' + deletecount + ' rows deleted.';

$$;








CREATE OR REPLACE TASK ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG_TASK
WAREHOUSE = {{ WAREHOUSE }}_RAW_WH
SCHEDULE = '120 minute'
AS CALL ELT_STAGE.USP_SNOWFLAKE_COPY_HISTORY_LOG();

ALTER TASK ELT_STAGE.SNOWFLAKE_COPY_HISTORY_LOG_TASK RESUME;





