Use Schema ELT_STAGE;

/*
CREATE OR REPLACE STAGE ELT_STAGE.AGGR_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://agaggrlakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.AGGR_CLIENTACCOUNT_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://agaggrlakescd.blob.core.windows.net/ingress/metadata/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.FSC_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the FSC DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_FSC_BLOB_INT
URL = 'azure://agfsclakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.RAA_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the RAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_RAA_BLOB_INT
URL = 'azure://agraalakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAI_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAI DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAA_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SPF_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SPF DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SPF_BLOB_INT
URL = 'azure://agspflakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.TRD_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the TRD DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_TRD_BLOB_INT
URL = 'azure://agtrdlakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.WFS_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the WFS DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_WFS_BLOB_INT
URL = 'azure://agwfslakescd.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;

*/

CREATE OR REPLACE STAGE ELT_STAGE.AGGR_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://staggrprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.AGGR_CLIENTACCOUNT_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://staggrprmyedip.blob.core.windows.net/ingress/metadata/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.FSC_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the FSC DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_FSC_BLOB_INT
URL = 'azure://stfscprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.RAA_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the RAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_RAA_BLOB_INT
URL = 'azure://straaprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAI_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAI DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://stsaiprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAA_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://stsaiprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SPF_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SPF DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SPF_BLOB_INT
URL = 'azure://stspfprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.TRD_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the TRD DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_TRD_BLOB_INT
URL = 'azure://sttrdprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.WFS_CLIENTACCOUNT_DATALAKE
COMMENT = 'External Stage for the DBA Account on the WFS DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_WFS_BLOB_INT
URL = 'azure://stwfsprmyedip.blob.core.windows.net/ingress/data/client_account/'
FILE_FORMAT = (TYPE='PARQUET') ;











CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.INGEST_REQUEST
(
	 ID NUMBER(38, 0) NOT NULL AUTOINCREMENT PRIMARY KEY
    ,COPY_COMMAND_RUNTIME TIMESTAMP
    ,EXECUTION_DATE TIMESTAMP
    ,INGEST_STAGE_NAME VARCHAR
    ,VARIANT_TABLE_NAME VARCHAR
	,TABLE_NAME VARCHAR
	,DATABASE_NAME VARCHAR
    ,COPY_COMMAND VARCHAR
    ,ELT_PROCESS_ID VARCHAR
) ;





CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.ELT_COPY_EXCEPTION (
	ID NUMBER(38, 0) NOT NULL AUTOINCREMENT PRIMARY KEY,
	ELT_PROCESS_ID VARCHAR,
	VARIANT_TABLE_NAME VARCHAR,
	ERROR VARCHAR,
	FILE VARCHAR,
	LINE NUMBER(38,0),
	CHARACTER NUMBER(38,0),
	BYTE_OFFSET NUMBER(38,0),
	CATEGORY VARCHAR,
	CODE NUMBER(38,0),
	SQL_STATE NUMBER(38,0),
	COLUMN_NAME VARCHAR,
	ROW_NUMBER NUMBER(38,0),
	ROW_START_LINE NUMBER(38,0),
	REJECTED_RECORD VARCHAR,
	EXECEPTION_CREATED_BY_USER VARCHAR,
	EXECEPTION_CREATED_BY_ROLE VARCHAR,
	EXCEPTION_SESSION VARCHAR,
	EXCEPTION_DATE_TIME TIMESTAMP_NTZ(9)
);






CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.INGESTION_RESULT_LOG
(
ID NUMBER(38, 0) NOT NULL AUTOINCREMENT PRIMARY KEY,
Failures NUMBER,
Successes NUMBER,
Runtime TIMESTAMP,
USPname VARCHAR,
CurrentUser VARCHAR,
CurrentRole VARCHAR,
LogDetails VARIANT
);





CREATE TABLE IF NOT EXISTS ELT_STAGE.CURATION_STREAMS_METADATA
(
ID NUMBER(38, 0) NOT NULL AUTOINCREMENT PRIMARY KEY,
PROCEDURE_NAME VARCHAR,
TABLE_NAME VARCHAR,
STREAM_NAME VARCHAR,
CREATED_DATE TIMESTAMP,
CREATED_BY VARCHAR
);





CREATE OR REPLACE TRANSIENT TABLE ELT_STAGE.CURATION_STREAM_DUMP (ID NUMBER);










CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_INGESTION_RESULT(USP VARCHAR, QUERIES ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$

function fixQuote(sql) { return sql.replace(/'/g, "''").replace(/\\n|\\r/g, " ") }
//'

function executeBatchSql(query_array) {
    var aggResults = [];
    var failures = 0;
    var successes = 0;
    var Msg = "SUCCEEDED";
	for (var i = 0; i < query_array.length; i++) {
        var stResults = [];
		try {
			execObj = snowflake.execute({sqlText: query_array[i]});
            if (execObj.getRowCount()>0) { 
                execObj.next()
                for (var j = 0; j < execObj.getColumnCount(); j++) {
                    stResults.push({[execObj.getColumnName(j+1)]: execObj.getColumnValueAsString(j+1)})
                }
            }
            outcome = "SUCCEEDED";
            outMsg = stResults;
            successes += 1;
		} catch(err) {
            outcome = "FAILED";
            outMsg = err.message.replace(/\n/g, " ");
            failures += 1;
		}
        aggResults.push({
            "QUERY": query_array[i],
            "QUERYID": execObj.getQueryId(),
            "OUTPUT": outMsg,
            "RESULT": outcome
            });
    }
	return [JSON.stringify(aggResults), failures, successes];
}


results = executeBatchSql(QUERIES);

aggResults = results[0];
failures = results[1];
successes = results[2];

sql_log = `INSERT INTO ELT_STAGE.INGESTION_RESULT_LOG 
(Failures, Successes, Runtime, USPname, CurrentUser, CurrentRole, LogDetails)
SELECT `+failures.toString()+`, `+successes.toString()+`, CURRENT_TIMESTAMP(), '`+USP+`', CURRENT_USER(), CURRENT_ROLE(), PARSE_JSON('`+fixQuote(aggResults)+`');
`;

try {
    execObj = snowflake.execute({sqlText: sql_log});
} catch(err) {
    return "--FAILED INSERTING LOG DATA: " + err.message.replace(/\n/g, " ") + "--SQL LOG COMMAND: " + sql_log;
}

return `Failures: `+failures.toString()+`, Successes: `+successes.toString();
$$;









CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_SOFT_DELETE_RAW(VARIANT_TABLE VARCHAR, NONVARIANT_TABLE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

var get_row_count = `
	SELECT COUNT(1) AS ROWCOUNT
	FROM `+VARIANT_TABLE+` V
	WHERE V.SRC:elt_delete_ind = 0
		AND V.SRC:elt_load_type = 'FULL'
`;

var sql_update = `
	UPDATE `+NONVARIANT_TABLE+` T
	SET T.ELT_DELETE_IND = 1
	FROM (
		SELECT NV.ELT_PRIMARY_KEY AS ELT_PRIMARY_KEY
		FROM `+NONVARIANT_TABLE+` NV
			LEFT JOIN `+VARIANT_TABLE+` V
				ON NV.ELT_PRIMARY_KEY = V.SRC:elt_primary_key
		WHERE V.SRC:elt_primary_key IS NULL
			AND NV.ELT_DELETE_IND != 1
	) D,
	(`+get_row_count+`) N
	WHERE T.ELT_PRIMARY_KEY = D.ELT_PRIMARY_KEY
		AND N.ROWCOUNT>0
	;
`;

var sql_update_execute = snowflake.execute({sqlText: sql_update})
sql_update_execute.next()

return "Number of Rows updated: " + sql_update_execute.getColumnValue(1).toString();

$$;











CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_INGESTION_EXCEPTION(ELT_PROCESS_ID VARCHAR, VARIANT_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

function executeBatchSql(sqlCommand) {
      
      var resultVARCHAR = '';      
      
      
          try{
                 cmd_input_dict = {sqlText: sqlCommand};
                 stmt = snowflake.createStatement(cmd_input_dict);
                 rs = stmt.execute();   
                 resultVARCHAR +=   " --Succeeded\n";
             }
          catch(err) {
                resultVARCHAR +=  " --Failed: " + err.message.replace(/\n/g, " ") + "\n";                
          }
     
      return resultVARCHAR;

}


	var SQL_COMMAND = `INSERT INTO ELT_STAGE.ELT_COPY_EXCEPTION
					(
					   ELT_PROCESS_ID
					  ,VARIANT_TABLE_NAME
					  ,EXCEPTION_DATE_TIME
					  ,EXECEPTION_CREATED_BY_USER
					  ,EXECEPTION_CREATED_BY_ROLE
					  ,EXCEPTION_SESSION
					  ,ERROR
					  ,FILE
					  ,LINE
					  ,CHARACTER
					  ,BYTE_OFFSET
					  ,CATEGORY
					  ,CODE
					  ,SQL_STATE
					  ,COLUMN_NAME
					  ,ROW_NUMBER
					  ,ROW_START_LINE
					  ,REJECTED_RECORD
					)
					SELECT `
 SQL_COMMAND += "'" + ELT_PROCESS_ID + "','" + VARIANT_TABLE_NAME +"',"
					   
	SQL_COMMAND +=	  `CURRENT_TIMESTAMP()
					  ,CURRENT_USER()
					  ,CURRENT_ROLE()
					  ,CURRENT_SESSION()
					  ,ERROR
					  ,FILE
					  ,LINE
					  ,CHARACTER
					  ,BYTE_OFFSET
					  ,CATEGORY
					  ,CODE
					  ,SQL_STATE
					  ,COLUMN_NAME
					  ,ROW_NUMBER
					  ,ROW_START_LINE
					  ,REJECTED_RECORD
					FROM TABLE(validate(`
	SQL_COMMAND += TARGET_TABLE + ", job_id => '_last'));"
	
	try {
        var resultVARCHAR = executeBatchSql(SQL_COMMAND);
        return resultVARCHAR;
    }
	catch (err)  {
		return "Failed: " + err;
    }
$$;












CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_INGEST(
COPY_COMMAND VARCHAR,
VARIANT_TABLE_NAME VARCHAR,
TABLE_NAME VARCHAR,
DATABASE_NAME VARCHAR,
EXECUTION_DATE VARCHAR,
INGEST_STAGE_NAME VARCHAR,
ELT_PROCESS_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

function executeBatchSql(query_array) {
	var results = '';
	for (var i = 0; i < query_array.length; i++)
		{
		try{
			exec_stmt = snowflake.execute({sqlText: query_array[i]});
			results += query_array[i] + " --Succeeded\n";
			}

		catch(err){
		results += query_array[i] + " --Failed: " + err.message.replace(/\n/g, " ") + "\n";
			}
		}
	return results;
}


var query_array = [];


//Set session query id
query_array.push("ALTER SESSION SET QUERY_TAG = '"+ELT_PROCESS_ID+"';");


//Truncate Variant Table Command
query_array.push("TRUNCATE TABLE " + VARIANT_TABLE_NAME + ";");


//Copy Command
query_array.push(COPY_COMMAND);


//Exception Capture
query_array.push("CALL ELT_STAGE.USP_INGESTION_EXCEPTION('"+ELT_PROCESS_ID+"','"+VARIANT_TABLE_NAME+"');");


//INGEST_REQUEST Log capture
query_array.push(`
INSERT INTO ELT_STAGE.INGEST_REQUEST
(COPY_COMMAND_RUNTIME, EXECUTION_DATE, INGEST_STAGE_NAME, VARIANT_TABLE_NAME, TABLE_NAME, DATABASE_NAME, COPY_COMMAND, ELT_PROCESS_ID)
VALUES
(current_timestamp, '`+EXECUTION_DATE+`', '`+INGEST_STAGE_NAME+`', '`+VARIANT_TABLE_NAME+`', '`+TABLE_NAME+`', '`+DATABASE_NAME+`',  '`+COPY_COMMAND.replace(/'/g, "''")+`', '`+ELT_PROCESS_ID+`');
`);

//'

results = executeBatchSql(query_array);
return results;
$$;













CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_CURATION_STREAM_DUMP(PROCEDURE_NAME VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

var sql_get_streams = `SELECT STREAM_NAME FROM ELT_STAGE.CURATION_STREAMS_METADATA WHERE UPPER(PROCEDURE_NAME) = UPPER('`+PROCEDURE_NAME+`');`;
get_streams = snowflake.execute({sqlText: sql_get_streams});

dump_list = [];
var failures = 0;
var successes = 0;
nstreams = get_streams.getRowCount();

for (var i = 0; i < nstreams; i++) {
    get_streams.next();
    stream_name = get_streams.getColumnValueAsString('STREAM_NAME');
    sql_dump_stream = `INSERT INTO ELT_STAGE.CURATION_STREAM_DUMP SELECT 1 FROM `+stream_name+` WHERE 1=0;`;
    try {
        dump_stream = snowflake.execute({sqlText: sql_dump_stream});
        dump_list.push({[stream_name]: "SUCCEEDED"});
        successes += 1;
    } catch(err) {
        dump_list.push({[stream_name]: "FAILED: " + err.message.replace(/\n/g, " ")});
        failures += 1;
    }
}

return JSON.stringify({
    "Failures": failures, 
    "Successes": successes,
    "Details": dump_list
    });
$$;








