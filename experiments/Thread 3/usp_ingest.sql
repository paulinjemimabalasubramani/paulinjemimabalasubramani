USE SCHEMA ELT_STAGE; 


CREATE OR REPLACE STAGE ELT_STAGE.AGGR_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://agaggrlakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.AGGR_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the AGGR DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_AGGR_BLOB_INT
URL = 'azure://agaggrlakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.FSC_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the FSC DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_FSC_BLOB_INT
URL = 'azure://agfsclakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.FSC_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the FSC DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_FSC_BLOB_INT
URL = 'azure://agfsclakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.RAA_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the RAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_RAA_BLOB_INT
URL = 'azure://agraalakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.RAA_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the RAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_RAA_BLOB_INT
URL = 'azure://agraalakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAI_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAI DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAI_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAI DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAA_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SAA_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SAA DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SAI_BLOB_INT
URL = 'azure://agsailakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SPF_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SPF DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SPF_BLOB_INT
URL = 'azure://agspflakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.SPF_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the SPF DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_SPF_BLOB_INT
URL = 'azure://agspflakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.TRD_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the TRD DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_TRD_BLOB_INT
URL = 'azure://agtrdlakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.TRD_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the TRD DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_TRD_BLOB_INT
URL = 'azure://agtrdlakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.WFS_METRICS_METADATA_DATALAKE
COMMENT = 'External Stage for the DBA Account on the WFS DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_WFS_BLOB_INT
URL = 'azure://agwfslakescd.blob.core.windows.net/ingress/metadata/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;


CREATE OR REPLACE STAGE ELT_STAGE.WFS_METRICS_DATALAKE
COMMENT = 'External Stage for the DBA Account on the WFS DataLake Blob Container'
STORAGE_INTEGRATION  = {{ ENV }}_WFS_BLOB_INT
URL = 'azure://agwfslakescd.blob.core.windows.net/ingress/data/metrics/'
FILE_FORMAT = (TYPE='PARQUET') ;





create or replace TABLE INGEST_REQUEST_VARIANT (
	SRC VARIANT
);


create or replace stream INGEST_REQUEST_VARIANT_STREAM on table INGEST_REQUEST_VARIANT;



CREATE OR REPLACE TABLE ELT_STAGE.INGEST_REQUEST
(
	UUID   STRING
    ,SOURCE_INSERTED_AT  TIMESTAMP
    ,TARGET_INSERTED_AT  TIMESTAMP
    ,EXECUTION_DATE  TIMESTAMP
    ,INGEST_STAGE_NAME  STRING
    ,FULL_OBJECT_NAME   STRING
    ,ELT_STAGE_SCHEMA   STRING
    ,INGEST_SCHEMA  STRING
    ,COPY_COMMAND   STRING
    ,SOURCE_SYSTEM STRING
) ;



create or replace TABLE ELT_COPY_EXCEPTION (
	SOURCE_SYSTEM VARCHAR(16777216),
	TARGET_TABLE VARCHAR(16777216),
	ELT_BATCH_ID NUMBER(38,0),
	ERROR VARCHAR(16777216),
	FILE VARCHAR(16777216),
	LINE NUMBER(38,0),
	CHARACTER NUMBER(38,0),
	BYTE_OFFSET NUMBER(38,0),
	CATEGORY VARCHAR(16777216),
	CODE NUMBER(38,0),
	SQL_STATE NUMBER(38,0),
	COLUMN_NAME VARCHAR(16777216),
	ROW_NUMBER NUMBER(38,0),
	ROW_START_LINE NUMBER(38,0),
	REJECTED_RECORD VARCHAR(16777216),
	EXECEPTION_CREATED_BY_USER VARCHAR(16777216),
	EXECEPTION_CREATED_BY_ROLE VARCHAR(16777216),
	EXCEPTION_SESSION VARCHAR(16777216),
	EXCEPTION_DATE_TIME TIMESTAMP_NTZ(9)
);










CREATE OR REPLACE PIPE AGGR_METRICS_ASSETS_INGEST_REQUEST_PIPE auto_ingest=false as COPY INTO {{ ENV }}_METRICS.ELT_STAGE.INGEST_REQUEST_VARIANT
FROM @{{ ENV }}_METRICS.ELT_STAGE.AGGR_METRICS_METADATA_DATALAKE/ASSETS/ingest_data/
file_format = (type = 'PARQUET')
PATTERN = '.*.parquet' 
ON_ERROR = CONTINUE;














CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_INGEST(
  COPY_COMMAND VARCHAR,
  SOURCE_SYSTEM VARCHAR,
  EXECUTION_DATE VARCHAR,
  FULL_OBJECT_NAME VARCHAR,
  INGEST_SCHEMA VARCHAR,
  ELT_STAGE_SCHEMA VARCHAR
)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
var return_value = "";

var sql_string = "";

var exception_sp_name = "USP_INGESTION_EXCEPTION";
var target_table_name = "";
var source_table_name = "";
var source_system = "";

function getSQLParameter(p) {
    var returnP = "'";
    returnP = returnP + p + "'";    
    return returnP;
}

function executeBatchSql(query_array) {
    var resultString = '';
    for (var i = 0; i < query_array.length; i++) 
    {
        try
        {
          exec_cmd_dict = {sqlText: query_array[i]};
          exec_stmt = snowflake.createStatement(exec_cmd_dict);
          exec_stmt.execute();
          resultString += query_array[i] + " --Succeeded" + "\\n";
        }
        catch(err) 
        {
                resultString += query_array[i] + " --Failed: " + err.message.replace(/\\n/g, " ") + "\\n";                
        }
    }
    return resultString;
}


var query_array = [];
return_value = "";
var elt_stage_schema = ELT_STAGE_SCHEMA;
var table_name = FULL_OBJECT_NAME;
var source_system = SOURCE_SYSTEM ;
var ingest_schema = INGEST_SCHEMA;
var target_table_name = ingest_schema + "." + table_name + "_VARIANT";
var source_table_name = ingest_schema + "." + table_name + "_VARIANT";

//Truncate Variant Table Command
query_array[0] = "TRUNCATE TABLE " + target_table_name + ";";

//Copy Command
query_array[1] = COPY_COMMAND; 

//Exception Capture
query_array[2] = "CALL " + elt_stage_schema + "." + exception_sp_name + "(" + getSQLParameter(source_system) + "," +getSQLParameter(target_table_name) + ");"; 


resultString = executeBatchSql(query_array);
return resultString;
$$;






CREATE OR REPLACE PROCEDURE USP_INGESTION_EXCEPTION(SOURCE_SYSTEM VARCHAR(16777216), TARGET_TABLE VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

function executeBatchSql(sqlCommand) {
      
      var resultString = '';      
      
      
          try{
                 cmd_input_dict = {sqlText: sqlCommand};
                 stmt = snowflake.createStatement(cmd_input_dict);
                 rs = stmt.execute();   
                 resultString +=   " --Succeeded" + "\\n";
             }
          catch(err) {
                resultString +=  " --Failed: " + err.message.replace(/\\n/g, " ") + "\\n";                
          }
     
      return resultString;

}


  var SQL_COMMAND =  "SET SOURCE_SYSTEM = '" + SOURCE_SYSTEM + "';" 
	SQL_COMMAND += "SET TARGET_TABLE = '" + TARGET_TABLE + "';" 
	var SQL_COMMAND = `INSERT INTO ELT_STAGE.ELT_COPY_EXCEPTION
					(
					   SOURCE_SYSTEM
					  ,TARGET_TABLE
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
 SQL_COMMAND += "'" + SOURCE_SYSTEM + "','" + TARGET_TABLE +"',"
					   
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
        var resultString = executeBatchSql(SQL_COMMAND);
        return resultString;
    }
	catch (err)  {
		return "Failed: " + err;
    }
$$;





CREATE OR REPLACE TASK ELT_STAGE.INGEST_REQUEST_TASK
WAREHOUSE = {{ ENV }}_RAW_WH
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('ELT_STAGE.INGEST_REQUEST_VARIANT_STREAM')
AS
    CALL ELT_STAGE.USP_INGEST(); 



ALTER TASK ELT_STAGE.INGEST_REQUEST_TASK RESUME ; 






