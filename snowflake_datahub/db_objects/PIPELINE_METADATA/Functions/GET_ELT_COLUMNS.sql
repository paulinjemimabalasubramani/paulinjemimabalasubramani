CREATE OR REPLACE FUNCTION DATAHUB.PIPELINE_METADATA.GET_ELT_COLUMNS("TABLE_TYPE" VARCHAR, "COLUMN_DATATYPE" VARCHAR)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS '
var elt_columns;

if (TABLE_TYPE.toUpperCase() === "STAGING") {
    if (COLUMN_DATATYPE.toUpperCase() === "Y") {
        elt_columns = ["PIPELINE_RUN_ID VARCHAR", "FILE_ROW_NUMBER NUMBER"];
    } else {
        elt_columns = ["PIPELINE_RUN_ID", "FILE_ROW_NUMBER"];
    }
} else if (TABLE_TYPE.toUpperCase() === "BRONZE") {
    if (COLUMN_DATATYPE.toUpperCase() === "Y") {
        elt_columns = [
            "ELT_PIPELINE_KEY VARCHAR",
            "ELT_PIPELINE_RUN_ID VARCHAR",
            "ELT_FILE_ROW_NUMBER NUMBER",
            "ELT_BUSINESS_DATE TIMESTAMP_NTZ",
            "ELT_SOURCE_SYSTEM_CODE VARCHAR",
            "ELT_SUBJECT_AREA_CODE VARCHAR",
            "ELT_LOAD_TYPE VARCHAR",
            "ELT_PRIMARY_KEY_HASH VARCHAR",
            "ELT_COLUMNS_HASH VARCHAR",
            "ELT_FILE_ID NUMBER",
            "ELT_EXECUTION_TS TIMESTAMP_NTZ",
            "ELT_CREATED_BY_NAME VARCHAR",
            "ELT_CREATED_TS TIMESTAMP_NTZ",
            "ELT_UPDATED_BY_NAME VARCHAR",
            "ELT_UPDATED_TS TIMESTAMP_NTZ"
        ];
    } else {
        elt_columns = [
            "ELT_PIPELINE_KEY",
            "ELT_PIPELINE_RUN_ID",
            "ELT_FILE_ROW_NUMBER",
            "ELT_BUSINESS_DATE",
            "ELT_SOURCE_SYSTEM_CODE",
            "ELT_SUBJECT_AREA_CODE",
            "ELT_LOAD_TYPE",
            "ELT_PRIMARY_KEY_HASH",
            "ELT_COLUMNS_HASH",
            "ELT_FILE_ID",
            "ELT_EXECUTION_TS",
            "ELT_CREATED_BY_NAME",
            "ELT_CREATED_TS",
            "ELT_UPDATED_BY_NAME",
            "ELT_UPDATED_TS"
        ];
    }
} else {
    elt_columns = [];
}
return elt_columns;
';