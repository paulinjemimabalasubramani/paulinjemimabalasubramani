create or replace TABLE DATAHUB.PIPELINE_METADATA.SQL_SOURCE_MAPPING_DETAILS (
	TABLE_CATALOG VARCHAR(50) COMMENT 'TABLE_CATALOG is the database name',
	TABLE_SCHEMA VARCHAR(50) COMMENT 'TABLE_SCHEMA is the schema name of the table',
	TABLE_NAME VARCHAR(50) COMMENT 'TABLE_NAME is the name of the table',
	COLUMN_NAME VARCHAR(50) COMMENT 'COLUMN_NAME is the name of the column',
	SQL_DATATYPE VARCHAR(50) COMMENT 'SQL_Datatype is the dataype of the sql',
	CHARACTER_MAXIMUM_LENGTH VARCHAR(50) COMMENT 'CHARACTER_MAXIMUM_LENGTH is the length of the characters',
	NUMERIC_PRECISION VARCHAR(50) COMMENT 'NUMERIC_PRECISION is the precision of numeric data types ',
	NUMERIC_SCALE VARCHAR(50) COMMENT 'NUMERIC_SCALE is the scale of numeric data types',
	IS_NULLABLE VARCHAR(50) COMMENT 'IS_NULLABLE indicate nullability details for each column',
	ORDINAL_POSITION VARCHAR(50) COMMENT 'ORDINAL_POSITION is a position of a column'
);