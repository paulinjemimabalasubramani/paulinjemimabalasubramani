
USE EDIPIngestion
GO




CREATE TABLE metadata.DataSource
(
DataSourceId int Identity,
DataSourceKey varchar(1000),
Category varchar(100),
SubCategory varchar(100),
Firm varchar(100),
DataSourceType varchar(100),
DataProvider varchar(1000),
UpdatedBy varchar(1000),
UpdateTS datetime,
DataSourceUniqueKey varchar(1000)
);



INSERT INTO metadata.DataSource
(DataSourceKey, Category, SubCategory, Firm, DataSourceType, DataProvider, UpdatedBy, UpdateTS)
VALUES
('METRICS_AG_DATASOURCE_ASSETS_RAA', 'Metrics', 'Assets', 'RAA', 'csv_with_date', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_AG_DATASOURCE_ASSETS_WFS', 'Metrics', 'Assets', 'WFS', 'csv_with_date', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_AG_DATASOURCE_ASSETS_SPF', 'Metrics', 'Assets', 'SPF', 'csv_with_date', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_AG_DATASOURCE_FRONTPOINT', 'Assets', 'Frontpoint', '', 'assets_frontpoint', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_AG_DATASOURCE_PRODUCT', 'Assets', 'Product', '', 'csv', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('CA_AG_DATASOURCE_PERSHING_RAA', 'Client Account', 'Pershing Customer Account', 'RAA', 'pershing', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_AG_DATASOURCE_PERSHING_RAA', 'Assets', 'Pershing Assets', 'RAA', 'pershing', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_AG_DATASOURCE_ALBRIDGE_WFS', 'Assets', 'Albridge', 'WFS', 'assets_albridge', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),

('CA_AG_DATASOURCE_NBS', 'Client Account', 'NBS', '', 'csv', 'AG', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_AG_DATASOURCE_CLIENTODS', 'Client Account', 'ClientODS', '', 'csv', 'AG', 'Seymur M.', CURRENT_TIMESTAMP)

;



SELECT * FROM metadata.DataSource;







CREATE TABLE metadata.Pipeline
(
PipelineId int Identity,
PipelineKey varchar(1000),
PipelineCategory varchar(1000),
PipelineDescription varchar(1000),
DataSourceKey varchar(1000),
PreRunCode varchar(1000),
Schedule varchar(1000),
IsActive varchar(10),
UpdatedBy varchar(1000),
UpdateTS datetime
);




INSERT INTO metadata.Pipeline
(PipelineKey, PipelineCategory, PipelineDescription, DataSourceKey, PreRunCode, Schedule, IsActive, UpdatedBy, UpdateTS)
VALUES
('METRICS_MIGRATE_ASSETS_RAA', 'outbound_migration', 'Pipeline to migrate Assets to Snowflake for RAA', 'METRICS_AG_DATASOURCE_ASSETS_RAA', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'outbound_migration', 'Pipeline to migrate Assets to Snowflake for RAA', 'METRICS_AG_DATASOURCE_ASSETS_WFS', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'outbound_migration', 'Pipeline to migrate Assets to Snowflake for RAA', 'METRICS_AG_DATASOURCE_ASSETS_SPF', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_MIGRATE_FRONTPOINT', 'outbound_migration', 'Pipeline to migrate Assets-Frontpoint SAI and LTS files to Snowflake', 'ASSETS_AG_DATASOURCE_FRONTPOINT', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_MIGRATE_PRODUCT', 'outbound_migration', 'Pipeline to migrate Assets-Product files to Snowflake', 'ASSETS_AG_DATASOURCE_PRODUCT', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('CA_MIGRATE_PERSHING_RAA', 'outbound_migration', 'Pipeline to migrate CA-Pershing files to Snowflake for RAA', 'CA_AG_DATASOURCE_PERSHING_RAA', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_MIGRATE_PERSHING_RAA', 'outbound_migration', 'Pipeline to migrate ASSETS-Pershing files to Snowflake for RAA', 'ASSETS_AG_DATASOURCE_PERSHING_RAA', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('ASSETS_MIGRATE_ALBRIDGE_WFS', 'outbound_migration', 'Pipeline to migrate Assets-Albridge files to Snowflake for WFS', 'ASSETS_AG_DATASOURCE_ALBRIDGE_WFS', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),

('CA_MIGRATE_NBS', 'outbound_migration', 'Pipeline to migrate CA-NBS files to Snowflake', 'CA_AG_DATASOURCE_NBS', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'outbound_migration', 'Pipeline to migrate CA-ClientODS files to Snowflake', 'CA_AG_DATASOURCE_CLIENTODS', '', '0 13 * * *', '1', 'Seymur M.', CURRENT_TIMESTAMP)

;





SELECT * FROM metadata.Pipeline;







CREATE TABLE metadata.PipelineConfiguration
(
PipelineConfigurationId int Identity,
PipelineKey varchar(1000),
ConfigKey nvarchar(1000),
ConfigValue nvarchar(4000),
UpdatedBy varchar(1000),
UpdateTS datetime
);






INSERT INTO metadata.PipelineConfiguration 
(PipelineKey, ConfigKey, ConfigValue, UpdatedBy, UpdateTS)
VALUES
('GENERIC', 'CREATE_DDL_FILES', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'OUTPUT_LOG_PATH', '/usr/local/spark/resources/fileshare/Shared/logs', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'OUTPUT_DDL_PATH', '/usr/local/spark/resources/fileshare/Shared/DDL', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'TEMPORARY_FILE_PATH', '/usr/local/spark/resources/Shared/TEMP', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'APP_DATA_PATH', '/usr/local/spark/resources/fileshare/Shared/APPDATA', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'SNOWFLAKE_ACCOUNT', 'advisorgroup-edip', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'SNOWFLAKE_KEY_VAULT_ACCOUNT', 'snowflake', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'SNOWFLAKE_ROLE', 'AD_SNOWFLAKE_QA_DBA', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'SNOWFLAKE_WAREHOUSE', 'QA_RAW_WH', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_STORAGE_ACCOUNTS_PREFIX', 'ag', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_STORAGE_ACCOUNTS_DEFAULT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_STORAGE_ACCOUNTS_SUFFIX', 'lakescd', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_CONTAINER_NAME', 'ingress', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_DATA_FOLDER', 'data', 'Seymur M.', CURRENT_TIMESTAMP),
('GENERIC', 'AZURE_METADATA_FOLDER', 'metadata', 'Seymur M.', CURRENT_TIMESTAMP),


('METRICS_MIGRATE_ASSETS_RAA', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/METRICS_ASSETS/RAA', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'DB_NAME', 'METRICS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'SCHEMA_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'FILE_HISTORY_START_DATE', '2021-08-15', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'DATE_FORMAT', '%Y%m%d', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_RAA', 'SQL_FILE_HISTORY_TABLE', 'metrics_assets_file_history3', 'Seymur M.', CURRENT_TIMESTAMP),

('METRICS_MIGRATE_ASSETS_WFS', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/METRICS_ASSETS/WFS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'DB_NAME', 'METRICS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'SCHEMA_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'FILE_HISTORY_START_DATE', '2021-08-15', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'DATE_FORMAT', '%Y%m%d', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_WFS', 'SQL_FILE_HISTORY_TABLE', 'metrics_assets_file_history3', 'Seymur M.', CURRENT_TIMESTAMP),

('METRICS_MIGRATE_ASSETS_SPF', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/METRICS_ASSETS/SPF', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'DB_NAME', 'METRICS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'SCHEMA_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'FILE_HISTORY_START_DATE', '2021-08-15', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'DATE_FORMAT', '%Y%m%d', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('METRICS_MIGRATE_ASSETS_SPF', 'SQL_FILE_HISTORY_TABLE', 'metrics_assets_file_history3', 'Seymur M.', CURRENT_TIMESTAMP),


('ASSETS_MIGRATE_FRONTPOINT', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/FRONTPOINT', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'DB_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'SCHEMA_NAME', 'FRONTPOINT', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'FILE_HISTORY_START_DATE', '2021-12-13', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'SCHEMA_FILE_PATH', '/usr/local/spark/resources/fileshare/EDIP-Code/config/assets/frontpoint_schema/frontpoint_schema.csv', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_FRONTPOINT', 'SQL_FILE_HISTORY_TABLE', 'assets_frontpoint_file_history3', 'Seymur M.', CURRENT_TIMESTAMP),


('ASSETS_MIGRATE_PRODUCT', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/AG_ASSETS_PRODUCT', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'DB_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'SCHEMA_NAME', 'PRODUCT', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'FILE_HISTORY_START_DATE', '2022-01-01', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PRODUCT', 'SQL_FILE_HISTORY_TABLE', 'assets_product_file_history3', 'Seymur M.', CURRENT_TIMESTAMP),


('CA_MIGRATE_PERSHING_RAA', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/PERSHING-CA/RAA', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'DB_NAME', 'CA', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'SCHEMA_NAME', 'PERSHING', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'AZURE_STORAGE_ACCOUNT_MID', 'raa', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'FILE_HISTORY_START_DATE', '2021-08-15', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'SCHEMA_FILE_PATH', '/usr/local/spark/resources/fileshare/EDIP-Code/config/pershing_schema', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'ADD_FIRM_TO_TABLE_NAME', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'START_LINE_RECORD_POSITION', '3', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_PERSHING_RAA', 'START_LINE_RECORD_STRING', 'A', 'Seymur M.', CURRENT_TIMESTAMP),



('ASSETS_MIGRATE_PERSHING_RAA', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/PERSHING-ASSETS/RAA', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'DB_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'SCHEMA_NAME', 'PERSHING', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'AZURE_STORAGE_ACCOUNT_MID', 'raa', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'FILE_HISTORY_START_DATE', '2021-08-15', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'SCHEMA_FILE_PATH', '/usr/local/spark/resources/fileshare/EDIP-Code/config/pershing_schema', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'ADD_FIRM_TO_TABLE_NAME', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'START_LINE_RECORD_POSITION', '3', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_PERSHING_RAA', 'START_LINE_RECORD_STRING', 'A', 'Seymur M.', CURRENT_TIMESTAMP),



('ASSETS_MIGRATE_ALBRIDGE_WFS', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/ALBRIDGE/WFS', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'DB_NAME', 'ASSETS', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'SCHEMA_NAME', 'ALBRIDGE', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'AZURE_STORAGE_ACCOUNT_MID', 'wfs', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'FILE_HISTORY_START_DATE', '2021-10-15', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'SCHEMA_FILE_PATH', '/usr/local/spark/resources/fileshare/EDIP-Code/config/assets/albridge_schema/albridge_schema.csv', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'FIN_INST_ID', '63', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'DATE_FORMAT', '%Y%m%d', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_ALBRIDGE_WFS', 'ADD_FIRM_TO_TABLE_NAME', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),


('CA_MIGRATE_NBS', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/NBS', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_NBS', 'DB_NAME', 'CA', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_NBS', 'SCHEMA_NAME', 'NBS', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_NBS', 'FILE_HISTORY_START_DATE', '2022-01-01', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_NBS', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_NBS', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),


('CA_MIGRATE_CLIENTODS', 'SOURCE_PATH', '/usr/local/spark/resources/fileshare/Shared/CLIENTODS', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'DB_NAME', 'CA', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'SCHEMA_NAME', 'CLIENTODS', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'FILE_HISTORY_START_DATE', '2022-01-01', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'AZURE_STORAGE_ACCOUNT_MID', 'aggr', 'Seymur M.', CURRENT_TIMESTAMP),
('CA_MIGRATE_CLIENTODS', 'IS_FULL_LOAD', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),





('ASSETS_MIGRATE_DATASTORE_ALBRIDGE_RAA', 'ADD_FIRM_TO_TABLE_NAME', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP),
('ASSETS_MIGRATE_DATASTORE_ALBRIDGE_WFS', 'ADD_FIRM_TO_TABLE_NAME', 'TRUE', 'Seymur M.', CURRENT_TIMESTAMP)
;




select * from metadata.PipelineConfiguration
where PipelineKey = 'ASSETS_MIGRATE_DATASTORE_ALBRIDGE_WFS'
;







SELECT TOP 1 p.*, ds.firm, ds.DataSourceType 
FROM metadata.Pipeline p
	LEFT JOIN metadata.DataSource ds ON p.DataSourceKey = ds.DataSourceKey
WHERE p.IsActive = 1
	and p.PipelineKey = 'METRICS_DATASTORE_MIGRATE_RAA'
ORDER BY p.UpdateTs DESC, p.PipelineId DESC
;









select * from metadata.PrimaryKey;



insert into metadata.PrimaryKey
(	[RevisionSeq],
	[DataSource],
	[AssetSchema],
	[AssetName],
	[AssetUniqueKey],
	[SystemPrimaryKey],
	[BusinessKey],
	[CreateTS],
	[CreatedBy],
	[UpdateTS],
	[UpdatedBy],
	[DomainName],
	[DataSourceUniqueKey]

)
VALUES
(
1,
'METRICS',
'ASSETS',
'ACCOUNT',
'ACCOUNT',
'AccountId',
'',
CURRENT_TIMESTAMP,
'Seymur M.',
CURRENT_TIMESTAMP,
'Seymur M.',
'METRICS',
'METRICS.ASSETS'
)

;
