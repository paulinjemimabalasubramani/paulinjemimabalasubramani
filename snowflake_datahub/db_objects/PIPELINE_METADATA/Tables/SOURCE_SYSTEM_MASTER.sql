create or replace TABLE DATAHUB.PIPELINE_METADATA.SOURCE_SYSTEM_MASTER (
	SOURCE_SYS_CODE VARCHAR(100) NOT NULL COMMENT 'Source System code -FINRA- NFS',
	SOURCE_SYS_NAME VARCHAR(100) COMMENT 'Source System Name- FINRA-National Financial services (NFS)',
	SOURCE_SYS_DESC VARCHAR(5000) COMMENT 'Source System Description- FINRA Regulatory Data: (',
	SOURCE_TYPE_TEXT VARCHAR(50) COMMENT 'Internal or External Source- External/ Internal',
	ACTIVE_FLG VARCHAR(1) COMMENT 'Active Flg is of Source_System_Master-''Y/N'' Comments',
	COMMENTS VARCHAR(1000) COMMENT 'Comments',
	CREATED_BY_NAME VARCHAR(100) COMMENT 'Created By Service Account',
	CREATED_TS TIMESTAMP_NTZ(9) COMMENT 'Created Date',
	UPDATED_BY_NAME VARCHAR(100) COMMENT 'Updated_By_Service account',
	UPDATED_TS TIMESTAMP_NTZ(9) COMMENT 'Updated Date time',
	constraint SOURCE_SYSTEM_MASTER_KEY primary key (SOURCE_SYS_CODE)
);