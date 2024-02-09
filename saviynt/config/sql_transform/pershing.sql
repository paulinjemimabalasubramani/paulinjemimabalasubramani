CREATE VIEW [dbo].[pershing_spat_e] AS
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [user_id],
    [introducing_broker_dealer_ibd_number],
    [user_first_name],
    [user_last_name],
    [id_creation_date],
    [effective_start_date_of_user_job_content_entitlement],
    [effective_end_date_of_user_content_entitlement],
    [job_function_entitlement_status_code],
    [user_id_of_administrator],
    [user_job_function_entitlement_update_date],
    [for_pershing_internal_use_only],
    [job_function_entitlement_name],
    [job_function_entitlement_description],
    [template_owner],
    [job_function_template_alternate_key_id],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_raa_spat_e] WHERE [meta_is_current] = 1
UNION ALL
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [user_id],
    [introducing_broker_dealer_ibd_number],
    [user_first_name],
    [user_last_name],
    [id_creation_date],
    [effective_start_date_of_user_job_content_entitlement],
    [effective_end_date_of_user_content_entitlement],
    [job_function_entitlement_status_code],
    [user_id_of_administrator],
    [user_job_function_entitlement_update_date],
    [for_pershing_internal_use_only],
    [job_function_entitlement_name],
    [job_function_entitlement_description],
    [template_owner],
    [job_function_template_alternate_key_id],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_sai_spat_e] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[pershing_spat_j] AS
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [for_pershing_internal_use_only],
    [introducing_broker_dealer_ibd_number],
    [job_function_name],
    [effective_start_date_of_job_function],
    [effective_end_date_of_user_content_entitlement],
    [job_function_create_or_update_date],
    [user_id_of_administrator],
    [category_name],
    [id_unique_bfe_identifier],
    [bfe_name],
    [bfe_access_level],
    [bfe_description],
    [template_owner],
    [job_function_template_alternate_key_id],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_raa_spat_j] WHERE [meta_is_current] = 1
UNION ALL
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [for_pershing_internal_use_only],
    [introducing_broker_dealer_ibd_number],
    [job_function_name],
    [effective_start_date_of_job_function],
    [effective_end_date_of_user_content_entitlement],
    [job_function_create_or_update_date],
    [user_id_of_administrator],
    [category_name],
    [id_unique_bfe_identifier],
    [bfe_name],
    [bfe_access_level],
    [bfe_description],
    [template_owner],
    [job_function_template_alternate_key_id],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_sai_spat_j] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[pershing_spat_m] AS
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [user_id],
    [introducing_broker_dealer_ibd_number],
    [user_first_name],
    [user_last_name],
    [id_creation_date],
    [effective_start_of_date_profile],
    [end_date_of_data_profile],
    [data_profile_update_status_code],
    [user_id_of_administrator],
    [user_content_entitlement_update_date],
    [data_profile_id],
    [data_profile_name],
    [data_profile_description],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_raa_spat_m] WHERE [meta_is_current] = 1
UNION ALL
SELECT
    [header_name],
    [header_firm_name],
    [header_remote_id],
    [header_refreshed_updated],
    [header_run_date],
    [header_run_time],
    [transaction_code],
    [record_identifier],
    [record_id_sequence_number],
    [user_id],
    [introducing_broker_dealer_ibd_number],
    [user_first_name],
    [user_last_name],
    [id_creation_date],
    [effective_start_of_date_profile],
    [end_date_of_data_profile],
    [data_profile_update_status_code],
    [user_id_of_administrator],
    [user_content_entitlement_update_date],
    [data_profile_id],
    [data_profile_name],
    [data_profile_description],
    [introducing_firm],
    [introducing_firm_context_enabled_indicator],
    [context_entitlement_name],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current],
    [meta_hash_key]
FROM [dbo].[pershing_sai_spat_m] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[pershing_netx360_backoffice_user_identity] AS
SELECT DISTINCT
M.INTRODUCING_BROKER_DEALER_IBD_NUMBER AS IBD_NUMBER_M,
E.INTRODUCING_BROKER_DEALER_IBD_NUMBER AS IBD_NUMBER_E,
J.INTRODUCING_BROKER_DEALER_IBD_NUMBER AS IBD_NUMBER_J,

M.USER_ID,
M.USER_ID_OF_ADMINISTRATOR,
M.USER_FIRST_NAME,
M.USER_LAST_NAME,
M.DATA_PROFILE_NAME,

J.JOB_FUNCTION_NAME,
J.BFE_NAME,
J.BFE_ACCESS_LEVEL,
J.BFE_DESCRIPTION,
J.CATEGORY_NAME,

E.JOB_FUNCTION_TEMPLATE_ALTERNATE_KEY_ID,
M.DATA_PROFILE_ID,
J.ID_UNIQUE_BFE_IDENTIFIER,

M.ID_CREATION_DATE,
M.EFFECTIVE_START_OF_DATE_PROFILE,
M.END_DATE_OF_DATA_PROFILE,
E.USER_JOB_FUNCTION_ENTITLEMENT_UPDATE_DATE,
M.USER_CONTENT_ENTITLEMENT_UPDATE_DATE,
J.JOB_FUNCTION_CREATE_OR_UPDATE_DATE

FROM [dbo].[pershing_spat_m] M

LEFT JOIN [dbo].[pershing_spat_e] E
    ON M.USER_ID = E.USER_ID
    --AND M.ELT_FIRM = E.ELT_FIRM
    --AND M.INTRODUCING_BROKER_DEALER_IBD_NUMBER = E.TEMPLATE_OWNER
    AND E.[meta_is_current] = 1

LEFT JOIN [dbo].[pershing_spat_j] J
    ON E.JOB_FUNCTION_TEMPLATE_ALTERNATE_KEY_ID = J.JOB_FUNCTION_TEMPLATE_ALTERNATE_KEY_ID
    --AND M.ELT_FIRM = J.ELT_FIRM
    --AND M.INTRODUCING_BROKER_DEALER_IBD_NUMBER = J.TEMPLATE_OWNER
    AND J.[meta_is_current] = 1

WHERE 1=1
    AND M.[meta_is_current] = 1
    AND (TRIM(UPPER(M.DATA_PROFILE_NAME)) IN ('HOME OFFICE USER ID', 'BRANCH OFFICE'))
    --AND COALESCE(M.INTRODUCING_BROKER_DEALER_IBD_NUMBER,'') NOT IN ('084')
    --AND COALESCE(E.INTRODUCING_BROKER_DEALER_IBD_NUMBER,'') NOT IN ('084')
    --AND COALESCE(J.INTRODUCING_BROKER_DEALER_IBD_NUMBER,'') NOT IN ('084')
;


