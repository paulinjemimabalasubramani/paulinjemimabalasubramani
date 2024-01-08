CREATE VIEW [dbo].[nfs_user_id_administration] AS
SELECT
    [header_firm_name],
    [header_client_id],
    [record_type],
    [record_number],
    [portal_user_id],
    [user_id_first_name],
    [user_id_last_name],
    [user_id_create_date],
    [user_id_create_user],
    [user_id_update_date],
    [user_id_update_user],
    [user_id_status_end_date],
    [user_id_status_code],
    [cupid_id_indicator],
    [meta_pipeline_key],
    [meta_is_full_load],
    [meta_run_date],
    [meta_date_of_data],
    [meta_start_date],
    [meta_end_date],
    [meta_is_current]
FROM [dbo].[nfs_raa_user_id_administration] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[nfs_user_id_administration_account_linking] AS
SELECT
     [header_firm_name]
    ,[header_client_id]
    ,[record_type]
    ,[record_number]
    ,[portal_user_id]
    ,[super_branch]
    ,[branch]
    ,[registered_rep_owning_rep_rr_]
    ,[registered_rep_exec_rep_rr2_]
    ,[registered_rep_rep_of_record_ror_]
    ,[registered_rep_pay_to_rep_ptr_account_level]
    ,[agency_code]
    ,[account_classification]
    ,[product_level]
    ,[product_class]
    ,[super_user_indicator]
    ,[ficis_id]
    ,[active_rule_indicator]
    ,[g_number]
    ,[meta_pipeline_key]
    ,[meta_is_full_load]
    ,[meta_run_date]
    ,[meta_date_of_data]
    ,[meta_start_date]
    ,[meta_end_date]
    ,[meta_is_current]
FROM [dbo].[nfs_raa_user_id_administration_account_linking] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[nfs_user_id_administration_product] AS
SELECT
     [header_firm_name]
    ,[header_client_id]
    ,[record_type]
    ,[record_number]
    ,[portal_user_id]
    ,[product_id]
    ,[group_id]
    ,[group_name]
    ,[group_type_code]
    ,[meta_pipeline_key]
    ,[meta_is_full_load]
    ,[meta_run_date]
    ,[meta_date_of_data]
    ,[meta_start_date]
    ,[meta_end_date]
    ,[meta_is_current]
FROM [dbo].[nfs_raa_user_id_administration_product] WHERE [meta_is_current] = 1
;



CREATE VIEW [dbo].[nfs_wealthscape_user_id] AS
WITH Descriptions AS (
        SELECT * FROM (
            SELECT
                D.*,
                ROW_NUMBER() OVER (PARTITION BY TRIM(UPPER(D.ENTITLEMENT)) ORDER BY D.CATEGORY, D.SUB_CATEGORY, D.DESCRIPTION) as row_num
            FROM [dbo].[wealthscape_entitlements_descriptions] D
        ) T WHERE T.row_num=1
    )
SELECT DISTINCT
    U.HEADER_FIRM_NAME,
    U.PORTAL_USER_ID,
    U.USER_ID_CREATE_DATE,
    U.USER_ID_FIRST_NAME,
    U.USER_ID_LAST_NAME,
    U.USER_ID_STATUS_CODE,
    U.USER_ID_UPDATE_DATE,
    P.GROUP_ID,
    P.GROUP_NAME,
    P.GROUP_TYPE_CODE,
    D.DESCRIPTION AS ENTITLEMENT_DESCRIPTION,
    D.CATEGORY AS ENTITLEMENT_CATEGORY,
    D.SUB_CATEGORY AS ENTITLEMENT_SUB_CATEGORY,
    L.SUPER_USER_INDICATOR
FROM [dbo].[nfs_user_id_administration] U
INNER JOIN [dbo].[nfs_user_id_administration_product] P
    ON U.PORTAL_USER_ID = P.PORTAL_USER_ID AND P.[meta_is_current]=1
INNER JOIN [dbo].[nfs_user_id_administration_account_linking] L
    ON U.PORTAL_USER_ID = L.PORTAL_USER_ID AND L.[meta_is_current]=1
LEFT JOIN Descriptions D
    ON TRIM(UPPER(P.GROUP_NAME)) = TRIM(UPPER(D.ENTITLEMENT)) AND D.[meta_is_current]=1
WHERE U.[meta_is_current]=1
    AND UPPER(TRIM(L.SUPER_USER_INDICATOR)) = 'Y'
    --AND UPPER(TRIM(U.USER_ID_STATUS_CODE)) = 'A'
;
