CREATE VIEW [dbo].[sabos_accounts_entitlements] AS
SELECT
    CONCAT(app_description, ' on ', user_application) appdes_on_usrapp,
    REPLACE(email_id, '@ADVISORGROUP.COM', '@OSAIC.COM') AS osaic_email,
    [empid],
    [user] as [account_name],
    [name],
    [email_id],
    [status],
    [application],
    [user_application],
    [app_description],
    [security_level],
    [access],
    [prodflag]
FROM [dbo].[sabos_sabos_access_reports]
WHERE [meta_is_current]=1
    AND status='*ENABLED'
;
