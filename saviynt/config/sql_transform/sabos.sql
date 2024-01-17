CREATE VIEW [dbo].[sabos_accounts_entitlements] AS
SELECT
    CONCAT(appdes, ' on ', usrapp) appdes_on_usrapp,
    REPLACE(emailtrim, '@ADVISORGROUP.COM', '@OSAIC.COM') AS osaic_email,
    *
FROM [dbo].[sabos_useraccess]
WHERE [meta_is_current]=1
    AND upstat='*ENABLED'
;
