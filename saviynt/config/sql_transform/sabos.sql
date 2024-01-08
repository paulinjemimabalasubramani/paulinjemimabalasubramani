CREATE VIEW [dbo].[sabos_accounts_entitlements] AS
SELECT
    *
FROM [dbo].[sabos_useraccess]
WHERE [meta_is_current]=1
;
