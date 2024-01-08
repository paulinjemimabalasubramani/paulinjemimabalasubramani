CREATE VIEW [dbo].[investalink_accounts_entitlements] AS
SELECT
    *
FROM [dbo].[investalink_users]
WHERE [meta_is_current]=1
;
