CREATE VIEW [dbo].[mips_accounts_entitlements] AS
SELECT
    *
FROM [dbo].[mips_users]
WHERE [meta_is_current]=1
;
