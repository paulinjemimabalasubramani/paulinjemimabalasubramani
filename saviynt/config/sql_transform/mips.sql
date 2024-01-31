CREATE VIEW [dbo].[mips_accounts_entitlements] AS
SELECT
    *
FROM [dbo].[mips_useraccess]
WHERE [meta_is_current]=1
;
