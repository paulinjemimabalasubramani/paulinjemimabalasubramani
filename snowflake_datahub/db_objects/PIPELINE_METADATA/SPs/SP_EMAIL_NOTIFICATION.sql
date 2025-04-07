CREATE OR REPLACE PROCEDURE DATAHUB.PIPELINE_METADATA.SP_EMAIL_NOTIFICATION("TO_ADDRESS" VARCHAR, "SUBJECT" VARCHAR, "BODY_INPUT_TYPE" VARCHAR, "BODY_CONTENT_1" VARCHAR, "BODY_CONTENT_2" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    V_TABLE VARCHAR(16777216);
     V_DETAILS VARCHAR(16777216);
     select_statement VARCHAR;
    
BEGIN
    IF (BODY_INPUT_TYPE = ''SQL'') THEN
        -- Convert SQL input to HTML table
        CALL DATAHUB.PIPELINE_METADATA.CONVERT2HTML(:BODY_CONTENT_2) INTO V_TABLE;
    ELSE
        -- Use the direct string input
        V_TABLE := :BODY_CONTENT_2;
    END IF;

    -- Send email notification
    CALL SYSTEM$SEND_EMAIL(
        ''DATAHUB_EMAIL_NOTIFICATION'', 
        :TO_ADDRESS,
        :SUBJECT,
        ''<style>table, th, td {border:1px solid black;}</style><table><tr><td>''||REPLACE (REPLACE(:BODY_CONTENT_1,''='',''</td><td>''),'':'',''</td></tr><tr><td>'')||''</td></tr></table>'' ||:V_TABLE,
        ''TEXT/HTML''
    );

END;
';