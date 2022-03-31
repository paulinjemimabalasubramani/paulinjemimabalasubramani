CREATE OR REPLACE PROCEDURE ELT_STAGE.USP_FLATTEN_COLUMN_VIEW(TABLE_NAME VARCHAR, COLUMN_NAME VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var sql_command = `
    select distinct value from 
    (
    select distinct OBJECT_KEYS(value) as v
    FROM `+TABLE_NAME+`, lateral flatten(input => PARSE_JSON(`+COLUMN_NAME+`))
    )  t, lateral flatten(input => V)
    ;`;

execObj = snowflake.execute({sqlText: sql_command});

nkeys = execObj.getRowCount();

if (nkeys>0) {
    ndot = TABLE_NAME.lastIndexOf('.');
    if (ndot>0) {
        view_name = TABLE_NAME.substring(0, ndot+1);
    } else {
        view_name = '';
}

view_name += `VW_FLATTEN_`+TABLE_NAME.substring(ndot+1)+`_`+COLUMN_NAME ;

result = `CREATE OR REPLACE VIEW `+view_name+`
AS
SELECT 
ELT_PRIMARY_KEY
`;

for (var i = 0; i < nkeys; i++) {
    execObj.next();
    keyname = execObj.getColumnValueAsString('VALUE');
    result += `,value:`+keyname+`::string as `+keyname+`
`;
}

result += `
FROM `+TABLE_NAME+`, lateral flatten(input => PARSE_JSON(`+COLUMN_NAME+`))
;
`;

} else {
result = 'No keys found in table';
}

return result
$$;
