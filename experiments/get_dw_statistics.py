
# %%

import pyodbc, csv
from datetime import datetime


# %%

now = datetime.now()

connection_string = 'DRIVER={SQL Server};SERVER=PW1SQLDATA01;DATABASE=DW;Trusted_Connection=yes;'

conn = pyodbc.connect(connection_string, autocommit=False)
cursor = conn.cursor()


# %%


# %%



sql_str = f"""
WITH TABLES1 AS (
	select 
		TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME
	from INFORMATION_SCHEMA.TABLES
	where TABLE_TYPE = 'BASE TABLE'
),
COLUMNS1 AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME 
			ORDER BY CASE 
				WHEN COLUMN_NAME LIKE 'UpdateTimeStamp' THEN 0
				WHEN COLUMN_NAME LIKE 'RunDate' THEN 1
				WHEN COLUMN_NAME LIKE 'STATEMENTDATEID' THEN 2
				WHEN COLUMN_NAME LIKE 'UPDATEDTS' THEN 3
				WHEN COLUMN_NAME LIKE 'UPDATETS' THEN 3
				WHEN COLUMN_NAME LIKE 'EFFDATE' THEN 4
				WHEN COLUMN_NAME LIKE '%DATEID' THEN 5
				ELSE 1000
				END
			) AS RN1
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE 1=1
		AND	
		(UPPER(COLUMN_NAME) IN ('UPDATETIMESTAMP', 'RUNDATE', 'STATEMENTDATEID', 'UPDATEDTS', 'UPDATETS', 'EFFDATE', 'CREATEDTS', 'CREATETS')
		OR COLUMN_NAME LIKE '%DATEID')
)
SELECT 
	T.*,
	C.COLUMN_NAME,
    C.DATA_TYPE
FROM TABLES1 T
LEFT JOIN COLUMNS1 C
	ON T.TABLE_CATALOG=C.TABLE_CATALOG
		AND T.TABLE_SCHEMA=C.TABLE_SCHEMA
		AND T.TABLE_NAME=C.TABLE_NAME
		--AND C.RN1 = 1
	ORDER BY TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME
;

"""

cursor.execute(sql_str)

fieldnames = [c[0] for c in cursor.description]



# %%

rows = cursor.fetchall()



# %%

final_rows = {}
for i, row in enumerate(rows):
    print(i, row)
    #if i<1690: continue
    row_dict = {c:row[i] for i, c in enumerate(fieldnames)}

    row_dict['date'] = now

    try:
        table_name_with_schema = f"[{row_dict['TABLE_SCHEMA']}].[{row_dict['TABLE_NAME']}]"
        sql_str = f"SELECT COUNT(*) FROM {table_name_with_schema}"
        cursor.execute(sql_str);
        result = cursor.fetchall()
        row_dict['total_rows'] = result[0][0]
    except:
        row_dict['total_rows'] = 'N/A'

    try:
        if row_dict['DATA_TYPE']:
            sql_str = f"SELECT MAX({row_dict['COLUMN_NAME']}) FROM {table_name_with_schema}"
            cursor.execute(sql_str);
            result = cursor.fetchall()
            row_dict['max_value'] = result[0][0]

            if 'date' in row_dict['DATA_TYPE']:
                filter_str = f"{row_dict['COLUMN_NAME']} >= DATEADD(DAY, -40, GETDATE())"
            else:
                filter_str = f"CONVERT(DATE, CONVERT(VARCHAR, {row_dict['COLUMN_NAME']}), 120) >= DATEADD(DAY, -40, GETDATE())"

            sql_str = f"SELECT COUNT(*) FROM {table_name_with_schema} WHERE {filter_str}"
            cursor.execute(sql_str);
            result = cursor.fetchall()
            row_dict['rows_changed_last_1_month'] = result[0][0]
        else:
            row_dict['max_value'] = 'N/A'
            row_dict['rows_changed_last_1_month'] = 'N/A'
            row_dict['COLUMN_NAME'] = '<NO DATE INDEX FOUND>'
            row_dict['DATA_TYPE'] = 'N/A'
    except Exception as e:
        print(e)
        row_dict['max_value'] = 'N/A'
        row_dict['rows_changed_last_1_month'] = 'N/A'
        row_dict['COLUMN_NAME'] = '<NO DATE INDEX FOUND>'
        row_dict['DATA_TYPE'] = 'N/A'
        pass

    row_index = (row_dict['TABLE_SCHEMA'], row_dict['TABLE_NAME'])

    if (row_index not in final_rows) or final_rows[row_index]['rows_changed_last_1_month'] == 'N/A' or \
        (row_dict['rows_changed_last_1_month'] != 'N/A' and \
        final_rows[row_index]['rows_changed_last_1_month'] < row_dict['rows_changed_last_1_month']):
        final_rows[row_index] = row_dict




# %%

fieldnames2 = ['date'] + fieldnames + ['total_rows', 'rows_changed_last_1_month', 'max_value']

with open(r'C:\myworkdir\EDIP-Code\experiments\dw_out.csv', 'wt', newline='') as f:

    out_writer = csv.DictWriter(f=f, delimiter=',', quotechar=None, quoting=csv.QUOTE_NONE, skipinitialspace=True, fieldnames=fieldnames2)
    out_writer.writeheader()

    for key, row_dict in final_rows.items():
        out_writer.writerow(row_dict)




# %%



