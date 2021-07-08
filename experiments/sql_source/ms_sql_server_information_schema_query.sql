USE LR ;

-- BEGIN retrive table information to build out metadata used to construct Snowflake objects, and loading constructs

SELECT c.TABLE_CATALOG AS SourceDatabase
      ,c.TABLE_SCHEMA  AS SourceSchema
	  ,c.TABLE_NAME    AS TableName
	  ,c.COLUMN_NAME   AS SourceColumnName
	  ,'N/A' AS [Label] -- used fro Informatica
      ,c.DATA_TYPE     AS SourceDataType
	  ,c.CHARACTER_MAXIMUM_LENGTH AS SourceDataLength
      ,c.NUMERIC_PRECISION    AS SourceDataPrecision
	  ,c.NUMERIC_SCALE    AS SourceDataScale
	  --,c.DATETIME_PRECISION   AS SourceDateTimePrecision
	  ,c.ORDINAL_POSITION  AS OrdinalPosition
	  ,c.DATA_TYPE AS CleanType
	  ,c.COLUMN_NAME   AS TargetColumnName
	  --,ddt.DataTypeTo AS TargetDataType
	  ,'N/A' AS TargetDataType
	  ,CASE
		  WHEN c.IS_NULLABLE = 'YES' THEN 1
		  ELSE 0
	  END AS IsNullable
	  ,CASE 
	       WHEN cnstr.Constraint_Type = 'PRIMARY KEY' AND cu.COLUMN_NAME = c.COLUMN_NAME THEN 1
		   ELSE 0
	  END AS KeyIndicator
FROM INFORMATION_SCHEMA.COLUMNS c       
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS cnstr       
ON  c.TABLE_CATALOG = cnstr.TABLE_CATALOG 
AND c.TABLE_SCHEMA = cnstr.TABLE_SCHEMA       
AND c.TABLE_NAME = cnstr.TABLE_NAME    
AND cnstr.CONSTRAINT_TYPE = 'PRIMARY KEY'       
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu       
ON  cnstr.TABLE_SCHEMA = cu.TABLE_SCHEMA                      
AND cnstr.TABLE_NAME = cu.TABLE_NAME         
AND cnstr.CONSTRAINT_NAME = cu.CONSTRAINT_NAME        
AND c.COLUMN_NAME = cu.COLUMN_NAME 
-- WHERE  c.TABLE_CATALOG = 'LR'
WHERE    c.TABLE_SCHEMA = 'OLTP'
ORDER  BY c.TABLE_CATALOG
         ,c.TABLE_SCHEMA
		 ,c.TABLE_NAME ;

-- END retrive table information to build out metadata used to construct Snowflake objects, and loading constructs

-- BEGIN retrive the distinct list of data types

SELECT DISTINCT 
		c.DATA_TYPE AS SourceDataType
FROM INFORMATION_SCHEMA.COLUMNS c       
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS cnstr       
ON  c.TABLE_CATALOG = cnstr.TABLE_CATALOG 
AND c.TABLE_SCHEMA = cnstr.TABLE_SCHEMA       
AND c.TABLE_NAME = cnstr.TABLE_NAME    
AND cnstr.CONSTRAINT_TYPE = 'PRIMARY KEY'       
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu       
ON  cnstr.TABLE_SCHEMA = cu.TABLE_SCHEMA                      
AND cnstr.TABLE_NAME = cu.TABLE_NAME         
AND cnstr.CONSTRAINT_NAME = cu.CONSTRAINT_NAME        
AND c.COLUMN_NAME = cu.COLUMN_NAME 
WHERE  c.TABLE_SCHEMA = 'OLTP' ;

-- END retrive the distinct list of data types
