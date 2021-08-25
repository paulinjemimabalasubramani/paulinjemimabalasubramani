"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

from .common_functions import make_logging, catch_error
from .config import is_pc


from pyspark.sql.functions import col, lit


# %% Logging
logger = make_logging(__name__)


# %% Parameters




# %% Join master_ingest_list with sql tables

@catch_error(logger)
def join_master_ingest_list_sql_tables(master_ingest_list, sql_tables):
    tables = master_ingest_list.join(
        sql_tables,
        (master_ingest_list.TABLE_NAME == sql_tables.TABLE_NAME) &
        (master_ingest_list.TABLE_SCHEMA == sql_tables.TABLE_SCHEMA),
        how = 'left'
        ).select(
            master_ingest_list.TABLE_NAME, 
            master_ingest_list.TABLE_SCHEMA,
            sql_tables.TABLE_NAME.alias('SQL_TABLE_NAME'),
            sql_tables.TABLE_TYPE,
            sql_tables.TABLE_CATALOG,
        )

    if is_pc: tables.printSchema()
    if is_pc: tables.show(5)

    # Check if there is a table in the master_ingest_list that is not in the sql_tables
    null_rows = tables.filter(col('SQL_TABLE_NAME').isNull()).select(col('TABLE_NAME')).collect()
    assert not null_rows, f"There are some tables in master_ingest_list that are not in sql_tables: {[x[0] for x in null_rows]}"

    return tables





