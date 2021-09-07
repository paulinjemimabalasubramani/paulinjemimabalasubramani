"""
Vacuum all delta tables in ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)

from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import is_pc, logger, mark_execution_end
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, default_storage_account_name, get_firms_with_crd



# %% Parameters

database = 'FINRA'
tableinfo_source = database



# %% Create Session

spark = create_spark()



# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=tableinfo_source)

if is_pc: pprint(firms)



# %% 







# %% Mark Execution End

mark_execution_end()


# %%

