"""
Create and Execute (if required) Snowflake DDL Steps and ingest_data for FINRA

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging
from modules.spark_functions import create_spark


from delta.tables import *


# %% Logging
logger = make_logging(__name__)


# %% Parameters




# %% Create Session

spark = create_spark()


# %%



# %%

