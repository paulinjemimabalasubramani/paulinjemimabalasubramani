"""
Library for common generic functions

"""

# %% Import Libraries
import os, sys, re


# %% Parameters

column_regex = r'[\W]+'



# %%

def normalize_column_name(col_name):
    """
    Clean up column name and give standard looking columns
    """
    return re.sub(column_regex, '_', str(col_name).lower().strip())



# %%


