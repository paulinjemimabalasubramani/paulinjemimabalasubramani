""" 
First file to be loaded in a module

"""

# %% Import Libraries

import sys, os



# %% Add Modules path to system

path = os.path.dirname(os.path.realpath(__file__))
if not path in sys.path:
    sys.path.append(path)



# %%


