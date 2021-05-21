""" 
First file to be loaded in a module

"""

import sys, os

path = os.path.dirname(os.path.realpath(__file__))
if not path in sys.path:
    sys.path.append(path)
