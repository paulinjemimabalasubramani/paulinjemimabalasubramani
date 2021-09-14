# %%

import os

x = os.environ.get('x')

print(x)


# %%

a='a'

if all([a, x]):
    print('ok')

# %%
