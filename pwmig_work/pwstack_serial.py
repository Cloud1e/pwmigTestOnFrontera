#!/usr/bin/env python
# coding: utf-8

# # pwstack serial version
# This is comparable to a similar notebook called pwstack but this runs pwstack in serial mode.  Useful only for benchmarking and validation of parallel processing code.

# In[1]:


from mspasspy.db.database import Database 
from mspasspy.db.client import DBClient
from  mspasspy.ccore.utility import AntelopePf
from run_commands import run_commands

from pwmigpy.pwmig.pwstack_serial import pwstack
# dask version
#from pwmigpy.pwmig.pwstack import pwstack
import time

import dask
# This one is ran to completetion without any issues.
dask.config.set(scheduler='threads',num_workers=8) 
# This version dies with pickle errors
#dask.config.set(scheduler='processes') 

dbclient = DBClient("mongodb://localhost:27017")

db = Database(dbclient,'pwmigtest')


pffile = 'pwstack.pf'
pf = AntelopePf(pffile)

t0=time.time()
#pwstack_return = pwstack(db,pf,slowness_grid_tag='Slowness_Grid_Definition',
#                       output_data_tag='test_pwstack_parallel_output')
pwstack_return = pwstack(db,pf,slowness_grid_tag='Slowness_Grid_Definition',
                         output_data_tag='test_pwstack_serial_output')
print('pwstack finished.  Elapsed time=',time.time()-t0)


# In[ ]:




