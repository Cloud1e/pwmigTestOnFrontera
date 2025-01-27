#!/usr/bin/env python
# coding: utf-8

# # pwstack
# This notebook runs pwstack in parallel mode on  the test data suite.  It assumes the user has previously run the notebook "pwmig_testsuite_dataprep"

# In[1]:


from mspasspy.db.database import Database 
from mspasspy.db.client import DBClient
from  mspasspy.ccore.utility import AntelopePf

#from pwmigpy.pwmig.pwstack_serial import pwstack
# dask version
from pwmigpy.pwmig.pwstack import pwstack
import time

import dask
import argparse


# This one is ran to completetion without any issues.
#dask.config.set(scheduler='processes',num_workers=8) 
# This version dies with pickle errors
#dask.config.set(scheduler='processes') 
def parse_args():
    parser = argparse.ArgumentParser(description='PWMig test suite setup')
    parser.add_argument('-cs', '--connection-string', 
                       default='mongodb://localhost:27017',
                       help='MongoDB connection string (default: mongodb://localhost:27017)')
    return parser.parse_args()

args = parse_args()
MONGO_URI = args.connection_string    
def main():

    dbclient = DBClient(MONGO_URI)
    db = Database(dbclient,'pwmigtest')
    pffile = 'pwstack.pf'
    pf = AntelopePf(pffile)
    t0=time.time()
    print("pwstack_return start")
    pwstack_return = pwstack(db,pf,slowness_grid_tag='Slowness_Grid_Definition',
                           output_data_tag='pwstack_parallel_output')
    #pwstack_return = pwstack(db,pf,slowness_grid_tag='Slowness_Grid_Definition',
    #                         output_data_tag='test_pwstack_serial_output')
    print('pwstack finished.  Elapsed time=',time.time()-t0)


    # Timing I got on an 8 core ubuntu machine as 436 s.
    #
    # Second run was a little longer with 447 s but the machine wasn't totally idle in the second run.

if __name__ == '__main__':
    main()