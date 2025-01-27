#!/usr/bin/env python
# coding: utf-8

# # pwmig run
# This notebook runs a simple python script to run pwmig.  It should only be run after running pwstack.

# In[ ]:


from mspasspy.db.database import Database 
from mspasspy.db.client import DBClient
from  mspasspy.ccore.utility import AntelopePf
from bson import json_util
import argparse
from run_commands import run_commands

from pwmigpy.pwmig.pwmig import migrate_event
import time

from pwmigpy.db.database import GCLdbsave
def parse_args():
    parser = argparse.ArgumentParser(description='PWMig test suite setup')
    parser.add_argument('-cs', '--connection-string', 
                       default='mongodb://localhost:27017',
                       help='MongoDB connection string (default: mongodb://localhost:27017)')
    return parser.parse_args()

args = parse_args()
MONGO_URI = args.connection_string    
commands = [
    "rm -rf /test/imagevolumes/test_8threads.dat",
    "rm -rf /test/imagevolumes/test_8threads.dat.pf"
]
run_commands(commands)
outdir="/test/imagevolumes"
outdfile="test_8threads.dat"
dbclient = DBClient(MONGO_URI)
db = Database(dbclient,'pwmigtest')
pffile = 'pwmig.pf'
t0=time.time()
pf = AntelopePf(pffile)
cursor = db.source.find({})
for doc in cursor:
    print("Working on data from this event")
    print(json_util.dumps(doc,indent=2))
    imagevolume = migrate_event(db,doc['_id'],pf)
t=time.time()
print('pwmig test finished:  elapsed time=',t-t0)
print("Saving output with dir=",outdir," and dfile=",outdfile)
doc_saved = GCLdbsave(db,imagevolume,dir=outdir,dfile=outdfile)
print("Contents of doc saved to GCLfielddata collection")
for k in doc_saved:
    print(k,doc_saved[k])

print("Done with pwmig.py")
# In[ ]:




