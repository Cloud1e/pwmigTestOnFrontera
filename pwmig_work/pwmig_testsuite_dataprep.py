#!/usr/bin/env python
# coding: utf-8

# # Building and running pwmig test suite
# This notebook replaces the README file in the older purely C++ based version of pwmig.  This notebook is limited at present to the data preparation to load data we need for running pwstack and pwmig into the new MongoDB formulation.
# 
# The original pwmig set had a utility called makegclgrid written in C++.  A mspass compatible python replacement has been written with a similar name but slightly different usage. Because I created makegclgrid as a command line tool those steps had to be done on the command line.  That is further complicated with by the fact that standard usage will now be done through docker.  I know of two ways to get a shell to run the following commands:
# 1.  Use the docker command line tools to launch a shell.  Check online of the syntax.
# 2.  The easier solution is to select New->Terminal from the jupyter home page that brings up a web page terminal interface.
# 
# Assuming you have a shell running by one of the above method run the following commands:
'''
pwmig-makegclgrid pwmigtest -pf makegclgrid_taimage.pf -cs mongodb://mongodb:27017
pwmig-makegclgrid pwmigtest -pf makegclgrid_ak135.pf -cs mongodb://mongodb:27017
pwmig-project1dmod pwmigtest ak135 ak135P -field ak135_P -mt Pvelocity -v -cs mongodb://mongodb:27017
pwmig-project1dmod pwmigtest ak135 ak135S -field ak135_S -mt Svelocity -v -cs mongodb://mongodb:27017
'''
# Noting:
# 1.  The older code used a coherence grid that has been depricated.
# 2.  The above needs some cleanup before release to set up the container to have /usr/local/bin in the default shell path.
# 
# We confirm in the next box this worked and stored the index to the gclgrid files in the default collection gclfielddata:

# In[2]:
import argparse
from run_commands import run_commands
from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
from bson import json_util
    
# MONGO_URI = "mongodb://localhost:27017"
# commands = [
#     "rm -rf /test/pf/GCLgrids/*",
#     "cd /test/pf",
#     "pwd",
#     "pwmig-makegclgrid pwmigtest -pf makegclgrid_taimage.pf -cs " + MONGO_URI,
#     "pwmig-makegclgrid pwmigtest -pf makegclgrid_ak135.pf -cs " + MONGO_URI ,
#     "pwmig-project1dmod pwmigtest ak135 ak135P -field ak135_P -mt Pvelocity -v -cs " + MONGO_URI,
#     "pwmig-project1dmod pwmigtest ak135 ak135S -field ak135_S -mt Svelocity -v -cs " + MONGO_URI,
#     "cd /test",
# ]
def parse_args():
    parser = argparse.ArgumentParser(description='PWMig test suite setup')
    parser.add_argument('-cs', '--connection-string', 
                       default='mongodb://localhost:27017',
                       help='MongoDB connection string (default: mongodb://localhost:27017)')
    return parser.parse_args()

args = parse_args()
MONGO_URI = args.connection_string  
# MONGO_URI = 'mongodb://localhost:27017'
commands = [
    "rm -rf /test/pf/GCLgrids/*",
    "cd /test/pf",
    "pwd",
    "pwmig-makegclgrid pwmigtest -pf makegclgrid_taimage.pf -cs " + MONGO_URI,
    "pwmig-makegclgrid pwmigtest -pf makegclgrid_ak135.pf -cs " + MONGO_URI ,
    "pwmig-project1dmod pwmigtest ak135 ak135P -field ak135_P -mt Pvelocity -v -cs " + MONGO_URI,
    "pwmig-project1dmod pwmigtest ak135 ak135S -field ak135_S -mt Svelocity -v -cs " + MONGO_URI,
    "cd /test",
]
# commands = ["sh /test/init.sh"]
print("MONGO_URI: ", MONGO_URI)
dbclient = DBClient(MONGO_URI)
dbclient['pwmigtest'].drop_collection('GCLfielddata')
dbclient['pwmigtest'].drop_collection('VelocityModel_1d')
dbclient['pwmigtest'].drop_collection('wf_Seismogram')

db = Database(dbclient,'pwmigtest')
for doc in db.GCLfielddata.find({}):
    print(json_util.dumps(doc,indent=2))


# You should see attributes of 4 documents corresponding to 4 GCLgrid objects the above created.  
# 
# The next step is a deviation from the older approach made necessary by a decision to support reading 1d velocity models from files (including an antelope mod1d table) and providing a mechanism to save such models to MongoDB.  This next block does that for ak135:

# In[2]:



# from pwmigpy.ccore.seispp import VelocityModel_1d
import pwmigpy.db.database
print('Testing read_1d_model_file')
from pwmigpy.db.database import read_1d_model_file
from pwmigpy.db.database import vmod1d_dbsave
from pwmigpy.db.database import vmod1d_dbread
vmodP = read_1d_model_file('modeldb.mod1d',format='mod1d',property='Pvelocity',model='ak135')
vmodS = read_1d_model_file('modeldb.mod1d',format='mod1d',property='Svelocity',model='ak135')
vmod1d_dbsave(db,vmodP,'ak135P',property='Pvelocity')
vmod1d_dbsave(db,vmodS,'ak135S',property='Svelocity')
run_commands(commands)


# Verify that actually worked:

# In[3]:


cursor=db.VelocityModel_1d.find({})
for doc in cursor:
    print(doc['name'],doc['property'])


# The next step requires the use of a new command line, python tool that has the same name as an older C++ program that did something very similar:  project1dmod.  (Currently with a "pwmig-" prefix for reasons we need to fix.)  Here are the run lines:
'''
pwmig-project1dmod pwmigtest ak135 ak135P -field ak135_P -mt Pvelocity -v -cs mongodb://mongodb:27017
pwmig-project1dmod pwmigtest ak135 ak135S -field ak135_S -mt Svelocity -v -cs mongodb://mongodb:27017

'''
# where the -v was used to produce verbose output.  That helped guide this next text to verify that worked correctly:

# In[4]:


query={'name' : 'ak135_P'}
n=db.GCLfielddata.count_documents(query)
print('number of documents with name ak135_P=',n)
print('Detailed contents:')
doc=db.GCLfielddata.find_one(query)
print(json_util.dumps(doc,indent=2))
query={'name' : 'ak135_S'}
n=db.GCLfielddata.count_documents(query)
print('number of documents with name ak135_S=',n)
print('Detailed contents:')
doc=db.GCLfielddata.find_one(query)
print(json_util.dumps(doc,indent=2))


# Anyone running this should validate that the document contents displayed here are a match to the -v output of project1dmod for the P and S models. 
# 
# The final data prep requirement is to create a wf_Seismogram collection from the old testdb Antelope database used for the earlier version.  We use some fairly generic but specialized code saved in this package in the dataprep area.   Some pieces of that will be superceded by more bullet proof mspass version that used those as a prototype.
# 
# Note first pass of this block had a nasty name collision.   In css3.0 the "delta" attribute is used for epicentral great circle path distances in degrees.   In mspass delta is a special keyword in wf collections used to define the time series data sample interval (the name was borrowed from obspy).   Hence, this block has to change the name of the delta css attribute to assoc_delta.  It also has to compute delta as sample from samprate because css chooses to save sampling frequency instead of sampling interval.  
# 
# Note: TODO  The container doesn't currently have set PFPATH and doesn't handle data contents.  This came up in the current run of this next block because I got a error that the script could not find the required pf file called "AntelopeDatabase.pf".   I had to do this externally to copy the script to the home directory mapped to the container when I launched docker (shell with current directory in the directory mapped to /home):
# ```
# cp ~/src/parallel_pwmig/data/pf/AntelopeDatabase.pf .
# ```

# In[5]:


import pwmigpy.db.datascope as datascope


dbname = 'testdb'
# turned parallel on and off to test both modes
dbhandle = datascope.AntelopeDatabase(dbname,pffile='AntelopeDatabase.pf',parallel=False)
df = dbhandle.get_table('wfprocess')
print('size of dataframe created from wfprocess=',len(df))
df = dbhandle.join(df,'evlink')
print('Size of join with evlink=',len(df))
df = dbhandle.join(df,'sclink')
print('Size of join with sclink=',len(df))
df = dbhandle.join(df,'event')
print('Size of join with event=',len(df))
df = dbhandle.join(df,'site',join_keys=['sta'])
print('Size of join with site=',len(df))
dfa = dbhandle.get_table('event')
dfa = dbhandle.join(dfa,'origin',join_keys=['evid'])
print("event-origin join size (should be 1 here)=",len(dfa))
print(dfa)
dfa = dbhandle.join(dfa,'assoc',join_keys=['orid'])
print('Size of join with assoc=',len(dfa))
dfa = dbhandle.join(dfa,'arrival',join_keys=['arid'])
df_final = dfa.merge(df,how='inner',on=['sta','evid'],suffixes=['_event','_site'])
print('size of final dataframe=',len(df_final))
keeplist = ['evid',
'lat_event',
'lon_event',
'depth',
'time',
'orid',
'sta',
'phase',
'delta',
'seaz',
'esaz',
'iphase',
'pwfid',
'starttime',
'endtime',
'time_standard',
'dir',
'dfile',
'foff',
'nsamp',
'dtype_site',
'samprate',
'lat_site',
'lon_site',
'elev',
'time_arrival']

rename_these = {'lat_event' : 'event_lat',
          'lon_event' : 'event_lon',
          'depth' : 'event_depth',
          'time' : 'event_time',
          'lat_site' : 'site_lat',
          'lon_site' : 'site_lon',
          'elev' : 'site_elev',
          'dtype_site' : 'dtype',
          'nsamp' : 'npts',
          'delta' : 'assoc_delta',
          'seaz' : 'assoc_seaz',
          'esaz' : 'assoc_esaz',
          'time_arrival' : 'Ptime'
} 
attribute_names=df_final.columns
df=df_final[keeplist]
df=df.rename(columns=rename_these)
# the wfprocess time_standard field has "a" for "absolute" - MsPASS needs this instead
df=df.assign(time_standard="UTC")
# MsPASS reader requires the storage_mode attribute.  here we set 
# all to file since that is the mode here
df.insert(2,'storage_mode','file')
df['delta'] = 1.0 / df['samprate']

db.wf_Seismogram.insert_many(df.to_dict('records'))


# Short verification.  Should show 651 documents and reasonable contents of the first document in the db.

# In[6]:


n=db.wf_Seismogram.count_documents({})
print('wf_Seismogram collection size=',n)
doc=db.wf_Seismogram.find_one({})
print('First document in collection')
print(json_util.dumps(doc,indent=2))


# We need another thing yet.   That is, pwstack wants to fetch the event data from the source collection.  That means we are going to have to extract the unique data for source information, save it, and add the id as source_id to all entries in wf_Seismogram.    

# In[7]:


# This only works for this test data - works because there is only one source.  We'll keep the event 
# versions even though they are redundant in the end. 
doc=db.wf_Seismogram.find_one()
#print(json_util.dumps(doc,indent=2))
lat=doc['event_lat']
lon=doc['event_lon']
depth=doc['event_depth']
time=doc['event_time']
srcdoc=dict()
srcdoc['lat'] = lat
srcdoc['lon'] = lon
srcdoc['depth'] = depth
srcdoc['time'] = time
srcid = db.source.insert_one(srcdoc).inserted_id
print(srcid)


# We now insert that id into each entry in wf_Seismogram - this works only because srcid was set in the previous box.

# In[8]:


insrec = { "$set" : {'source_id' : srcid}}
# there might be a way to do the following with insert_many but this db is so small the cost for
# this loop is negligible
cursor = db.wf_Seismogram.find({})
n=0
for doc in cursor:
    id = doc['_id']
    db.wf_Seismogram.update_one({'_id' : id},insrec)
    n+=1
print('updated ',n,' documents with source_id')
# print one record 
print('example')
doc=db.wf_Seismogram.find_one({})
print(json_util.dumps(doc,indent=2))


# Now we need a similar thing for site.  For this test data set with one event there is one and only one site entry per wf_Seismogram records so we can build site (mostly) easily from wf_Seismogram or the dataframe.   We'll use the database at this stage since we are relatively certain it is clean.  We first build site.  We then need to add a geo index, but that is more adventure land so will do that in the next code box.

# In[9]:


# Needed this while debugging this box - retain in case there are other problems later
# db.drop_collection('site')
cursor=db.wf_Seismogram.find({})    
for doc in cursor:
    lat = doc.get('site_lat')
    if lat is None:
        print("Missing site_lat for doc:", doc)
    wfid = doc['_id']
    lat = doc['site_lat']
    lon = doc['site_lon']
    elev = doc['site_elev']
    sta = doc['sta']  # We actually don't need this for this application but will make the result more readable
    coords = [lon,lat]   # We need this for geo index
    insdoc={'lat' : lat,
            'lon' : lon,
            'elev' : elev,
            'coordinates' : coords,
            'sta' :sta
           }
    ret=db.site.insert_one(insdoc)
    site_id = ret.inserted_id
    db.wf_Seismogram.update_one({'_id' : wfid},{'$set' : {'site_id' : site_id}})
n = db.site.count_documents({})
print('number of documents now in site=',n)


# The algorithm used in pwstack requires a geo index.   This block constructs that for site.

# In[10]:


from pymongo import MongoClient, GEO2D
db.site.create_index([("coordinates", GEO2D)])


# Let's verify site_id and source_id are set in wf_Seismogram - we just check a few.

# In[11]:


cursor = db.wf_Seismogram.find({}).limit(10)
for doc in cursor:
    print(doc['sta'],doc['source_id'],doc['site_id'])


# This next box is a workaround for a deficiency in how I created the site collection above.   We need to insert some attributes that stock mspass readers (correctly) demand.

# In[12]:


cursor=db.site.find({})
setdoc={'$set' :{ 'net' : 'TA','loc' : '00', 'starttime' : 1577909670.0 , 'endtime' : 1578912967.53105} }
for doc in cursor:
    sid=doc['_id']
    db.site.update_one({'_id' : sid},setdoc)
cursor=db.source.find({})
setdoc={'$set' : {'magnitude' : 9.0}}
for doc in cursor:
    sid=doc['_id']
    db.source.update_one({'_id' : sid},setdoc)


# This is a bit out of place, but didn't realize this need at first.   The old pwmig had an option to convert a model defined as velocity internally to slowness.  I judged that a dumb idea with the capabilities of MsPASS and the python binds created for this new implementation of pwmig.   So, I created a small function to convert a field stored as velocity to slowness.  We also want the 3d model use to be perturbation, so this does that do.  In this case the perturbation is a bit stupid because the field we produce is machine zeros, but it is appropriate for this test case. 

# In[16]:


from pwmigpy.db.database import GCLdbsave,GCLdbread_by_name
from pwmigpy.utility.earthmodels import Velocity3DToSlowness

vel = GCLdbread_by_name(db,"ak135_P")
Velocity3DToSlowness(vel,ConvertToPerturbation=True)
# Need to change the name field so it will be different when saved 
vel.name="ak135_dUP"
# The above saved the other field data in this directory we put this one there too
# This line is what I used for a local run
#dir="/home/pavlis/data/copy_pwmigtest/pf/GCLgrids"
# This is the replacement for docker with mapped /home to the top level data directory for the test
dir="/test/pf/GCLgrids"
GCLdbsave(db,vel,dir=dir)


# The above error seems irrelevant.  External checks so the files and the document were saved.  I'll need to work this out later, but I think the problem will go away if I define a schema for pwmig and enforce it in the save.   That is only a theory though.  Weirdly this problem does not occur when I run the same script with spyder.  Haven't tried straight python. 
# 
# Anyway, we that here is the save for the S model we also require:

# In[17]:


vel = GCLdbread_by_name(db,"ak135_S")
Velocity3DToSlowness(vel,ConvertToPerturbation=True)
# Need to change the name field so it will be different when saved 
vel.name="ak135_dUS"
# The above saved the other field data in this directory we put this one there too
#dir="/home/pavlis/data/copy_pwmigtest/pf/GCLgrids"
GCLdbsave(db,vel,dir=dir)


# Let's verify that really did work as I found earlier with spyder.  

# In[18]:


doc=db.GCLfielddata.find_one({'name' : 'ak135_dUP'})
print(json_util.dumps(doc,indent=2))
doc=db.GCLfielddata.find_one({'name' : 'ak135_dUS'})
print(json_util.dumps(doc,indent=2))
print("This is the end of the data prep for the pwmig test suite")

# So, the errors in the blocks above are indeed harmless.  The above output is what I expect.

# In[ ]:




