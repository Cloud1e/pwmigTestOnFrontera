
from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
import argparse
def parse_args():
    parser = argparse.ArgumentParser(description='PWMig test suite setup')
    parser.add_argument('-cs', '--connection-string', 
                       default='mongodb://localhost:27017',
                       help='MongoDB connection string (default: mongodb://localhost:27017)')
    return parser.parse_args()

args = parse_args()
MONGO_URI = args.connection_string    
def main():
    print("start running test.py")
    print("MONGO_URI in test.py: ", MONGO_URI)
    # MONGO_URI = "mongodb://localhost:27017"

    dbclient = DBClient(MONGO_URI)
    db = Database(dbclient,'pwmigtest')
    n=db.wf_Seismogram.count_documents({})
    print('In the beginning, wf_Seismogram collection size=',n)
    # show all databases
    # dbclient['pwmigtest'].list_collection_names()
    # dbclient['pwmigtest'].drop_collection('GCLfielddata')
    # dbclient['pwmigtest'].drop_collection('VelocityModel_1d')

if __name__ == "__main__":
    main()
