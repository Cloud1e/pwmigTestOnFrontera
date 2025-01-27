import os
from pwmigpy.ccore.gclgrid import GCLgrid, GCLgrid3d, GCLscalarfield3d
from pwmigpy.db.database import GCLdbread

from pwmigpy.paraview.vtk_converters import GCLfield2vtksg, vtkFieldWriter
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
import argparse
def parse_args():
    parser = argparse.ArgumentParser(description='PWMig test suite setup')
    parser.add_argument('-cs', '--connection-string', 
                       default='mongodb://localhost:27017',
                       help='MongoDB connection string (default: mongodb://localhost:27017)')
    return parser.parse_args()

args = parse_args()
MONGO_URI = args.connection_string
COLLECTION_NAME = "GCLfielddata"
QUERY_NAME = "ak135_dUP"  
OUTPUT_DIR = "/test/paraview"
VTK_FILE_NAME = "ak135_dUP.vts"  


def convert_gcl_to_vtk(db_uri, collection_name, query_name, output_dir, vtk_file_name):
    """
    Converts a GCLgrid object stored in MongoDB to a VTK file for visualization in Paraview.
    
    :param db_uri: URI for connecting to MongoDB.
    :param collection_name: MongoDB collection name where the GCLgrid is stored.
    :param query_name: The "name" attribute value to query for the GCLgrid document.
    :param output_dir: Directory to save the generated VTK files.
    :param vtk_file_name: Base name for the output VTK file (without extension).
    """
    # Step 1: Connect to MongoDB
    client = DBClient(db_uri)
    db = Database(client, 'pwmigtest')  # Replace 'pwmigtest' with your database name.
    collection = db[collection_name]
    
    # Step 2: Query the database for the specified GCLgrid object
    print(f"Querying collection '{collection_name}' for GCLgrid with name '{query_name}'...")
    query = {"name": query_name}
    doc = collection.find_one(query)
    if not doc:
        raise ValueError(f"No document found in collection '{collection_name}' with name '{query_name}'.")
    
    # Step 3: Load the GCLgrid object into memory
    print("Loading GCLgrid object from MongoDB...")
    gcl_grid = GCLdbread(db, doc, collection=collection_name)
    
    # Step 4: Convert the GCLgrid object to a VTK structured grid
    print("Converting GCLgrid to VTK structured grid...")
    vtk_grid = GCLfield2vtksg(gcl_grid, gridname=query_name)
    
    # Step 5: Save the VTK structured grid to a file
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    vtk_file_path = os.path.join(output_dir, vtk_file_name)
    print(f"Saving VTK structured grid to '{vtk_file_path}'...")
    vtkFieldWriter(vtk_grid, vtk_file_path, format="vts", use_binary=True)
    
    print(f"Conversion complete. VTK file saved at: {vtk_file_path}")

convert_gcl_to_vtk(MONGO_URI, COLLECTION_NAME, QUERY_NAME, OUTPUT_DIR, VTK_FILE_NAME)
