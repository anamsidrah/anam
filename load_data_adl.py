from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import os
from urllib.parse import urlparse
from pathlib import Path


# Define function to get file extension from Azure Data Lake file url

def extract_file_extension(blob_url):
    parsed_url = urlparse(blob_url)
    path = Path(parsed_url.path)
    filename = path.name
#    extension = path.suffix
    return filename

# Enter credentials

account_name = os.environ['ADL_ACCOUNT_NAME']
account_key = os.environ['ADL_ACCOUNT_KEY']
container_name = os.environ['ADL_CONTAINER_NAME']

# Create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

# Get a list of all blob files in the container
blob_list = []
for blob_i in container_client.list_blobs():
    blob_list.append(blob_i.name)
    
df_list = []

# Generate a shared access signiture for files and load them into Python

for blob_i in blob_list:
    #generate a shared access signature for each blob file
    sas_i = generate_blob_sas(account_name = account_name,
                                container_name = container_name,
                                blob_name = blob_i,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
    
    sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas_i
    
    filename=extract_file_extension(sas_url)
    file_ext=os.path.splitext(filename)[1:]
    extension = file_ext[0][1:]
  
     
    if extension == 'pdf':
        print(f"This {filename} contains pdf file")
    elif extension == 'csv':
        df = pd.read_csv(sas_url)
        df_list.append(df)
    elif extension in ('jpg', 'png', 'jpeg'):
        print(f"This {filename} contains image file")
    else:
        print('This seems to be document in word format')
 
#    df_list.append(df)
     
#df_combined = pd.concat(df_list, ignore_index=True)
