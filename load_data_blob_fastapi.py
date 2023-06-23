from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import os
from urllib.parse import urlparse
from pathlib import Path

app = FastAPI()

class AzureCredentials(BaseModel):
    account_name = os.environ['ADL_ACCOUNT_NAME']
    account_key = os.environ['ADL_ACCOUNT_KEY']
    container_name = os.environ['ADL_CONTAINER_NAME']

@app.post("/process_blobs/")
async def process_blobs(azure_cred: AzureCredentials):

    def extract_file_extension(blob_url):
        parsed_url = urlparse(blob_url)
        path = Path(parsed_url.path)
        filename = path.name
        return filename

    account_name = azure_cred.account_name
    account_key = azure_cred.account_key
    container_name = azure_cred.container_name

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_client = blob_service_client.get_container_client(container_name)

    blob_list = []
    for blob_i in container_client.list_blobs():
        blob_list.append(blob_i.name)
        
    df_list = []
    message_list = []

    for blob_i in blob_list:
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
            message_list.append(f"This {filename} contains pdf file")
        elif extension == 'csv':
            message_list.append(f"This {filename} contains csv file")
 #           df = pd.read_csv(sas_url)
 #           df_list.append(df)
        elif extension in ('jpg', 'png', 'jpeg'):
            message_list.append(f"This {filename} contains image file")
        else:
            message_list.append('This seems to be a document in a Word format')

#    return {"dataframes": df_list, "messages": message_list}
