import os

import pandas as pd
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

# Azure storage access info
azure_storage_account_name = "azureopendatastorage"
azure_storage_sas_token = r""
container_name = "holidaydatacontainer"
folder_name = "Processed"


if azure_storage_account_name is None or azure_storage_sas_token is None:
    raise Exception(
        "Provide your specific name and key for your Azure Storage account--see the Prerequisites section earlier."
    )

print(
    "Looking for the first parquet under the folder "
    + folder_name
    + ' in container "'
    + container_name
    + '"...'
)
container_url = f"https://{azure_storage_account_name}.blob.core.windows.net/"
blob_service_client = BlobServiceClient(
    container_url, azure_storage_sas_token if azure_storage_sas_token else None
)

container_client = blob_service_client.get_container_client(container_name)
blobs = container_client.list_blobs(folder_name)
sorted_blobs = sorted(list(blobs), key=lambda e: e.name, reverse=True)
targetBlobName = ""
for blob in sorted_blobs:
    if blob.name.startswith(folder_name) and blob.name.endswith(".parquet"):
        targetBlobName = blob.name
        break

print("Target blob to download: " + targetBlobName)
_, filename = os.path.split(targetBlobName)
blob_client = container_client.get_blob_client(targetBlobName)
with open(filename, "wb") as local_file:
    blob_client.download_blob().download_to_stream(local_file)

# Read the parquet file into Pandas data frame
print("Reading the parquet file into Pandas data frame")
df = pd.read_parquet(filename)

# Espen addition: save to a file with a simpler name
df.to_parquet("holidays.parquet.gzip", compression="gzip")
