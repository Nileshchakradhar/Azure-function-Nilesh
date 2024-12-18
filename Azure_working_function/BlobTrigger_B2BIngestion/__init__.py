import json
import os
import logging
import uuid
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from BlobTrigger_B2BIngestion.Oedpblob import OedpAzureReader, OedpAzureWriter
from BlobTrigger_B2BIngestion.OedpIdentifyRequestProcessor import process_request
from BlobTrigger_B2BIngestion.Event import Event
from BlobTrigger_B2BIngestion.Oedpstoragequeue import OedpStorageQueueIO, OedpStorageQueueReader, OedpStorageQueueWriter

def main(myblob: func.InputStream):
    ''' 
    main function to trigger the identify function 
    '''
    logging.info(f"Python blob trigger function processed blob \n"f"Blob Size:  {myblob.length} bytes : %s")
    logging.info(func)

    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container = myblob.name.split('/')[0]
    folder_name = myblob.name.split('/')[1]
    blob = myblob.name.split('/')[2]

    try:
        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get blob client
        blob_client = blob_service_client.get_blob_client(container=container + '/' + folder_name, blob=blob)
        # Get blob properties
        blob_properties = blob_client.get_blob_properties()
        # Extract ETag from properties
        etag = blob_properties.etag
        # Generate request ID
        request_id = str(uuid.uuid1())
        # Create event object
        event = Event(container, folder_name, blob, etag, request_id)
        # Process the request
        result = process_request('Azure', event)
        if result == 0:
            print("file processed successfully")
        else:
            print("Error processing file. Check log for details.")
    except Exception as e:
        # Handle exceptions here if necessary
        logging.error(f"An error occurred: {e}")
        print("Error processing file. Check log for details.")