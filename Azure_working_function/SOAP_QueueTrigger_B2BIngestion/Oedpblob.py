import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from SOAP_QueueTrigger_B2BIngestion.OedpIO import *
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *


class OedpAzureReader(OedpReader):
    """_summary_
    Args:
        OedpReader (_type_): _description_
    """    
    def __init__(self, Oedp_logger):
        self.Oedp_logger = Oedp_logger
        
        # Retrieve the connection string from the environment variable
        self.__connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        # Create a BlobServiceClient object to access the storage account
        self.__blob_service_client = BlobServiceClient.from_connection_string(self.__connection_string)
        
        

    @property
    def blob_service_client(self):
        return self.__blob_service_client


    def read(self, input_container, input_file_name):
        """
        Method : read(self)
        Input: : None
        Output : lines (contents of the said input Azure blob container,blob)
        """
        try:
        # Get a BlobClient object to access the blob
            blob_client = self.blob_service_client.get_blob_client(input_container, input_file_name)
            # Download the blob content as bytes
            lines = blob_client.download_blob().readall()
            # Decode the bytes as ISO-8859-1 and split by lines
            lines = lines.decode('ISO-8859-1')
            lines = lines.splitlines()
            return lines
    
        except FileNotFoundError as fnf:
            raise OedpCustomException(f'Technical: File not found : {input_container}/{input_file_name}. {fnf}')
        except Exception as e:
            raise OedpCustomException(f'Technical: Error reading {input_container}/{input_file_name}. {e}')

    
    def read_binary(self, input_container, input_blob):
        """_summary_:  read the blob as a binary

        Args:
            input_container (_type_): name of the container
            input_blob (_type_): name of the blob

        Raises:
            OedpCustomException: _description_
            OedpCustomException: _description_

        Returns:
            _type_: file data after reading it
        """      
        try:
        # Get a BlobClient object to access the blob
            blob_client = self.blob_service_client.get_blob_client(input_container, input_blob)
            # Download the blob content as bytes
            lines = blob_client.download_blob().readall()
            return lines
        except FileNotFoundError as fnf:
            raise OedpCustomException(f'Technical: File not found : {input_container}/{input_blob}. {fnf}')
        except Exception as e:
            raise OedpCustomException(f'Technical: Error reading {input_container}/{input_blob}. {e}')


    def download_file(self, container, blob, temp_location):
        """_summary_ : download the blob in a temperary location from the container

        Args:
            container (_type_): name of the container
            blob (_type_): name of the blob
            temp_location (_type_): temperary location where the file will download

        Raises:
            OedpCustomException: _description_

        Returns:
            _type_: temperary location
        """        
        try:
            
            blob_client = self.blob_service_client.get_blob_client(container, blob)
            with open(temp_location, mode="wb") as download_file:
            #you should have write access to download the blob from the storage
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())   #C:\\Users\\abc\\custom_temp_directory//SFTPOutgoing/RRMTXQ0093_234030A_20230203191200_001

            return temp_location
        except Exception as e:
            raise OedpCustomException(f'Technical: Error downloading file {container}/{blob}. {e}')

class OedpAzureWriter(OedpWriter):
    """
    *****************************************************
    class OedpAzureWriter(OedpWriter):
    *****************************************************
    Class that defines write, copy, move, delete_blob
    operations from a given input stream to an Azure output container/blob
    Implements abstract methods of OedpWriter.
    """

    def __init__(self, Oedp_logger):
        self.Oedp_logger = Oedp_logger
        account_url = os.environ['AZURE_ACCOUNT_URL']
        account_key = os.environ['ACCOUNT_KEY']
        self.__blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        #self.__blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())

    @property
    def blob_service_client(self):
        return self.__blob_service_client


    def write(self, lines, input_container, input_blob):
        """
        Method : write(self)
        Input: : lines
        Output : (writes lines provided in the input to an output container/blob)
        """
        try:
            print(input_container, )
            response = self.blob_service_client.get_container_client(input_container).upload_blob(input_blob, lines, overwrite=True)  #
            return response
        
        # except NoCredentialsError as nce:
        #     raise OedpCustomException(f'Technical: Credentials error while accessing {input_container}. {nce}')
        except Exception as e:
            raise OedpCustomException(f'Technical: Could not write file {input_container}/{input_blob}. {e}')

    
    def copy(self, input_container, input_blob, output_container, output_blob):
        """
            Method: copy(self)
            Input: input_container=str(''), input_blob=str(''),
                   output_container=str(''), output_blob=str('')
            Output: Copies files from the given input container/blob to the output container/blob
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container=input_container, blob=input_blob)
            output_blob_client = self.blob_service_client.get_blob_client(container=output_container, blob=output_blob)
            output_blob_client.start_copy_from_url(blob_client.url)
            return output_blob_client.get_blob_properties()
        except Exception as err:
            raise Exception(f'Technical: Error copying {input_container}/{input_blob} to '
                                  f'{output_container}/{output_blob}. {err}')
    


    def move(self, input_container, input_blob,
    output_container, output_blob):
        """
        Method : move(self)
        Input : input_container=str(''), input_blob=str(''),
        output_container=str(''), output_blob=str('')
        Output : (moves blobs from a given input container/blob to output container/blob)
        """
        try:
            self.copy(input_container, input_blob,
            output_container, output_blob)
            self.delete_object(input_container, input_blob)
        except Exception as err:
            raise Exception(f'Technical: Error moving {input_container}/{input_blob} to '
                                  f'{output_container}/{output_blob}. {err}')


    def delete_object(self, input_container, input_blob):
        """
        Method : delete_object(self)
        Input : None
        Output : (deletes blob from input container)
        """
        try:
            self.blob_service_client.get_blob_client(input_container, input_blob).delete_blob()
        except Exception as err:
            raise OedpCustomException(f'Technical: Error deleting {input_container}:{input_blob}. {err}')

