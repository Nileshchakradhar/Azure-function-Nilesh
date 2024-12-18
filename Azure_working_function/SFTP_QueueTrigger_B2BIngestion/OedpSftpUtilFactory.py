import json
import tempfile

from SFTP_QueueTrigger_B2BIngestion.Oedpstoragequeue import *
from SFTP_QueueTrigger_B2BIngestion.OedpLogger import *
from SFTP_QueueTrigger_B2BIngestion.Oedpblob import *
from SFTP_QueueTrigger_B2BIngestion.OedpSftpIO import *
import ast
from SFTP_QueueTrigger_B2BIngestion.Event import Event
import logging
# basically it is used to transfer files to and from an SFTP server.
class OedpSftpUtilFactory:
    """_summary_: this class is used to assign the value to the parameter
    Raises:
        OedpCustomException: _description_
    Returns:
        _type_: _description_
    """
    def __init__(self, event):
        try:
# first initializes variables to store information about the file transfer
            self.__event = event
            self.__pop_receipt = self.__event.pop_receipt
            self.__storage_queue_msg = self.__event.message_body#ast.literal_eval(self.__event.message_body)
            self.__message_id = None
            self.__container = None
            self.__output_key = None
            self.__input_key = None
            self.parse_storage_queue_msg()
            #file_name = self.get_filename_from_key(self.__output_key)
            folder_name = self.__output_key[0:self.__output_key.find('/')]  
            self.__file_transfer_error_location = 'Error' + folder_name + 'TransferError/'
            # here it creates the instance of PsaLogger class to log any errors that may occur during the file transfer.
            self.__Oedp_logger = OedpLogger(self.__message_id, event)
            # os.getenv is used to get the environment variable
            # These variables are used by the OedpSftpUtil class to connect to the SFTP server and transfer files.
            self.__sftp_queue = os.getenv('SFTP_QUEUE')
            self.__user_name = os.getenv('SFTP_USER')
            self.__server_url = os.getenv('SFTP_SERVER_URL')
            # self.__source_arn = os.getenv('storage_queue_sftp_ARN','https://storage_queue.eu-west-3.amazonaws.com/178035008959/storage_queue-cvl-convlayer-dev01-psa')
            self.__source_arn = os.getenv('STORAGE_sftp_queue')
            self.__key_location = os.getcwd() + "/pKey/mykey_openssh.key" #'b2b-container'#
            self.__out_path = os.getenv('OUT_PATH')
        except KeyError as ke:
            logging.error(f'Key Errorr {ke}')
            raise ErrorB4LoggerInstance("Unexpected Error")
        except Exception as e:
            raise e
        
        
    @property
    def server_url(self):
        return self.__server_url

    @property
    def file_transfer_error_location(self):
        return self.__file_transfer_error_location

    @property
    def out_path(self):
        return self.__out_path

    @property
    def key_location(self):
        return self.__key_location

    @property
    def sftp_user(self):
        return self.__user_name

    @property
    def sftp_queue(self):
        return self.__sftp_queue

    @property
    def storage_queue_msg(self):
        return self.__storage_queue_msg
    
    
    
    @property
    def message_id(self):
        return self.__message_id
    
    
    

    @property
    def source_arn(self):
        return self.__source_arn

    @property
    def pop_receipt(self):
        return self.__pop_receipt

    @property
    def event(self):
        return self.__event

    @property
    def container(self):
        return self.__container

    @property
    def input_key(self):
        return self.__input_key

    @property
    def output_key(self):
        return self.__output_key

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger.logger

    # here is parse and extract relevant information from an storage_queue msg
    def parse_storage_queue_msg(self):
        self.__message_id = str(self.event.MessageId).replace('FileIdentifier', 'SFTPSend')
        self.__container = self.event.Blobcontainer
        self.__output_key = self.event.OutputFileName
        self.__input_key = self.event.InputFileName

    # reading data from azure blob storage.
    def get_blob_reader(self):
        """
            Method : get_reader(self)
            Input  : input_path_or_container, input_file_name
            Output : (provides file or blob storage reader )
        """
        return OedpAzureReader(self.Oedp_logger)



    # that defines write, copy, move, delete_object.
    # operations from a given input stream to a AWS output container/key
    def get_blob_writer(self):
        """
            Method : get_writer(self)
            Input  : output_path_or_container=str(''), output_file_name=str(''),
                    input_path_or_container=str(''), input_file_name=str('')
            Output : (provides file or blob storage writer)
        """
        return OedpAzureWriter(self.Oedp_logger)

    def get_storage_queue_reader(self):
        return OedpStorageQueueReader(self.Oedp_logger, self.sftp_queue)

    def get_storage_queue_writer(self):
        return OedpStorageQueueWriter(self.Oedp_logger, self.sftp_queue)

    def get_sftp_client(self):
        return OedpSftpIO(self.server_url, self.sftp_user, self.key_location, self.Oedp_logger)

    @staticmethod
    def get_filename_from_key(key: str):
        file_name = key[key.rfind('/') + 1:]
        return file_name

