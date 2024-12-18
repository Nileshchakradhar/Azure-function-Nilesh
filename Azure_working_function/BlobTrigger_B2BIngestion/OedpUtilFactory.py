import time
import os
from BlobTrigger_B2BIngestion.Oedpblob import *
from BlobTrigger_B2BIngestion.OedpCommonUtil import *
from BlobTrigger_B2BIngestion.OedpLogger import *
from BlobTrigger_B2BIngestion.OedpConstants import *
from BlobTrigger_B2BIngestion.OedpExceptions import *
from BlobTrigger_B2BIngestion.OedpIO import *
from BlobTrigger_B2BIngestion.Oedpstoragequeue import *

class OedpUtilFactory:
    """_summary_
    Raises:
        OedpCustomException: _description_
    Returns:
        _type_: _description_
    """
    def __init__(self, environment, myblob):
        try:
            self.__message_id = ''
            self.__Oedp_logger = None
            self.__environment = environment
            self.__myblob = myblob
            self.__input_location = OedpFileInfo()
            self.__output_location = OedpFileInfo()
            self.__general_error_location = OedpFileInfo()
            self.__file_transfer_error_location = OedpFileInfo()
            self.__soap_output_location = OedpFileInfo()
            
            self.__sftp_queue = os.getenv('SFTP_QUEUE_NAME') #need to add in the local.setting.json
            self.__soap_queue = os.getenv('SOAP_QUEUE_NAME')
            self.create_io_locations()
            self.__Oedp_logger = OedpLogger(self.__message_id, myblob)    #None
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating OedpUtilfactory: Runtime error {e}.')
            

        
        
        
    def create_io_locations(self):
        '''
        it create the input and output location for the parameters
        and set the values for the parameters 
        '''
        # input path in Azure is container and blob name, in local is base location
        self.input_location.container = self.__myblob.container
        self.sftp_location.container = self.input_location.container
        self.general_error_location.container = self.input_location.container
        self.file_transfer_error.container = self.input_location.container
        self.soap_location.container = self.input_location.container  # os.getenv
        self.input_location.file_name = self.__myblob.blob_name  #XQ0093_234030A_20230207160000_001     get_filename_from_blob_name(self.__myblob.blob_name)
        self.input_location.key_prefix = self.__myblob.folder_name +'/'  #key.removesuffix(self.input_location.file_name)  #Incoming/
        folder_name = self.__myblob.folder_name #get_main_folder_from_key(self.input_location.key_prefix) #Incoming
        
        ##self.input_location.blob_name = folder_name + self.__input_location.blob_name
        ##########################################################
     #   self.sftp_location.blob_name = 'sftp_files/' + self.input_location.blob_name
        self.sftp_location.key_prefix = 'SFTPOutgoing/' #+ self.input_location.blob_name # remove the prefix folder name
        self.soap_location.key_prefix = 'SOAPOutgoing/'# + self.input_location.blob_name
    #    self.sftp_location.blob_name = 'soap_files/' + self.input_location.blob_name # remove the prefix folder name
        self.general_error_location.key_prefix = 'Error/'   #+'Invalidfile/'  #+ self.input_location.file_name.removeprefix(
           # folder_name)  #+ self.__input_location.file_name + '.err'
        self.file_transfer_error.key_prefix = 'Error' + self.input_location.file_name.removeprefix(
            folder_name) + 'TransferError/' #+ self.__input_location.file_name + '.err'
        self.general_error_location.file_name = self.input_location.file_name + '.err'
        self.file_transfer_error.file_name = self.input_location.file_name + '.err'
        self.sftp_location.file_name = self.input_location.file_name
        guid = self.__myblob.etag
        self.__message_id = f'FileIdentifier_{guid}_{self.input_location.file_name}'


    @property
    def sftp_queue(self):
        return self.__sftp_queue

    @sftp_queue.setter
    def sftp_queue(self, url):
        self.__sftp_queue = url

    @property
    def soap_queue(self):
        return self.__soap_queue

    @soap_queue.setter
    def soap_queue(self, url):
        self.__soap_queue = url

    @property
    def message_id(self):
        return self.__message_id

    @property
    def myblob(self):
        return self.__myblob

    @property
    def environment(self):
        return self.__environment

    @property
    def input_location(self):
        return self.__input_location

    @input_location.setter
    def input_location(self, input_location: OedpFileInfo):
        self.input_location = input_location

    @property
    def sftp_location(self):
        return self.__output_location

    @sftp_location.setter
    def sftp_location(self, output_location: OedpFileInfo):
        self.__output_location = output_location

    @property
    def soap_location(self):
        return self.__soap_output_location

    @soap_location.setter
    def soap_location(self, output_location: OedpFileInfo):
        self.__soap_output_location = output_location

    @property
    def general_error_location(self):
        return self.__general_error_location

    @general_error_location.setter
    def general_error_location(self, err_location: OedpFileInfo):
        self.__general_error_location = err_location

    @property
    def file_transfer_error(self):
        return self.__file_transfer_error_location

    @file_transfer_error.setter
    def file_transfer_error(self, err_location: OedpFileInfo):
        self.__file_transfer_error_location = err_location

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger.logger


    def get_reader(self):
        """
            Method : get_reader(self)
            Input  : input_path_or_bucket, input_file_name
            Output : (provides file or S3 reader based on execution environment)
        """
        return OedpAzureReader(self.Oedp_logger)


    def get_writer(self):
        """
            Method : get_writer(self)
            Input  : output_path_or_bucket=str(''), output_file_name=str(''),
                    input_path_or_bucket=str(''), input_file_name=str('')
            Output : (provides file or S3 writer based on execution environment)
        """
        return OedpAzureWriter(self.Oedp_logger)

    def get_storage_queue_reader(self):
        return OedpStorageQueueReader(self.Oedp_logger, self.soap_queue)

    def get_soap_queue_writer(self):
        return OedpStorageQueueWriter(self.Oedp_logger, self.soap_queue)
    
    def get_sftp_queue_writer(self):
        return OedpStorageQueueWriter(self.Oedp_logger, self.sftp_queue)

    @classmethod
    def get_RICEC150_file_name(cls, SINCOM ):
        current_time = time.time()
        # RIVR1001.XQ0093_<RRDI code of the hub>_<timestamp>_incrementation
        readable_time = time.strftime('%Y%m%d%H%M%S', time.localtime(current_time))
        file_name = f'RIVR1001.XQ0093_{SINCOM.strip()}_{readable_time}'
        return file_name

    @classmethod
    def get_RIVF00_file_name( cls, SINCOM):
        #RICFA000.XQ0093_<RRDI code of the hub>_<timestamp>_<incrementation>
        current_time = time.time()
        readable_time = time.strftime('%Y%m%d%H%M%S', time.localtime(current_time))
        file_name = f'RICFA000.XQ0093_{SINCOM.strip()}_{readable_time}'
        return file_name

    @classmethod
    def get_sftp_file_name( cls, orig_file_name):

        file_name = None
        if str(orig_file_name).startswith(DEFAULT_FILE_PREFIX):
            file_name = FRONTAL_FILE_PREFIX + orig_file_name
        elif str(orig_file_name).startswith(PLAQUEPR_FILE_PREFIX):
            file_name = FRONTAL_FILE_PREFIX + orig_file_name[4:]
        else:
            return orig_file_name
        return file_name
