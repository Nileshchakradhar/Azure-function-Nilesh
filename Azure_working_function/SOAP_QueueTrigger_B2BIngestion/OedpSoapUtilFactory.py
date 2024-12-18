import ast
import os
from SOAP_QueueTrigger_B2BIngestion.OedpLogger import *
from SOAP_QueueTrigger_B2BIngestion.OedpSoapConnectionManager import *
from SOAP_QueueTrigger_B2BIngestion.OedpKeyVault import *
from SOAP_QueueTrigger_B2BIngestion.Oedpblob import *
from SOAP_QueueTrigger_B2BIngestion.Oedpstoragequeue import *
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *
from SOAP_QueueTrigger_B2BIngestion.OedpSoapResponseParser import *

SQS_FCA_MSG = {
	'S3Bucket': 's3-cvl-convlayer-dev-sftpstore',
	'InputFileName': 'Incoming/XQ0093.002',
	'OutputFileName': 'FCAOutgoing/MTS2/RICRXQ0093_20230117110805',
	'MessageId': '917f3d6bc7eb9594c9ed983a6db8a85c_XQ0093.002',
	'Output_ETag': 'ba9a2c8262355cba068921e67c40f336'
}

SQS_EVENT = {
	'Records': [{
		'messageId': '0248e68a-b2d3-422c-8077-563447671dc1',
		'receiptHandle': 'AQEBBnh0pCrnriiBgAdkAcjn4u3lLStfXYo0ACWCHGgkrVsEjzAZbjFXuutFYJExB3u4SC+12y8rzfaSgv070TNPjAe98BGuSK7KQ5ex/VZosf/ExLZheenaCr1z96qyp7aHsgl5ZecVEAbHflHRLaOq3A07QUzm3lFcL6gw0VlzQfqQj1JCSjSdnGB+tkSIpiWMr0NFhMgzP4Yt4jpS1X5E2x2btDtIJJkC87zWqA296/3vctElOMBtUgMvsjntkAQm5z7vYuM7gQCFCXssS1sQ8iKOu+zcfraaBynvUj1Ldh7HKLICfvDZpCx2bLRh1JUEIR9HoBpKi4yDC/6qD90PkNzFjM0kh9pg/KQblNas+varz/zzHm7kX7CtjX8+FlLR6LlTrMbEJcHFScM/XK6AzsFTGtxVd5jM7iWGy4Amjr0=',
		'body': "{'S3Bucket': 's3-cvl-convlayer-dev-sftpstore', 'InputFileName': 'Incoming/XQ0093DEU_FCA', 'OutputFileName': 'PSAOutgoing/MTS2/XQ0093DEU_FCA', 'MessageId': 'b16cf07516de86f8248c9954f1afec93_XQ0093DEU_FCA', 'Output_ETag': 'b466b0bf8c4d59d4b12bb17923dbc509'}",
		'attributes': {
			'ApproximateReceiveCount': '1',
			'SentTimestamp': '1673603863477',
			'SenderId': 'AROASS45WUW76DE7HYXBX:lmd-cvl-convlayer-dev-idfy-repairer',
			'ApproximateFirstReceiveTimestamp': '1673603863482'
		},
		'messageAttributes': {},
		'md5OfBody': 'afca9c32dd1dc3aa6d1a9ed373bc20d6',
		'eventSource': 'aws:sqs',
		'eventSourceARN': 'arn:aws:sqs:eu-west-3:178035008959:sqs-cvl-convlayer-dev01-psa',
		'awsRegion': 'eu-west-3'
	}]
}
class OedpSoapUtilFactory:
    def __init__(self, event):
        try:

            self.__event = event
            self.__soap_market_code = os.getenv('SOAP_MARKET_CODE')
            self.__soap_sincom = os.getenv('SOAP_SINCOM_CODE')
            self.__soap_brand = os.getenv('SOAP_BRAND')
            self.__source_arn = os.getenv("STORAGE_SOAP_QUEUE")
            self.__secret_name = os.getenv("SECRET_ARN")
            self.__key_vault_url = os.getenv("KEY_VAULT_URL")
            self.__connection_url = os.getenv("SOAP_CONNECTION_URL")
            self.__post_url = os.getenv("SOAP_POST_URL")


            self.__receiptHandle = self.__event.reciept_handle
            self.__storage_queue_message  = self.__event.storage_queue_message  #ast.literal_eval(self.event["Records"][0]["body"])
            self.__message_id = None
            self.__container = None
            self.__output_key = None
            self.__input_key = None
            
            self.__soap_queue = os.getenv('SOAP_QUEUE')
            
            
            self.parse_storage_queue_msg()
            file_name = self.get_filename_from_key(self.__output_key)
            folder_name = self.__output_key[0:self.__output_key.find('/')]
            folder_name = self.__output_key.removeprefix(folder_name)
            folder_name = folder_name.removesuffix(file_name)
            self.__file_transfer_error_location = 'Error' + folder_name + 'TransferError/'
            self.__Oedp_logger = OedpLogger(self.__message_id, event)

        except KeyError as ke:
            print(f'Key Errorr {ke}')
            raise ErrorB4LoggerInstance("Unexpected Error")
        except Exception as e:
            print(e)
            raise e
        
    @property
    def key_vault_url(self):
        return self.__key_vault_url

    @property
    def file_transfer_error_location(self):
        return self.__file_transfer_error_location

    @property
    def post_url(self):
        return self.__post_url
    
    @property
    def soap_queue(self):
        return self.__soap_queue

    @property
    def soap_brand(self):
        return self.__soap_brand

    @property
    def sincom(self):
        return self.__soap_sincom

    @property
    def soap_market_code(self):
        return self.__soap_market_code

    @property
    def connection_url(self):
        return self.__connection_url
    
    @property
    def storage_queue_message(self):
        return self.__storage_queue_message

    @property
    def source_arn(self):
        return self.__source_arn

    @property
    def receipt_handle(self):
        return self.__receiptHandle

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

    @property
    def secret_name(self):
        return self.__secret_name



    # here is parse and extract relevant information from an storage_queue msg
    def parse_storage_queue_msg(self):
        self.__message_id = str(self.event.MessageId).replace('FileIdentifier', 'SOAPSend')
        self.__container = self.event.Blobcontainer
        self.__output_key = self.event.OutputFileName
        self.__input_key = self.event.InputFileName


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
        
    # def get_sqs_writer(self):
    #     return OedpSqsWriter(self.Oedp_logger)
    def get_storage_queue_writer(self):
        return OedpStorageQueueWriter(self.Oedp_logger, self.soap_queue)
    
    def get_storage_queue_reader(self):
        return OedpStorageQueueReader(self.Oedp_logger, self.soap_queue)

    def get_connection_manager(self):
        return OedpSoapConnectionManager(self.Oedp_logger)

    def get_secret_manager(self):
        return OedpSecretsManager(self.Oedp_logger, self.secret_name, self.key_vault_url)

    def get_response_parser(self):
        return OedpSoapResponseParser(self.Oedp_logger)

    @staticmethod
    def get_filename_from_key(key: str):
        file_name = key[key.rfind('/') + 1:]
        return file_name

    @staticmethod
    def get_file_format(file_name: str):

        if file_name[0:5] == 'RIVR1':
            return 'RIVR1001'
        elif file_name[0:5] == 'RICFA':
            return 'RICFA000'
        else:
            return 'UNK'

