import os
from azure.storage.queue import QueueClient, QueueMessage
from abc import ABC
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *

class OedpStorageQueueIO(ABC):
    def __init__(self, Oedp_logger, queue_name):
        self.Oedp_logger = Oedp_logger
        self.__connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        self.__queue_client = QueueClient.from_connection_string(self.__connection_string, queue_name)
        

    @property
    def queue_client(self):
        return self.__queue_client

    def delete_object(self, message, pop_receipt):  #message_id
        """
        Method : delete_object(self)
        Input : None
        Output : (deletes blob from input container)
        """
        try:
            res = self.queue_client.delete_message(message, pop_receipt)   #message_id
            return res
        except Exception as e:
            raise OedpCustomException(f'Technical: Storage Queue delete object error for message id: {message}. {e}')
        
class OedpStorageQueueReader(OedpStorageQueueIO):
    def __init__(self, Oedp_logger, queue_name):
        super().__init__(Oedp_logger, queue_name)

    def read(self):
        """
        Method : read(self)
        Input: : None
        Output : lines (contents of the said input Azure blob container,blob)
        """
        try:
            messages = self.queue_client.receive_messages()
            for message in messages:
                return message.content
        except Exception as e:
            raise OedpCustomException(f'Technical: Storage Queue Read error. {e}')


class OedpStorageQueueWriter(OedpStorageQueueIO):
    def __init__(self, Oedp_logger, queue_name):
        super().__init__(Oedp_logger, queue_name)

    def write(self, message):
        """
        Method : write(self)
        Input: : lines
        Output : (writes lines provided in the input to an output container/blob)
        """
        try:
            response = self.queue_client.send_message(message)
            return response
        except Exception as e:
            raise OedpCustomException(f'Technical: Error writing to Storage Queue {message}. {e}')

