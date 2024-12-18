import logging
import json
import uuid

import azure.functions as func
from SOAP_QueueTrigger_B2BIngestion.Event import Event
from SOAP_QueueTrigger_B2BIngestion.OedpSoapFileTransfer import process_storage_queue_msg


def main(msg: func.QueueMessage) -> None:
    ''' 
    main function to trigger the identify function 
    '''
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
    
    reciept_handle = msg.pop_receipt
    logging.info('reciept_handle: %s', reciept_handle)
    
    storage_queue_message = msg.get_body().decode('utf-8')
    logging.info('storage_queue_message: %s', storage_queue_message)
    
    # Generate request ID
    request_id = str(uuid.uuid1())
    logging.info('request_id: %s', request_id)
    
    message_dict = json.loads(storage_queue_message)

    MessageId = message_dict["MessageId"]
    Blobcontainer = message_dict["Blobcontainer"]
    InputFileName = message_dict["InputFileName"]
    OutputFileName = message_dict["OutputFileName"]
    
    event = Event(reciept_handle, storage_queue_message, MessageId, Blobcontainer, InputFileName, OutputFileName, request_id)
    
    result = process_storage_queue_msg(event)
    if result == 0:
            print("soap file processed successfully")
    else:
        print("Error processing soap file. Check log for details.")
