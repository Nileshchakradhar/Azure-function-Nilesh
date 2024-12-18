import logging
import uuid
import json
import azure.functions as func
from SFTP_QueueTrigger_B2BIngestion.Event import Event
from SFTP_QueueTrigger_B2BIngestion.OedpSftpFileTransfer import process_storage_queue_msg
from SFTP_QueueTrigger_B2BIngestion.Oedpblob import OedpAzureReader, OedpAzureWriter
from SFTP_QueueTrigger_B2BIngestion.OedpLogger import OedpLogger

def main(msg: func.QueueMessage) -> None: 
    ''' 
    main function to trigger the SFTP function 
    '''
    # Retrieve the receipt handle from the 'receipt_handle' attribute
    request_id = str(uuid.uuid1())
    # Retrieve the message content from the 'body' attribute
    message_body = msg.get_body().decode('utf-8')
    logging.info('Python queue trigger function processed a queue item: %s', message_body)
    # Retrieve the receipt handle from the 'pop_receipt' attribute
    pop_receipt = msg.pop_receipt
    logging.info('Receipt Handle: %s', pop_receipt)
    
    message_dict = json.loads(message_body)

    Blobcontainer = message_dict["Blobcontainer"]
    InputFileName = message_dict["InputFileName"]
    OutputFileName = message_dict["OutputFileName"]
    MessageId = message_dict["MessageId"]
    

    event = Event(pop_receipt, message_body, Blobcontainer, InputFileName, OutputFileName, MessageId, request_id)
    result = process_storage_queue_msg(event)
    # so the "event" parameter will contain details about the message that has been received.
    # so this function basically parse the message, extract the relevant information and then
    # take appropriate action based on the message content.
    # the purpose of the function is to transfer files to an SFTP server,
    if result == 0:
        print("SFTP file processed successfully ON SFTP server")
    else:
        print("Error processing file. Check log for details.")
