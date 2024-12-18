import tempfile
import uuid
import os

from SFTP_QueueTrigger_B2BIngestion.OedpSftpUtilFactory import *
import logging


def process_storage_queue_msg(event):
    '''
    it is the main function which calls all the classes and functions
    and implement all the methods in these function
    '''
    # It is generating a unique identifier for the execution using the uuid library.creates random
    # UUID (Universal Unique Identifier)
    logging.info(f'Execution uuid {uuid.uuid4()}')
    try:
        # util_factory class is responsible for creating and managing various
        # utility objects required for processing the Storage queue message.
        util_factory = OedpSftpUtilFactory(event)
        Oedp_logger = util_factory.Oedp_logger # initialize logging
        Oedp_logger.debug(f'Event: {event}') # debug used to give Detailed information
        Oedp_logger.info(f'Received message {util_factory.storage_queue_msg}.') # to confirm that things are working as expected
        # it retrieves the necessary utility objects from the sftpUtilityFactory instance
        sftp_client = util_factory.get_sftp_client()
        blob_reader = util_factory.get_blob_reader()
        blob_writer = util_factory.get_blob_writer()
        container = util_factory.container
        key = util_factory.output_key
        file_name = util_factory.get_filename_from_key(key)
        temp_location = os.path.join(tempfile.gettempdir(), file_name)
        res = blob_reader.download_file(container, key, temp_location)
        Oedp_logger.debug(f'blob download Successful  {res}.')
        # It then uses the SFTP client to copy the file to a designated location on an SFTP server.
        ################################################
        # for testing we are commenting the sftp server
        sftp_out_path = sftp_client.get_home_directory() + util_factory.out_path
        sftp_client.open_connection()     ##need to be tested when the sftp server available
        sftp_client.copy(temp_location, sftp_out_path + '/' + file_name)   #sftp_out_path + '/' + file_name   container + '/' + 'SFTPIncoming/'
        # if no Exceptions file copied successfully
        Oedp_logger.info(f"File {file_name} put at {sftp_out_path} successfully.")
        sftp_client.close()
        # if the file copied successfully then it will delete the file from blob storage
        blob_writer.delete_object(container, key)
        Oedp_logger.info(f'File deleted from {container}/{key}. ') 
        Oedp_logger.info(f"Message removed from queue {util_factory.source_arn}.")
        return 0
    except ErrorB4LoggerInstance as unexpected_error:
        logging.error(f'Technical: {unexpected_error}')
        return -1
    except (SftpTransferError, Exception) as ste:
        Oedp_logger.error(f"{ste}")
        file_name += ".err"
        # if an SFTP transfer error occurs, the function moves the file to an error location in the blob storage container and appends ".err" to the file name.
        blob_writer.move(container, key, container, util_factory.file_transfer_error_location + file_name)
        Oedp_logger.error(f"Technical: File moved from {container}:{key} to "
                         f"{container} , {util_factory.file_transfer_error_location}{file_name}.")
        Oedp_logger.error(f"Technical: Message removed from queue {util_factory.source_arn}.")
        return -1

