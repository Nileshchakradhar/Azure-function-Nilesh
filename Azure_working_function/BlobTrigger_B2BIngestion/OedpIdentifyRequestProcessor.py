import json
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.queue import QueueServiceClient, QueueClient
from BlobTrigger_B2BIngestion.OedpUtilFactory import OedpUtilFactory
from BlobTrigger_B2BIngestion.OedpExceptions import *     
from BlobTrigger_B2BIngestion.OedpValidations import *
from BlobTrigger_B2BIngestion.OedpIdentify import *   
from BlobTrigger_B2BIngestion.SftpSoapRecordProcessor import *
from BlobTrigger_B2BIngestion.Oedpstoragequeue import *

def process_request(environment, event):
    '''
    it is the main function which calls all the classes and functions
    and implement all the methods in these function
    '''
    util_factory = OedpUtilFactory(environment, event)
    
    try:
        input_container = util_factory.input_location.container
        input_key_prefix = util_factory.input_location.key_prefix
        input_file_name = util_factory.input_location.file_name
        Oedp_logger = util_factory.Oedp_logger
        reader = util_factory.get_reader() # used to read data from blob container.
        writer = util_factory.get_writer() # handles the writing of data to a blob container
        
        Oedp_logger.info(f'Received {input_container}/{input_key_prefix + input_file_name}.')
        
        if not IsValidFileName(input_file_name):
            raise FileNotSupportedException(f'Functional: File name : {input_file_name}, not supported.')

        # The function then reads the contents of the file using the reader object and
        # splits the records into three lists - sftp, soap, and error records.
        lines = reader.read(input_container, input_key_prefix + input_file_name)
        sftpList, soapList, ErrorList = OedpIdentify.split_records(lines, Oedp_logger)
        if len(sftpList) == 0 and len(soapList) == 0 and len(ErrorList) == 0:
            raise EmptyOrInvalidRecords(f'Functional: File Validation Failure. No valid records found in file {input_file_name}.')
          
        
        def create_storage_queue_Message(output_key, output_etag):
            """
                It creates an Azure Queue Storage message with the required parameters, 
                such as the input container, input file name, output file name, message ID, and output ETag. 
                The function returns the created message. 
            """            
            try:
                # Create a message object with the required parameters
                msg = {
                    "Blobcontainer": input_container,
                    "InputFileName": input_key_prefix + input_file_name,
                    "OutputFileName": output_key,
                    "MessageId": util_factory.message_id,
                    "Output_ETag": str(output_etag).replace('"', '')
                }
                # Encode the message as JSON
                message_str = json.dumps(msg)
                # Add the message to the queue
                return message_str
            except Exception as e:
                raise OedpCustomException(f'Functional: Error creating Azure Queue Storage message. {e}')
        
        def send_msg_to_storage_queue(storage_queue_writer, out_file_key, out_etag):
            """_summary_

            Args:
                storage_queue_writer (_type_): name of the queue writer
                out_file_key (_type_): _output file name
                out_etag (_type_): output etag

            Raises:
                queue_e: _description_

            Returns:
                _type_: msg after writting it from the storage queue writer
            """            
            try:
                queue_msg = create_storage_queue_Message(out_file_key, out_etag)
                queue_msg = f"{queue_msg}" #convert the queue_msg into string
                res = storage_queue_writer.write(queue_msg)#queue_client.send_message(queue_msg) #send the message to the specified Storage Queue.
                Oedp_logger.info(f'Message sent to Storage Queue : {queue_msg}.')
                queue_msg = None
                return res
            except Exception as queue_e:
                raise queue_e
        
        def write_records(rec_list, container, blob_name, no_of_recs):
            """_summary_

            Args:
                rec_list (_type_): _description_
                container (_type_): name of the container
                blob_name (_type_): name of the blob
                no_of_recs (_type_): _description_

            Raises:
                ex: _description_

            Returns:
                _type_: response by writting the record in azure blob storage
            """            
            try:
                # write the records in Azure blob storage
                response = writer.write(convert_list_to_str(rec_list), container, blob_name)
                Oedp_logger.debug(f'{no_of_recs} records written to {container}/{blob_name}.')
                return response
            except Exception as ex:
                raise ex
            
        if len(ErrorList) > 0:
            no_of_faulty_records = len(OedpIdentify.GetHeaders(ErrorList))
            Oedp_logger.warning(f'Failed processing {no_of_faulty_records} records. :%s')
            write_records(ErrorList,
                          util_factory.general_error_location.container,  #need to correct
                          util_factory.general_error_location.key_prefix + util_factory.general_error_location.file_name,
                          no_of_faulty_records)
        SINCOM = OedpIdentify.GetCustomerCode(lines[0])
        
        if len(sftpList) > 0:
            no_of_sftp_records = len(OedpIdentify.GetHeaders(sftpList))
            sftp_file_name = util_factory.get_sftp_file_name(util_factory.sftp_location.file_name)
            res = write_records(sftpList,
                                util_factory.sftp_location.container,
                                util_factory.sftp_location.key_prefix + sftp_file_name,
                                no_of_sftp_records)
            
            sftp_queue_writer = util_factory.get_sftp_queue_writer() # handles the writing of messages to a queue
                        
            send_msg_to_storage_queue(sftp_queue_writer,
                            util_factory.sftp_location.key_prefix + sftp_file_name,
                            res.get_blob_properties().etag)

        if len(soapList) > 0:
            soap_processor = soapProcessor(Oedp_logger)
            RiceList, RivList = soap_processor.process_soap(soapList)

            no_of_rice_recs = len(RiceList)
            if no_of_rice_recs > 0:
                util_factory.soap_location.file_name = util_factory.get_RICEC150_file_name(SINCOM) #############
                res = write_records(RiceList,
                                    util_factory.soap_location.container,
                                    util_factory.soap_location.key_prefix + util_factory.soap_location.file_name,
                                    no_of_rice_recs)
                
                soap_queue_writer = util_factory.get_soap_queue_writer()

                send_msg_to_storage_queue(soap_queue_writer,
                                util_factory.soap_location.key_prefix + util_factory.soap_location.file_name,
                                res.get_blob_properties().etag)

            no_of_riv_recs = len(RivList)
            if no_of_riv_recs > 0:
                util_factory.soap_location.file_name = util_factory.get_RIVF00_file_name(SINCOM)  ###############
                res = write_records(convert_list_to_str(RivList),
                                    util_factory.soap_location.container,
                                    util_factory.soap_location.key_prefix + util_factory.soap_location.file_name,
                                    no_of_riv_recs)
                print(type(res))
               
                print(res.get_blob_properties().etag)
                send_msg_to_storage_queue(util_factory.soap_queue,
                                util_factory.soap_location.key_prefix + util_factory.soap_location.file_name,
                                res.get_blob_properties().etag)

        writer.delete_object(input_container, input_key_prefix + input_file_name)
        Oedp_logger.info(f'File removed from {input_container}/{input_key_prefix+input_file_name}')
        return 0

    except (FileNotSupportedException, Exception) as e:
        Oedp_logger.error(e)
        # if file is not supported then it will move to general error container
        writer.move(input_container,
                    input_key_prefix + input_file_name,
                    util_factory.general_error_location.container,
                    util_factory.general_error_location.key_prefix + util_factory.general_error_location.file_name)

        Oedp_logger.error(f'Technical: File moved from {input_container}/{input_file_name} to '
                        f'{util_factory.general_error_location.container}/'
                        f'{util_factory.general_error_location.file_name}.')
        return -1
