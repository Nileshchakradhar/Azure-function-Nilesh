import os
from SOAP_QueueTrigger_B2BIngestion.OedpSoapUtilFactory import *
import tempfile
import gzip

def process_storage_queue_msg(event):
    '''
    it is the main function which calls all the classes and functions
    and implement all the methods in these function
    '''
    try:
        # util_factory class is responsible for creating and managing various
        # utility objects required for processing the storage_queue message.
        util_factory = OedpSoapUtilFactory(event)
        Oedp_logger = util_factory.Oedp_logger
        Oedp_logger.info(f'Recived message : %s{util_factory.storage_queue_message}')
        # objects are used to manage connections to external services and perform various tasks related to processing the message.
        storage_queue_writer = util_factory.get_storage_queue_writer()
        secret_manager = util_factory.get_secret_manager()
        connection_manager = util_factory.get_connection_manager()
        response_parser = util_factory.get_response_parser()
        blob_writer = util_factory.get_blob_writer()
        container = util_factory.container
        key = util_factory.output_key
        # if connect_pwd fails we do not continue, raise and exception and exit
        # to establish a connection to the remote server
        #START OF SOAP CALL
        ret_val = connect_pwd(Oedp_logger, connection_manager, util_factory,
                              secret_manager, response_parser)
        # If the connection is established successfully then the return value of the function is 0
        if 0 == ret_val:
            Oedp_logger.debug('soapConnectPWD successful')

        ret_val = soap_login(Oedp_logger, connection_manager, util_factory,
                            secret_manager, response_parser)
        
        # after logout it deletes the original message from the storage_queue queue using the storage_queue_writer object.
        blob_writer.delete_object(container, key)
        Oedp_logger.info(f'File {container}/{key} removed.')
        return 0

    except ErrorB4LoggerInstance as eb:
        print(f"Error executing lambda {eb}")
    except Exception as e:
        Oedp_logger.error(f"{e}")
        blob_writer.move(container, key, container, util_factory.file_transfer_error_location ) #+ file_name
        Oedp_logger.error(f"Functional: File moved from {container}:{key} to "
                         f"{container} , {util_factory.file_transfer_error_location}.")
        return -1


def soap_login(Oedp_logger, connection_manager, util_factory,
              secret_manager, response_parser):
    """_summary_: it login to the soap endpoint

    Args:
        Oedp_logger (_type_): _description_
        connection_manager (_type_): _description_
        util_factory (_type_): _description_
        secret_manager (_type_): _description_
        response_parser (_type_): _description_

    Returns:
        _type_: _description_
    """
    login_attempts = 0
    # that the function will attempt to log in to the soap system a maximum of two times.
    while login_attempts < 2:
        login_attempts += 1
        res = connection_manager.soap_login(util_factory.post_url, response_parser.session_ticket,
                                           response_parser.session_service_id, util_factory.soap_market_code,
                                           util_factory.soap_brand, util_factory.sincom)
        Oedp_logger.debug(f"soapLogin response: {res.text}")
        # to parse the response and extract the login return value 0
        response_parser.parse_login_response(res.text)
    return response_parser.login_return_value


def soap_logout(Oedp_logger, connection_manager, util_factory,
               secret_manager, response_parser):
    """_summary_ it logout from the soap endpoint

    Args:
        Oedp_logger (_type_): _description_
        connection_manager (_type_): _description_
        util_factory (_type_): _description_
        secret_manager (_type_): _description_
        response_parser (_type_): _description_
    """
    res = connection_manager.soap_logout(util_factory.post_url,
                                        response_parser.session_ticket,
                                        response_parser.session_service_id,
                                        response_parser.soap_login_id)
    Oedp_logger.debug(f'Logout response: {res.text}')

# 
# secret_manager for managing secrets such as passwords, and response_parser for parsing response messages.
def connect_pwd(Oedp_logger, connection_manager, util_factory,
                secret_manager, response_parser):
    """_summary_: connection_manager for managing connections, util_factory for creating connection URLs,
    secret_manager for managing secrets such as passwords, and response_parser for parsing response messages.

    Args:
        Oedp_logger (_type_): _description_
        connection_manager (_type_): _description_
        util_factory (_type_): _description_
        secret_manager (_type_): _description_
        response_parser (_type_): _description_

    Raises:
        soapTransferError: _description_
        soapTransferError: _description_
        OedpCustomException: _description_

    Returns:
        _type_: _description_
    """
    login_attempts = 0
    connection_success = False
    try:
        res = connection_manager.connect_pwd(util_factory.connection_url,
                                             secret_manager.user_name,
                                             secret_manager.passcode)
        Oedp_logger.info(f'response of connect_pwd: {res.text}')
        # The response from the system is then parsed using the response_parser object.
        response_parser.parse_connect_pwd_response(res.text, Oedp_logger)
        while login_attempts < 2 and not connection_success:
            login_attempts += 1

            if response_parser.connection_return_value == -1 or response_parser.connection_return_value == -6:  # invalid user
                raise soapTransferError(f"Functional: Error connecting to : {util_factory.connection_url}. Invalid "
                                       f"user name or password")
            elif response_parser.connection_return_value == 0:  # success
                # response passcode same as secret passcode. Password is not expired
                if response_parser.pass_code == secret_manager.passcode:
                    # if parser_passcode = secrete_passcode then the connection varible set to true
                    connection_success = True
                else:
                    Oedp_logger.debug(f'Password Expired')
                    #update_secret_manager(Oedp_logger, response_parser,
                                          #secret_manager, secret_manager.passcode)
                    res = connection_manager.connect_pwd(util_factory.connection_url,
                                                         secret_manager.user_name,
                                                         secret_manager.passcode)
                    response_parser.parse_connect_pwd_response(res.text , Oedp_logger)
                    connection_success = True

        #
        if not connection_success:
            raise soapTransferError(f"Functional: SOAP connection failure. {login_attempts} ConnectPwd attempts made.")

    except Exception as e:
        raise OedpCustomException(f'Technical: Error while performing soapConnectPWD request. {e}')

    Oedp_logger.debug(f"soapConnectPWD response: {res.text}")
    return response_parser.connection_return_value


def update_secret_manager(Oedp_logger, response_parser, secret_manager, new_pass_code):
    """_summary_ it update the secret manager

    Args:
        Oedp_logger (_type_): _description_
        response_parser (_type_): _description_
        secret_manager (_type_): _description_
        new_pass_code (_type_): _description_

    Raises:
        SecretUpdateError: _description_
        soapTransferError: _description_
    """
    try:
        res = secret_manager.update_pass_code(new_pass_code)
        Oedp_logger.debug(f"Reset Response: {res}")
        secret_manager.parse_secret_response(res)
        if response_parser.secret_return_value != 200:
            raise SecretUpdateError(f"Technical: Error updating secret. Response {response_parser.secret_return_value}")
    except Exception as e:
        raise soapTransferError(f"Technical: SOAP connection failure. {e}")
    secret_manager.reset_client()
    secret_manager.set_secret_values()

# this function is to compress the data and save it in the specified location using gzip compression.
def zip_file(data, location):
    """_summary_: it zip the file

    Args:
        data (_type_): _description_
        location (_type_): _description_

    Raises:
        soapTransferError: _description_
    """
    try:
        with gzip.open(location, 'wb') as out_file:
             # write the data to the compressed file.
            out_file.write(data)

    except Exception as e:
        raise soapTransferError(f"Technical: SOAP connection failure. {e}")


def get_data_to_transfer(util_factory, Oedp_logger):
    """_summary_: it get the data which has to be transfer from the utilfactory

    Args:
        util_factory (_type_): _description_
        Oedp_logger (_type_): _description_

    Raises:
        soapTransferError: _description_
        dde: _description_
        soapTransferError: _description_

    Returns:
        _type_: _description_
    """
    try:
        blob_reader = util_factory.get_blob_reader()
        container = util_factory.container
        key = util_factory.output_key
        data = blob_reader.read_binary(container, key)
        file_name = util_factory.get_filename_from_key(key)
        Oedp_logger.debug(f'Lines Type: {type(data)}. Read {data}')
    except Exception as e:
        raise soapTransferError(f'Technical: Error reading file {container}/{key}. {e}')
    try:
        Oedp_logger.debug(f'File Name : {file_name}')
        zip_filename = file_name + ".gz"
        temp_location = tempfile.gettempdir() + '/' + zip_filename
        Oedp_logger.debug(f'Temp location: {temp_location}')
        # The data variable is then compressed using the zip_file() method and saved in a temporary location on disk
        zip_file(data, temp_location)
        file_info_after_zip = os.stat(temp_location)
        Oedp_logger.debug(f'File info afterw zip : {file_info_after_zip}')
        # read the compress data from temp_location
        with open(temp_location, 'rb') as bf:
            zip_data = bf.read()

        return zip_data
    except soapTransferError as dde:
        raise dde
    except Exception as e:
        raise soapTransferError(f'Technical: Error creating temp file. {e}')


# 
def soap_send(Oedp_logger, connection_manager, util_factory,
             secret_manager, response_parser, data, file_format):
    """ 
    it sends data to a remote server in chunks
    """
    post_success = False
    post_attempt = 0

    # executes up to two times and continues to run as long as post_success is False.
    while post_attempt < 2 and not post_success:
        post_attempt += 1
        chunk_size = 500
        chunk_length = chunk_size * 2
        total_bytes = len(data)
        bytes_sent = 0
        # doc_id is initialized to an empty string,
        doc_id = ''
        operation = None

        if total_bytes < 500:
            chunk_size = total_bytes
            chunk_length = chunk_size * 2
            operation = "end:0"
        else:
            operation = "start"
        # data = hex(int.from_bytes(data, 'big'))

        hex_data = data.hex()
        no_of_calls = 0
        last_packet = False
        while bytes_sent < total_bytes:
            if no_of_calls > 0:
                doc_id = response_parser.doc_id
                operation = f'next:{bytes_sent}'
                if last_packet:
                    operation = "end:0"
            no_of_calls += 1
            # The response from the server is parsed using the response_parser.parse_post_response method,
            # and bytes_sent is incremented by the chunk_size.
            
            res = connection_manager.soap_post(util_factory.post_url, hex_data,
                                              response_parser.session_ticket,
                                              response_parser.session_service_id,
                                             response_parser.soap_login_id,
                                             doc_id, operation, chunk_length,
                                              file_format)
        
            Oedp_logger.debug(f'Post attempt {post_attempt}, Call {no_of_calls}')
            Oedp_logger.debug(f'Post response {res.text}')
            response_parser.parse_post_response(res.text)
            bytes_sent += chunk_size

            # If the session has expired, the code calls the connect_pwd and soap_login functions to reestablish the session.
            if session_expired(response_parser.post_return_value):
                ret_val = connect_pwd(Oedp_logger, connection_manager, util_factory,
                                      secret_manager, response_parser)

                ret_val = soap_login(Oedp_logger, connection_manager, util_factory,
                                    secret_manager, response_parser)
                post_success = False
                post_attempt = 0
                break
            # If the response_parser.post_return_value is 0, post_success is set to True.
            elif 0 == response_parser.post_return_value:
                post_success = True

            if 0 < (total_bytes - bytes_sent) < 500:
                chunk_size = total_bytes - bytes_sent
                chunk_length = chunk_size * 2
                last_packet = True
            Oedp_logger.debug(f'call {no_of_calls}, chunk size {chunk_size}, length {chunk_length}'
                             f'total bytes {total_bytes}, bytes sent {bytes_sent}, last_packet: {last_packet}')

        if response_parser.post_return_value == 0:
            post_success = True
    if post_attempt == 2 and not post_success:
        raise soapTransferError(f"Functional: soap post failure. {post_attempt} attempts made."
                              f"Reason: {response_parser.post_return_value}")

    return response_parser.post_return_value


def soap_commit(Oedp_logger, connection_manager, util_factory,
               secret_manager, response_parser):
    """_summary_: it sends the data related to the commit by the soap endpoint

    Args:
        Oedp_logger (_type_): _description_
        connection_manager (_type_): _description_
        util_factory (_type_): _description_
        secret_manager (_type_): _description_
        response_parser (_type_): _description_

    Returns:
        _type_: _description_
    """
    commit_attempts = 0

    while commit_attempts < 2:

        if commit_attempts > 0:
            ret_val = connect_pwd(Oedp_logger, connection_manager, util_factory,
                                  secret_manager, response_parser)

            ret_val = soap_login(Oedp_logger, connection_manager, util_factory,
                                secret_manager, response_parser)
        commit_attempts += 1
        
        res = connection_manager.soap_commit(util_factory.post_url,
                                            response_parser.session_ticket,
                                            response_parser.session_service_id,
                                            response_parser.soap_login_id,
                                            response_parser.doc_id)
        
        Oedp_logger.debug(f"No of Commit attempts {commit_attempts}")
        Oedp_logger.debug(f'commit response {res.text}')
        response_parser.parse_commit_response(res.text)

        if response_parser.commit_return_value == 0 or response_parser.commit_return_value == 72:
            break
        if session_expired(response_parser.commit_return_value):
            continue

    if commit_attempts > 0 and response_parser.commit_return_value != 0:
        soapTransferError(f"Functional: Commit failed. Reason: {response_parser.commit_return_value}")

    return response_parser.commit_return_value


def session_expired(ret_val):
    return ret_val == -1 or ret_val == -2
