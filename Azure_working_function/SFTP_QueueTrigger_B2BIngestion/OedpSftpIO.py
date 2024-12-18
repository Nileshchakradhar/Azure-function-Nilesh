import os
import paramiko
from SFTP_QueueTrigger_B2BIngestion.OedpExceptions import *


class OedpSftpIO:
    """
    this code sets up the basic infrastructure for an SFTP client, 
    including storing the necessary connection information and 
    creating the objects needed for secure file transfers.
    """  
    def __init__(self, server_url, user_name, key_location, Oedp_logger):
        self.__user_name = user_name
        self.__server_url = server_url
        self.__key_location = key_location
        self.__Oedp_logger = Oedp_logger
        # to create an SFTP transport object, which is responsible for handling the secure connection to the SFTP server.
        self.__transport = None
        # to create an SFTP client object, which can be used to interact with the server and perform file transfers.
        self.__sftp = None

    @property
    def sftp(self):
        return self.__sftp

    @sftp.setter
    def sftp(self, value):
        self.__sftp = value

    @property
    def user_name(self):
        return self.__user_name

    @user_name.setter
    def user_name(self, value):
        self.__user_name = value

    @property
    def server_url(self):
        return self.__server_url

    @property
    def key_location(self):
        return self.__key_location

    @property
    def transport(self):
        return self.__transport

    @transport.setter
    def transport(self, value):
        self.__transport = value

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger
    
    def open_connection(self):
        """
        This method is used to open an SFTP connection to the server.
        """
        try:
            self.Oedp_logger.info(f'Opening SFTP connection on {self.server_url} on port 22')
            self.transport = paramiko.Transport(self.server_url, 22)
            # get the username and password from environment variables
            username = os.environ['SFTP_USERNAME']
            password = os.environ['SFTP_PASSWORD']
            self.Oedp_logger.debug(f'UserName: {username}, password : {password}')
            # initiates the SFTP connection by calling the connect method of the Transport object,
            # specifying the username and the password.
            self.transport.connect(username=username, password=password)
        except Exception as e:
            raise SftpTransferError(f'Technical: Error opening SFTP connection. {e}')
        try:
            self.Oedp_logger.info(f'Opening SFTP connection on {self.server_url} on port 22')
            self.transport = paramiko.Transport(self.server_url, 22)            
            # get the username and password from environment variables
            username = os.environ['SFTP_USERNAME']
            password = os.environ['SFTP_PASSWORD']            
            self.Oedp_logger.debug(f'UserName: {username}, password : {password}')            
            # initiates the SFTP connection by calling the connect method of the Transport object,
            # specifying the username and the password.
            self.transport.connect(username=username, password=password)
        except Exception as e:
            raise SftpTransferError(f'Technical: Error opening SFTP connection. {e}')


    def start_sftp_session(self):
        """
        This method is used to start an SFTP session.
        """
        try:
            self.Oedp_logger.debug(f'Starting sftp session')
            res = self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.Oedp_logger.debug(f'Start Session Response: {res}')
        except Exception as e:
            raise SftpTransferError(f"Technical: Error starting SFTP Session. {e}")

    def copy(self, from_location, to_location):
        """
        This method is used to copy a file from one location to another.
        """
        try:
            self.start_sftp_session()
            self.Oedp_logger.debug(f'Copying from /{from_location} to {to_location}')
            res = self.sftp.put(f"{from_location}", to_location) #/
            self.Oedp_logger.debug(f'Copying response: {res}')
            return res
        except Exception as e:
            raise SftpTransferError(f'Technical: Error putting file from {from_location} to {to_location}. {e}')

    def get_file(self, from_location, to_location):
        """
        This method is used to get a file from one location to another.
        """
        try:
            self.start_sftp_session()
            self.Oedp_logger.debug(f'pulling from /{from_location} to {to_location}')
            self.sftp.get(from_location, to_location)   #res = 
            #self.Oedp_logger.debug(f'Get response: {res}')
        except Exception as e:
            raise SftpTransferError(f'Technical: Error downloading file from {from_location} to {to_location}. {e}')

    def close(self):
        """
        This method is used to close the SFTP connection.
        """
        try:
            self.Oedp_logger.info("Close SFTP Connection.")
            if self.sftp is not None:
                self.sftp.close()
                self.Oedp_logger.debug(f'SFTP Closed.')
                self.sftp = None
            if self.transport is not None:
                self.transport.close()
                self.Oedp_logger.debug(f'Transport Closed.')
                self.transport = None
        except Exception as e:
            raise OedpCustomException(f'Technical: Error closing SFTP connection. {e}')

    def get_home_directory(self):
        """
        This method is used to get the user's home directory.
        """
        try:
            self.open_connection()
            self.start_sftp_session()
            # not understand
            home_dir = self.sftp.normalize(".")
            self.close()
            return home_dir
        except Exception as e:
            raise OedpCustomException(f'Technical: Error getting user home directory. {e}')

    def get_list_of_files(self, path):
        """
        This method is used to get a list of files from a specified path.
        """
        try:
            list_files = self.sftp.listdir(path)
            return list_files
        except Exception as e:
            self.Oedp_logger.debug(f'Error listing files from {path}')
            raise OedpCustomException(f'Technical: Error getting file list. {e}')

    def get_file_attribute(self, file_name):
        """
        This method is used to get the attribute of a file.
        """
        try:
            # stat(): This method is used to get status of the specified path.
            file_attribute = self.sftp.stat(file_name)
            return file_attribute
        except Exception as e:
            self.Oedp_logger.debug(f'Error getting file attribute for {file_name}.')
            raise OedpCustomException(f'Technical: Error getting file attribute. {e}')
