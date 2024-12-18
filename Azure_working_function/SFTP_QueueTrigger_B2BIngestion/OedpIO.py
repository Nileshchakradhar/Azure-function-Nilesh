from abc import ABC, abstractmethod



class OedpReader(ABC):
    """
    class OedpReader(ABC):
    Abstract class that defines read operations from any stream
    Eg. File, SQS or S3. 
    The OedpFileInfo is meant to contain location information
    split in 3 parts. The Bucket, the intermedite path and the actual file name
    """
    def __init__(self):
        pass

    @abstractmethod
    def read(self, base_path, input_file_name):
        pass




class OedpWriter(ABC):
    """
    class OedpWriter(ABC):
    Abstract class that defines write, copy, move, delete_object
    operations from give input stream to output stream
    Eg. File, storageQueue or blob
    """
    def __init__(self):
        pass

    @abstractmethod
    def write(self, lines, base_path_or_container, blob_or_file_name):
        pass

    @abstractmethod
    def copy(self, input_path, input_file_name,
             output_path, output_file_name):
        pass

    @abstractmethod
    def move(self, input_path, input_file_name,
             output_path, output_file_name):
        pass

    @abstractmethod
    def delete_object(self, input_path, input_file_name):
        pass


class OedpFileInfo:
    def __init__(self):
        self.__container = ''
        self.__file_name = ''
        self.__key_prefix = ''

    @property
    def container(self):
        return self.__container

    @container.setter
    def container(self, container):
        self.__container = container

    @property
    def file_name(self):
        return self.__file_name

    @file_name.setter
    def file_name(self, file_name):
        self.__file_name = file_name

    @property
    def key_prefix(self):
        return self.__key_prefix

    @key_prefix.setter
    def key_prefix(self, key_prefix):
        self.__key_prefix = key_prefix
