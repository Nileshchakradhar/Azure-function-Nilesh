import logging
import os

class OedpLogger:
    """
    *************************************************
    OedpLoogger.py
    provides standard formatting for logging
    *************************************************
    """
    def __init__(self, message_id, event):
        self.__message_id = message_id
        self.__logger = logging.getLogger(f'Oedp_{event.request_id}')
        self.__logger.propagate = False
        for handler in self.__logger.handlers:
            handler.flush()
            handler.close()
        stream_handle = logging.StreamHandler()
        self.__logger.addHandler(stream_handle)
        deployed = os.getenv('DEPLOYED', '0')
        if deployed == '1': 
            self.__logger.setLevel(logging.INFO)
        else:
            self.__logger.setLevel(logging.DEBUG)
        text = f'[Oedp_LOG]'
        start_index = message_id.find('_')
        # the file name will be found after the 2nd instance of '_'
        file_name = message_id[message_id.find('_', start_index + 1) + 1:]

        log_msg = "{" + f'"FileName": {file_name}, "MessageId": {message_id}, "Message": '
        # log_msg = "{" + f'"RequestID": {request_id}, "MessageId": {message_id}, "Message": '
        formatter = logging.Formatter(f'%(levelname)s: {text} @%(asctime)s - {log_msg} %(message)s' + "}")
        # add formatter to ch
        stream_handle.setFormatter(formatter)

    @property
    def logger(self):
        return self.__logger
