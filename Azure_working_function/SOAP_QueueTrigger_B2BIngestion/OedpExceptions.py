class ErrorB4LoggerInstance(Exception):
    def __init__(self, message):
        self.message = message


class soapTransferError(Exception):
    def __init__(self, message):
        self.message = message


class OedpCustomException(Exception):
    def __init__(self, message):
        self.message = message


class SecretUpdateError(Exception):
    def __init__(self, message):
        self.message = message
