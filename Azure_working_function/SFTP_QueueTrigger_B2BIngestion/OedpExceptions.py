"""
*************************************************
OedpExceptions.py
Custom exceptions
*************************************************
"""


class ErrorB4LoggerInstance(Exception):
    def __init__(self, message):
        self.message = message


class SftpTransferError(Exception):
    def __init__(self, message):
        self.message = message


class OedpCustomException(Exception):
    def __init__(self, message):
        self.message = message
