"""
*************************************************
OedpExceptions.py
Custom exceptions
*************************************************
"""


class FileNotSupportedException(Exception):
    def __init__(self, message):
        self.message = message

class EmptyOrInvalidRecords(Exception):
    def __init__(self, message):
        self.message = message

class OedpCustomException(Exception):
    def __init__(self, message):
        self.message = message