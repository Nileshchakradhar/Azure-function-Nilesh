from BlobTrigger_B2BIngestion.OedpCommonUtil import *
import os



"""
*************************************************
OedpValidations.py
Contains Business validations
*************************************************
"""
supported_file_names = list(os.getenv('VALID_FILE_NAMES').split(','))
valid_customer_prefix = list(os.getenv('VALID_CUSTOMER_PREFIX').split(','))


def Issftp(customer_code):
    """
        Method : Issftp(self)
        Input  : customer_code
        Output : bool (returns True if customer code is of type sftp)
    """
    return str(customer_code.strip()[-1:]).isalpha() or str(customer_code.strip()[0:2]).upper() in valid_customer_prefix




def Issoap(customer_code):
    """
            Method : Issoap(self)
            Input  : customer_code
            Output : bool (returns True if customer code is of type soap)
    """
    return str(customer_code.strip()[-1:]).isdigit() and str(
        customer_code.strip()[0:2]).upper() not in valid_customer_prefix




def IsCustomerCodeValid(customer_code):
    """
            Method : IsCustomerCodeValid(self)
            Input  : customer_code
            Output : bool (true if customer code is valid)
    """
    CustomerCode = customer_code.strip()
    return CustomerCode != '' and (Issftp(CustomerCode) or Issoap(CustomerCode))




def IsHeaderInfo(line):
    """
            Method : IsHeaderInfo(self)
            Input  : line
            Output : bool(returns true only if line contains header information)
    """
    if len(line.strip()) == 0:
        return False
    return line[0] == '$'




def IsDeliveryOrInvoiceInfo(line):
    """
            Method : IsDeliveryAndInvoiceInfo(self)
            Input  : line
            Output : bool (returns true if line has delivery or invoice info)
    """
    if len(line.strip()) == 0: # The function checks if the length of the string after stripping any leading or trailing whitespace is zero.
        return False
    return line[0] == '1' # it checks if the first character of the line is equal to the character '1'




def IsPackagingInfo(line):
    """
            Method : IsPackagingInfo(self)
            Input  : line
            Output : bool(returns True if line )
    """
    if len(line.strip()) == 0:
        return False
    return line[0] == '2'




def IsFooterInfo(line):
    """
            Method : copy(self)
            Input  : input_path=str(''), input_file_name=str(''),
                    output_path=str(''), output_file_name=str('')
            Output : (copies files from a given input bucket/key to output bucket/key)
    """
    if len(line.strip()) == 0:
        return False
    return line[0] == '3'




def is_supported_file_name(supported_file_names: list, file_name: str):
    """
            Method : copy(self)
            Input  : input_path=str(''), input_file_name=str(''),
                    output_path=str(''), output_file_name=str('')
            Output : (copies files from a given input bucket/key to output bucket/key)
    """
    for supported_file in supported_file_names:
        # checks if the given file name starts with any of them (using the ".startswith()" method)
        if str(file_name).startswith(supported_file):
            return True
    return False



def IsValidFileName(input_file_name):
    """
            Method : copy(self)
            Input  : input_path=str(''), input_file_name=str(''),
                    output_path=str(''), output_file_name=str('')
            Output : (copies files from a given input bucket/key to output bucket/key)
    """
    return is_supported_file_name(supported_file_names, input_file_name)
