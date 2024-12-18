from BlobTrigger_B2BIngestion.OedpValidations import *
from BlobTrigger_B2BIngestion.OedpCommonUtil import *



class OedpIdentify:
    """
    ********************************************************
    Class OedpIdentify to identify sftp soap
    Acts as a helper class
    ********************************************************
    """

    @staticmethod
    def GetHeaders(lines):
        """
        Method : GetHeaders
        Input: lines (list of lines from input file)
        Output : lines (filered on header contents)
        """
        return [line for line in lines if line[0:2] == '$$']


    @staticmethod
    def GetCustomerCode(header):
        """
            Method : GetCustomerCode
            Input: header (line containing header information)
            Output : custumer_code 
        """
        CustomerCode = header[19:26]
        return CustomerCode


    @staticmethod
    def split_records(Lines, Oedp_logger):
        """
            Method : SplitsftpsoapRecords
            Input: lines (lines from input file)
            Output : sftpList, soapList (list containing sftp only and soap only records.) 
        """
        sftpList = []
        soapList = []
        ErrList = []
        ProcessingsftpRec = False
        ProcessingsoapRec = False
        ProcessErrRecs = False
        line_num = 0
        for line in Lines:
            line_num += 1
            if len(line.strip()) == 0:
                continue
            if IsHeaderInfo(line):
                customer_code = OedpIdentify.GetCustomerCode(line)
                if not IsCustomerCodeValid(customer_code):
                    if not ProcessErrRecs:
                        Oedp_logger.warning(f'Functional: Invalid Customer code found at line {line_num}')
                        ProcessErrRecs = True
                        ProcessingsftpRec = False
                        ProcessingsoapRec = False
                    ErrList.append(line)
                elif Issftp(customer_code):
                    if not ProcessingsftpRec:
                        ProcessingsftpRec = True
                        ProcessingsoapRec = False
                        ProcessErrRecs = False
                    sftpList.append(line)
                else:
                    if not ProcessingsoapRec:
                        ProcessingsoapRec = True
                        ProcessingsftpRec = False
                        ProcessErrRecs = False
                    soapList.append(line)
            else:
                if ProcessingsftpRec:
                    sftpList.append(line)
                elif ProcessingsoapRec:
                    soapList.append(line)
                else:
                    ErrList.append(line)
        return sftpList, soapList, ErrList


    @staticmethod
    def CreatesftpFileName(input_fileName):
        """
            Method : CreatesftpFileName
            Input: input_fileName (lines from input file)
            Output : fileName (filename that has sftp specific extension) 
        """
        fileName = StripFileExtension(input_fileName)
        fileName += '.XQ0093'

        return fileName
