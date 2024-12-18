import requests
from SOAP_QueueTrigger_B2BIngestion.OedpXmlFormatter import *
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *



class OedpSoapConnectionManager:
    def __init__(self, Oedp_logger):
        self.__Oedp_logger = Oedp_logger
        self.__xml_formatter = OedpXmlFormatter()

    @property
    def xml_formatter(self):
        return self.__xml_formatter

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger

    @staticmethod
    def get_header_type():
        return {'content-type': 'application/soap+xml; charset=utf-8'}

    def get_connection_xml(self, user_name, password):
        """
        it returns an XML string used to establish a connection with a remote system.
        """
        try:
            root = self.xml_formatter.get_soap_env()
            soap_body = self.xml_formatter.add_sub_element(root, 'soapenv:Body')
            urn = self.xml_formatter.add_sub_element(soap_body, 'urn:soapConnectPWD')
            self.xml_formatter.add_field(urn, 'urn:User', "xsd:string", user_name)
            self.xml_formatter.add_field(urn, 'urn:Password', "xsd:string", password)
            self.xml_formatter.add_field(urn, 'urn:CertificationSystem', "xsd:string", "UPRS")
            connection_xml = self.xml_formatter.xml_to_string(root)
            return connection_xml.decode('utf-8')
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating connection xml. {e}')
        
    def get_login_xml(self, ticket, service_id, login_market, login_brand, SINCOM):
        """
        it returns an login XML string used to establish a connection with a remote system.
        """
        try:
            root = self.xml_formatter.get_soap_env()
            soap_body = self.xml_formatter.add_sub_element(root, 'soapenv:Body')
            urn = self.xml_formatter.add_sub_element(soap_body, 'urn:soapLogin')
            self.xml_formatter.add_field(urn, 'Ticket', "xsd:string", ticket)
            self.xml_formatter.add_field(urn, 'ServiceId', "xsd:string", service_id)
            self.xml_formatter.add_field(urn, 'Market', "xsd:string", login_market)
            self.xml_formatter.add_field(urn, 'Application', "xsd:string", "SOAPWEB")
            self.xml_formatter.add_field(urn, 'Brand', "xsd:string", login_brand)
            self.xml_formatter.add_field(urn, 'Sincom', "xsd:string", SINCOM)
            self.xml_formatter.add_field(urn, 'CertificationSystem', "xsd:string", "UPRS")
            login_xml = self.xml_formatter.xml_to_string(root)
            return login_xml.decode('utf-8')
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating login xml. {e}')

    def get_post_xml(self, data, ticket, service_id, login_id, doc_id,
                     operation, chunk_length, file_format):
        """
        it returns an post XML string used to establish a connection with a remote system.
        """
        try:
            root = self.xml_formatter.get_soap_env()
            soap_body = self.xml_formatter.add_sub_element(root, 'soapenv:Body')
            urn = self.xml_formatter.add_sub_element(soap_body, 'urn:soapPost')
            self.xml_formatter.set_encoding_style(urn, "http://schemas.xmlsoap.org/soap/encoding/")
            self.xml_formatter.add_field(urn, 'Ticket', "xsd:string", ticket)
            self.xml_formatter.add_field(urn, 'ServiceId', "xsd:string", service_id)
            self.xml_formatter.add_field(urn, 'LoginId', "xsd:string", login_id)
            self.xml_formatter.add_field(urn, 'DocId', "xsd:string", doc_id)
            self.xml_formatter.add_field(urn, 'DocAppl', "xsd:string", "SOAPWEB")
            self.xml_formatter.add_field(urn, 'DocType', "xsd:string", file_format)
            self.xml_formatter.add_field(urn, 'Compress', "xsd:string", "")
            self.xml_formatter.add_field(urn, 'Encoding', "xsd:string", "asciihex")
            self.xml_formatter.add_field(urn, 'Operation', "xsd:string", operation)
            self.xml_formatter.add_field(urn, 'ChunkLength', "xsd:string", str(chunk_length))
            self.xml_formatter.add_field(urn, 'Buffer', "xsd:string", str(data))
            self.xml_formatter.add_field(urn, 'DestMarket', "xsd:string", "")
            self.xml_formatter.add_field(urn, 'DestApplic', "xsd:string", "")
            self.xml_formatter.add_field(urn, 'DestUser', "xsd:string", "")
            post_xml = self.xml_formatter.xml_to_string(root)
            return post_xml.decode('utf-8')
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating post xml. {e}')
        
    def get_commit_xml(self, ticket, service_id, soap_login_id, doc_id):
        """
        it returns an commit XML string by decoding it and used to establish a connection with a remote system.
        """
        try:
            root = self.xml_formatter.get_soap_env()
            soap_body = self.xml_formatter.add_sub_element(root, 'soapenv:Body')
            urn = self.xml_formatter.add_sub_element(soap_body, 'urn:soapCommit')
            self.xml_formatter.set_encoding_style(urn, "http://schemas.xmlsoap.org/soap/encoding/")
            self.xml_formatter.add_field(urn, 'Ticket', "xsd:string", ticket)
            self.xml_formatter.add_field(urn, 'ServiceId', "xsd:string", service_id)
            self.xml_formatter.add_field(urn, 'LoginId', "xsd:string", soap_login_id)
            self.xml_formatter.add_field(urn, 'DocId', "xsd:string", doc_id)
            commit_xml = self.xml_formatter.xml_to_string(root)
            return commit_xml.decode('utf-8')
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating commit xml. {e}')

    def get_logout_xml(self, ticket, service_id, soap_login_id):
        """
        it returns an logout XML string by decoding it and used to establish a connection with a remote system.
        """
        try:
            root = self.xml_formatter.get_soap_env()
            soap_body = self.xml_formatter.add_sub_element(root, 'soapenv:Body')
            urn = self.xml_formatter.add_sub_element(soap_body, 'urn:soapLogout')
            self.xml_formatter.set_encoding_style(urn, "http://schemas.xmlsoap.org/soap/encoding/")
            self.xml_formatter.add_field(urn, 'Ticket', "xsd:string", ticket)
            self.xml_formatter.add_field(urn, 'ServiceId', "xsd:string", service_id)
            self.xml_formatter.add_field(urn, 'LoginId', "xsd:string", soap_login_id)
            logout_xml = self.xml_formatter.xml_to_string(root)
            return logout_xml.decode('utf-8')
        except Exception as e:
            raise OedpCustomException(f'Functional: Error creating logout xml. {e}')

    def connect_pwd(self, connection_url, user_name, password):
        """ 
        it is used to connect to connect with the endpoint using the username and password
        """
        try:
            connection_xml = self.get_connection_xml(user_name, password)
            self.Oedp_logger.debug(f'connection xml: {connection_xml}')
            connection_header = self.get_header_type()
            response = requests.post(connection_url, data=connection_xml, headers=connection_header)
            # response = requests.post(" ")
            return response
        except Exception as e:
            raise soapTransferError(f'Technical: Error while posting ConnectPwd request. {e}')

    def soap_login(self, post_url, ticket, service_id, login_market, login_brand, SINCOM):
        """ 
        it is used to login to the soap endpoint
        """
        try:
            login_xml = self.get_login_xml(ticket, service_id, login_market, login_brand, SINCOM)
            self.Oedp_logger.debug(f'login xml: {login_xml}')
            login_header = self.get_header_type()
            login_response = requests.post(post_url, data=login_xml, headers=login_header)
            # login_response = requests.post(" ")
            return login_response
        except Exception as e:
            raise soapTransferError(f'Technical: Error while posting Login Request. {e}')

    def soap_post(self, post_url, data, ticket, service_id, login_id, doc_id, operation,
                 chunk_length, file_format):
        """ 
        it is used to post the data to the soap endpoint 
        """
        try:
            post_xml = self.get_post_xml(data, ticket, service_id, login_id, doc_id, operation,
                                        chunk_length, file_format, )
            self.Oedp_logger.debug(f'post xml {post_xml}')
            login_header = self.get_header_type()
            post_response = requests.post(post_url, data=post_xml, headers=login_header)
            # post_response =requests.post(" ")
            return post_response
        except Exception as e:
            raise soapTransferError(f'Technical : Error while posting Post Request. {e}')

    def soap_commit(self, commit_url, ticket, service_id, soap_login_id, doc_id):
        """ 
        it is used to commit the data to the soap endpoint 
        """
        try:
            commit_xml = self.get_commit_xml(ticket, service_id, soap_login_id, doc_id)
            self.Oedp_logger.debug(f'commit xml {commit_xml}')
            login_header = self.get_header_type()
            commit_response = requests.post(commit_url, data=commit_xml, headers=login_header)
            # commit_response = requests.post(" ")
            return commit_response
        except Exception as e:
            raise soapTransferError(f'Technical: Error while posting commit Request. {e}')

    def soap_logout(self, logout_url, ticket, service_id, soap_login_id):
        """ 
        it is used to logout from the soap endpoint 
        
        """
        try:
            commit_xml = self.get_logout_xml(ticket, service_id, soap_login_id)
            self.Oedp_logger.debug(f'logout xml {commit_xml}')
            login_header = self.get_header_type()
            commit_response = requests.post(logout_url, data=commit_xml, headers=login_header)
            # commit_response = requests.post(" ")
            return commit_response
        except Exception as e:
            raise soapTransferError(f'Technical: Error while posting logout Request. {e}')
