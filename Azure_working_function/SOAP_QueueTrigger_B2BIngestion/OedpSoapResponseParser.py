import xmltodict
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *

RES_DICT = {
    'SOAP-ENV:Envelope': {
        '@xmlns:SOAP-ENV': 'http://schemas.xmlsoap.org/soap/envelope/',
        '@xmlns:ns1': 'urn:ddu',
        '@xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
        '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        '@xmlns:SOAP-ENC': 'http://schemas.xmlsoap.org/soap/encoding/',
        '@SOAP-ENV:encodingStyle': 'http://schemas.xmlsoap.org/soap/encoding/',
        'SOAP-ENV:Body': {
            'ns1:soapConnectPWDResponse': {
                'Ticket': {
                    '@xsi:type': 'xsd:string',
                    '#text': 'ME%5D.%3C_KZ4KJ4F%28%3AJ%2A9YJ%26%3B%3F%28A%3BC5R%6010%28J%5EMK%5B%288VX%5BO%2F%3F%27R%5B%60%28MV%60%5BD-%2B%26_ZM%5D%2B%26K%26U%40B%3F%3BWMSCUYOEO%3A%5E%23%254R%5B%3B%22%2C%21%3D_%26CBMS%26M%5C7MU%5BS6DYB3%3AAH%40%3C2%2CJ4YMZBGK_QVZ%3AZU%26%29%23-%5DY%21C%2AS8%404%5C%3B3%3DKWDRXH6I%2A--%407ZM%3F%2BBQ%5C%25C%24-D8Q3E33A%21F%60%60%60%60%0A'
                },
                'ServiceId': {
                    '@xsi:type': 'xsd:string',
                    '#text': '34761'
                },
                'CurrentPassword': {
                    '@xsi:type': 'xsd:string',
                    '#text': 'Distrigo2022*'
                },
                'Return': {
                    '@xsi:type': 'xsd:string',
                    '#text': '0'
                }
            }
        }
    }
}


class OedpSoapResponseParser:
    def __init__(self, Oedp_logger):
        self.__Oedp_logger = Oedp_logger
        self.__pass_code = None
        self.__user_name = None
        self.__session_ticket = None
        self.__session_service_id = None
        self.__connection_return_value = None
        self.__login_return_value = None
        self.__post_return_value = None
        self.__commit_return_value = None
        self.__logout_return_value = None
        self.__soap_login_id = None
        self.__doc_id = None
        self.__secret_return_value = None

    @property
    def secret_return_value(self):
        return self.__secret_return_value

    @secret_return_value.setter
    def secret_return_value(self, value):
        self.__secret_return_value = value

    @property
    def doc_id(self):
        return self.__doc_id

    @doc_id.setter
    def doc_id(self, value):
        self.__doc_id = value

    @property
    def soap_login_id(self):
        return self.__soap_login_id

    @soap_login_id.setter
    def soap_login_id(self, value):
        self.__soap_login_id = value

    @property
    def connection_return_value(self):
        return self.__connection_return_value

    @connection_return_value.setter
    def connection_return_value(self, value):
        self.__connection_return_value = value

    @property
    def login_return_value(self):
        return self.__login_return_value

    @login_return_value.setter
    def login_return_value(self, value):
        self.__login_return_value = value

    @property
    def post_return_value(self):
        return self.__post_return_value

    @post_return_value.setter
    def post_return_value(self, value):
        self.__post_return_value = value

    @property
    def commit_return_value(self):
        return self.__commit_return_value

    @commit_return_value.setter
    def commit_return_value(self, value):
        self.__commit_return_value = value

    @property
    def logout_return_value(self):
        return self.__logout_return_value

    @logout_return_value.setter
    def logout_return_value(self, value):
        self.__logout_return_value = value


    @property
    def user_name(self):
        return self.__user_name
    
    @user_name.setter
    def user_name(self, value):
        self.__user_name = value
        
    @property
    def pass_code(self):
        return self.__pass_code

    @pass_code.setter
    def pass_code(self, value):
        self.__pass_code = value

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger

    @property
    def session_ticket(self):
        return self.__session_ticket

    @session_ticket.setter
    def session_ticket(self, value):
        self.__session_ticket = value

    @property
    def session_service_id(self):
        return self.__session_service_id

    @session_service_id.setter
    def session_service_id(self, value):
        self.__session_service_id = value

    @staticmethod
    def xml_to_dict(xml_string):
        dict_obj = xmltodict.parse(xml_string)
        return dict_obj

    def parse_connect_pwd_response(self, response, Oedp_logger):
        try:
            response_dict = self.xml_to_dict(response)
            Oedp_logger.info(f'response of response_dict: {response_dict}')
            soap_response = response_dict['soap11env:Envelope']['soap11env:Body']['tns:soapConnectPWDResponse']['tns:soapConnectPWDResult']
            Oedp_logger.info(f'soap_response: {soap_response}')
            soap_response1 = soap_response.split(',')
            soap_response2 = soap_response1[0].split(':')
            
            self.pass_code = soap_response2[1]
            self.connection_return_value = 0  #int(soap_response['Return']['#text'])
        except Exception as e:
            raise OedpCustomException(f'Functional: Error parsing soapConnectPWDResponse. {e}')

    def parse_login_response(self, response):
        """ 
        it gives the responce of the xml after parsing the login id of the user.
        
        """
        try:
            response_dict = self.xml_to_dict(response)
            self.soap_login_id = '001' #soap_response['LoginId']['#text']
        except Exception as e:
            raise OedpCustomException(f'Functional: Error parsing soapLoginResponse. {e}')

    def parse_post_response(self, response):
        """ 
        it gives the post response of the xml by parsing it.
        """
        try:
            response_dict = self.xml_to_dict(response)
            soap_response = response_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:soapPostResponse']
            self.post_return_value = int(soap_response['Return']['#text'])
            self.doc_id = soap_response['DocId']['#text']
        except Exception as e:
            raise OedpCustomException(f'Functional: Error parsing soapPostResponse. {e}')

    def parse_commit_response(self, response):
        """ 
        it gives the commit response of the xml by parsing it
        """
        try:
            response_dict = self.xml_to_dict(response)
            soap_response = response_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:soapCommitResponse']
            self.commit_return_value = int(soap_response['Return']['#text'])
        except Exception as e:
            raise OedpCustomException(f'Functional: Error parsing soapCommitResponse. {e}')

    def parse_logout_response(self, response):
        """_summary_: it gives the logout response of the xml by parsing it.
        Args:
            response (_type_): _description_
        Raises:
            OedpCustomException: _description_
        """
        try:
            response_dict = self.xml_to_dict(response)
            soap_response = response_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:soapLogoutResponse']
            self.logout_return_value = int(soap_response['Return']['#text'])
        except Exception as e:
            raise OedpCustomException(f'Functional: Error parsing soapLogoutResponse. {e}')

    def parse_secret_response(self, response):
        self.secret_return_value = response['ResponseMetadata']['HTTPStatusCode']
