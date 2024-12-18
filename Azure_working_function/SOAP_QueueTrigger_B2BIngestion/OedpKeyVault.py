import os
import json
import ast
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from SOAP_QueueTrigger_B2BIngestion.OedpExceptions import *

class OedpSecretsManager:
    def __init__(self, Oedp_logger, secret_name, key_vault_url):
        self.__Oedp_logger = Oedp_logger
        self.__secret_name = secret_name
        self.__key_vault_url = key_vault_url
        self.__user_name = None
        self.__pass_code = None
        self.set_secret_values()
    
    @property
    def user_name(self):
        return self.__user_name

    @property
    def passcode(self):
        return self.__pass_code
    
    @property
    def Oedp_logger(self):
        return self.__Oedp_logger

    @property
    def secret_name(self):
        return self.__secret_name

    @property
    def key_vault_url(self):
        return self.__key_vault_url

    def get_secret(self):
        """_summary_: take the secrets from the secret client
        from the key vault by using the secret name.

        Raises:
            OedpCustomException: _description_

        Returns:
            _type_: _description_
        """
        try:
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=self.key_vault_url, credential=credential)
            secret = secret_client.get_secret(self.secret_name)
            return secret.value
        except Exception as e:
            raise OedpCustomException(f'Technical: Error getting secret value. {e}')

    def update_pass_code(self, pass_code):
        """_summary_: update the pass code in the secret client by passsing the new value of the password

        Args:
            pass_code (_type_): _description_

        Raises:
            OedpCustomException: _description_
        """
        try:
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=self.key_vault_url, credential=credential)
            secret_value = json.dumps({
                "username": self.user_name,
                "password": pass_code
            })
            secret_client.set_secret(self.secret_name, secret_value)
        except Exception as e:
            raise OedpCustomException(f'Technical: Error updating secret value. {e}')

    # def parse_assign_secret(self, secret):
    #     """_summary_: assigning the value to the parameters from the secrets

    #     Args:
    #         secret (_type_): _description_
    #     """
    #     self.__user_name = os.getenv('USERNAME') #secret['USERNAME']
    #     self.__pass_code = os.getenv('PASSCODE') #secret['PASSCODE']
    
    
    def parse_assign_secret(self, secret):
        print(f"secret : {secret}")
        self.__user_name = secret['username']
        self.__pass_code = secret['password']

    def set_secret_values(self):
        """_summary_: set the values of the parameters from the secrets
        """
        secret = self.get_secret()
        self.parse_assign_secret(secret)
