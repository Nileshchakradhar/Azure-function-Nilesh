from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement

class OedpXmlFormatter:
    def __init__(self):
        self.__root = None
    @property
    def root_element(self):
        return self.__root

    @root_element.setter
    def root_element(self, value):
        self.__root = value

    def get_soap_env(self):
        # to create a SOAP envelope element that will be used in sending SOAP requests.
        self.root_element = Element('soapenv:Envelope')
        # they specify the XML namespace and the schema locations for the SOAP envelope.
        self.root_element.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        self.root_element.set("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        self.root_element.set("xmlns:soapenv", "http://schemas.xmlsoap.org/soap/envelope/")
        self.root_element.set("xmlns:urn", "http://0.0.0.0:5000")
        return self.root_element

    # root : XML element
    def add_sub_element(self, root, sub_element):
        element = SubElement(root, sub_element)
        return element

# you should subi.set("xy", "3") it should make subi tag as <subi xy="3"></subi>
    def set_type(self, element, type):
        element.set('xsi:type', type)

    def add_field(self, element, field_name, field_type, field_value):
        field = SubElement(element, field_name)
        field.text = field_value
        # It sets the xsi:type attribute of the field element to the value of the field_type parameter.
        field.set('xsi:type', field_type)
        return field

    @staticmethod
    def xml_to_string(element):
        return ElementTree.tostring(element)
