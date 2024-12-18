"""
*************************************************
OedpCommonUtility.py
File containing common utility functions. Including
ones having business related logic
*************************************************
"""



def GetFileExtension(file_name: str):
    """
        Method : GetFileExtension
        Input: file_name 
        Output : file_extension 
    """
    file_extension = file_name[file_name.find('.'):]
    return file_extension




def StripFileExtension(file_name):
    """
            Method : StripFileExtension
            Input: file_name 
            Output : fileName (file_name without extension) 
    """
    OldExt = GetFileExtension(file_name)
    file_name = file_name.removesuffix(OldExt)
    return file_name




def convert_list_to_str(listToConvert):
    """
            Method : ConvertListToStrLines
            Input: listToConvert (list)
            Output : strLines (str) 
    """
    strLines = ''
    for recs in listToConvert:
        strLines += recs
    return strLines


def convert_to_float_with_2_decimals(value):
    """_summary_

    Args:
        value (_type_): _description_

    Returns:
        _type_: result after calculating the functionality
    """    
    result = float(value) / 100
    return result


def pad_str(string, final_length, char):
    return string.strip().rjust(final_length, char)


def get_space(no_of_spaces):
    return ' ' * no_of_spaces


def get_char(char, length):
    return char * length


def get_eol():
    return '\r\n'


# lines starting with ## is type header, position 20-27 contains SINCOM also known as customer code
def get_customer_code(header):
    return header[19:26]


def get_filename_from_blob_name(key: str):
    file_name = key[key.rfind('/') + 1:]
    return file_name


def get_main_folder_from_blob_name(key: str):
    folder_name = key[0:key.find('/')]
    return folder_name


def get_flow_type(line):
    return line[330:331]


def get_delivery_note(line):
    return line[25:35]


def get_sales_organization(line):
    return line[1:5]


def get_sold_to_Party(line):
    return line[5:15]


def get_ship_to_party_code(line):
    return line[15:25]  # note as per type1 documentation it shoulde be 15:26


def get_invoice_number(line):
    return line[25:35]


def get_invoice_line_number(line):
    return line[35:40]


def get_delivery_date(line):
    return line[40:48]


def get_network_order_number(line):
    return line[56:66]


def get_dms_doc_number(line):
    return line[66:76]


def get_order_date(line):
    return line[76:84]


def get_order_type(line):
    return line[84:88]


def get_shipped_material(line):
    return line[88:106]


def get_delivered_quantity(line):
    return line[131:137]


def get_ordered_material(line):
    return line[137:155]


def get_ordered_quantity(line):
    return line[155:161]


def get_delivery_note_number(line):
    return line[181:196]


def get_delivery_note_number_date(line):
    return line[196:204]


def get_unit_exvat_list_price(line):
    return line[212:220]


def get_net_exvat_amount(line):
    return line[220:230]


def get_vat_amount(line):
    return line[230:240]


def get_currency(line):
    return line[242:245]


def get_invoicing_type(line):
    return line[306:307]


def get_credit_note_invoice(line):
    return line[320:330]


def get_vat_rate(line):
    return line[309:314]


def get_discount_code(line):
    return line[240:242]


def get_package_number(line):
    return line[84:104]


def get_footer_invoice_number(line):
    return line[1:11]


def get_footer_net_amount(line):
    return line[11:21]


def get_footer_tax_amount(line):
    return line[21:31]


def get_footer_tax_type(line):
    return line[31:35]


def convert_to_rivf00_date(xq0093_date):
    return xq0093_date[6:8] + '.' + xq0093_date[4:6] + '.' + xq0093_date[0:4]


def convert_to_ricec150_date(xq0093_date):
    return xq0093_date[2:4] + xq0093_date[4:6] + xq0093_date[6:8]
