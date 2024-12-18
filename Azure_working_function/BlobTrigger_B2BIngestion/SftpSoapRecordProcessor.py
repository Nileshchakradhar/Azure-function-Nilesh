from BlobTrigger_B2BIngestion.OedpCommonUtil import *
from BlobTrigger_B2BIngestion.OedpTableDefinitions import *
from BlobTrigger_B2BIngestion.OedpValidations import *
from BlobTrigger_B2BIngestion.OedpConstants import *
from BlobTrigger_B2BIngestion.OedpExceptions import *


class soapProcessor:
    def __init__(self, Oedp_logger):
        self.__Oedp_logger = Oedp_logger

    @property
    def Oedp_logger(self):
        return self.__Oedp_logger

    def process_soap(self, lines):
        '''
        it process the soap file and its internal process
        '''
        delivery_set = {}
        RIVF00List = []
        RICEC150List = []
        processing_delivery = False
        processing_invoice = False
        package_number_set = False

        def create_RIVF00_rec():
            ''''
            create the value of RIVF00_rec
            '''
            RIVF000_Content = self.CreateRIVF000_Content(delivery_set)
            self.Oedp_logger.debug(f'RIVF00 created: {RIVF000_Content}')
            RIVF00List.append(RIVF000_Content)
            nonlocal processing_invoice
            nonlocal package_number_set
            processing_invoice = False
            package_number_set = False
            delivery_set['DeliveryDocuments'].clear()

        def create_RICEC_rec():
            '''
            create the value of RICEC_rec
            '''
            RICEC150_Content = self.CreateRICEC150_Content(delivery_set)
            self.Oedp_logger.debug(f'RICEC150 created: {RICEC150_Content}')
            RICEC150List.append(RICEC150_Content)
            nonlocal processing_delivery
            nonlocal package_number_set
            processing_delivery = False
            package_number_set = False
            delivery_set['DeliveryDocuments'].clear()

        def create_rec_if_data_available():
            ''''
            it check for the condition which one is available from the above
            and then perform functionality
            '''
            if processing_delivery:
                create_RICEC_rec()
            if processing_invoice:
                create_RIVF00_rec()

        try:
            for line in lines:
                if IsHeaderInfo(line):
                    create_rec_if_data_available()
                    delivery_set = self.process_header(line, delivery_set)
                elif IsDeliveryOrInvoiceInfo(line):
                    create_rec_if_data_available()
                    self.process_type_1(line, delivery_set)
                    if get_flow_type(line) == INVOICE_AT_DELIVERY or get_flow_type(line) == CONSOLIDATED_INVOICE:
                        processing_invoice = True
                    elif get_flow_type(line) == DELIVERY:
                        processing_delivery = True

                elif IsPackagingInfo(line):
                    if not package_number_set:
                        self.process_type_2(line, delivery_set)
                        package_number_set = True
                elif IsFooterInfo(line):
                    create_rec_if_data_available()
                    self.process_type_3(line, delivery_set)
            # ensure to process last record
            create_rec_if_data_available()
            return RICEC150List, RIVF00List
            # return both lists
        except KeyError as key:
            raise OedpCustomException(f'Functional : process_soap: Key - {key} not found at line {line}.')

    def process_header(self, line, delivery_set):
        delivery_set['Recipient'] = get_customer_code(line)
        delivery_set['DeliveryDocuments'] = []
        return delivery_set

    def process_type_1(self, line, delivery_set):
        """_summary_

        Args:
            line (_type_): _description_
            delivery_set (_type_): _description_

        Returns:
            _type_: _description_
        """
        content_processed = False
        # if DeliveryDocument['Number'] already exists, append else create a new entry
        for DeliveryDocument in delivery_set['DeliveryDocuments']:
            if DeliveryDocument['Number'] == get_delivery_note(line).strip():
                dd = self.create_delivery_line(line)
                self.Oedp_logger.debug(f'Delivery line created: {dd}')
                DeliveryDocument['DeliveryLines'].append(dd)
                content_processed = True

        if not content_processed:
            DeliveryDocument = {}
            DeliveryDocument['Number'] = get_delivery_note(line).strip()
            dd = self.create_delivery_line(line)
            self.Oedp_logger.debug(f'Delivery line created: {dd}')
            DeliveryDocument['DeliveryLines'] = [dd]
            delivery_set['DeliveryDocuments'].append(DeliveryDocument)

        return delivery_set

    def process_type_2(self, line, delivery_set):
        """_summary_

        Args:
            line (_type_): _description_
            delivery_set (_type_): _description_

        Returns:
            _type_: _description_
        """
        content_processed = False

        for DeliveryDocument in delivery_set['DeliveryDocuments']:
            if DeliveryDocument['Number'].strip() == get_delivery_note(line).strip():
                for DeliveryLine in DeliveryDocument['DeliveryLines']:
                    self.Oedp_logger.debug(f"Processing record type 2 for {DeliveryLine['ShipToParty']}")
                    if DeliveryLine['OrderLineNumber'].strip() == get_invoice_line_number(line).strip():
                        DeliveryLine['PackageNumber'] = get_package_number(line)
                        content_processed = True
        if not content_processed:
            self.Oedp_logger.warning(f"Functional: Missing creation of delivery line for {delivery_set['Recipient']}")

        return delivery_set

    @staticmethod
    def process_type_3(line, delivery_set): 
        """_summary_

        Args:
            line (_type_): _description_
            delivery_set (_type_): _description_
        """
        delivery_set['FooterInvoiceNumber'] = get_footer_invoice_number(line)
        delivery_set['FooterNetAmount'] = get_footer_net_amount(line)
        delivery_set['FooterTaxAmount'] = get_footer_tax_amount(line)
        delivery_set['FooterTaxType'] = get_footer_tax_type(line)

    @staticmethod
    def get_default_delivery_line():
        """_summary_

        Returns:
            _type_: _description_
        """
        delivery_line = {'MarketCode': '',
                         'ShipToParty': '',
                         'InvoiceNumber': '',
                         'OrderLineNumber': '',
                         'DeliveryDate': '',
                         'OrderNumber': '',
                         'DmsDocNumber': '',
                         'OrderDate': '',
                         'OrderType': '',
                         'ShippedPartNumber': '',
                         'ShippedPartQuantity': '',
                         'OrderedPartNumber': '',
                         'OrderedPartQuantity': '',
                         'DocumentType': '',
                         'ListPrice': '',
                         'NetPrice': '',
                         'VAT_Amount': '',
                         'Currency': '',
                         'VAT_Rate': '',
                         'DiscountCode': '',
                         'DeliveryNoteNumber': '',
                         'DeliveryNoteNumberDate': '',
                         'PackageNumber': ''
                         }
        return delivery_line

    @classmethod
    def create_delivery_line(cls, line):
        """_summary_

        Args:
            line (_type_): _description_

        Returns:
            _type_: _description_
        """
        DeliveryLine = cls.get_default_delivery_line()

        if get_flow_type(line) == INVOICE_AT_DELIVERY:
            DeliveryLine['Type'] = 'Invoice at delivery'
        elif get_flow_type(line) == DELIVERY:
            DeliveryLine['Type'] = 'Delivery note'
        elif get_flow_type(line) == CONSOLIDATED_INVOICE:
            DeliveryLine['Type'] = 'Consolidated invoice'
        # sales organisation has 4 chars. we use 1st 2 for market code
        DeliveryLine['MarketCode'] = get_sales_organization(line)[0:2]
        DeliveryLine['ShipToParty'] = get_ship_to_party_code(line)  # original line[16:25]
        DeliveryLine['InvoiceNumber'] = get_invoice_number(line)
        DeliveryLine['OrderLineNumber'] = get_invoice_line_number(line)
        DeliveryLine['DeliveryDate'] = get_delivery_date(line)
        DeliveryLine['OrderNumber'] = get_network_order_number(line)
        DeliveryLine['DmsDocNumber'] = get_dms_doc_number(line)
        DeliveryLine['OrderDate'] = get_order_date(line)
        DeliveryLine['OrderType'] = get_order_type(line)
        DeliveryLine['ShippedPartNumber'] = get_shipped_material(line)
        DeliveryLine['ShippedPartQuantity'] = get_delivered_quantity(line)
        DeliveryLine['OrderedPartNumber'] = get_ordered_material(line)
        DeliveryLine['OrderedPartQuantity'] = get_ordered_quantity(line)
        DeliveryLine['DeliveryNoteNumber'] = get_delivery_note_number(line)
        DeliveryLine['DeliveryNoteNumberDate'] = get_delivery_note_number_date(line)
        DeliveryLine['ListPrice'] = get_unit_exvat_list_price(line)  # List price (ex-VAT) in centimes of currency
        DeliveryLine['NetPrice'] = get_net_exvat_amount(line)  # Net price (ex-VAT) in centimes of currency
        DeliveryLine['VAT_Amount'] = get_vat_amount(line)  # VAT amount in centimes of currency
        DeliveryLine['DiscountCode'] = get_discount_code(line)  # Discount code defined by up to two characters
        # Currency on three characters (EUR for Euro, CHF for Swiss Franc, GBP for British pound Sterling)
        DeliveryLine['Currency'] = get_currency(line)
        DeliveryLine['DocumentType'] = get_invoicing_type(line)
        DeliveryLine['VAT_Rate'] = get_vat_rate(line)  # VAT rate multiplied by 10000
        # DeliveryLine['InvoiceNumber'] = get_credit_note_invoice(line)
        return DeliveryLine

    def CreateRIVF000_Content(self, delivery_set):
        """_summary_

        Args:
            delivery_set (_type_): _description_

        Raises:
            OedpCustomException: _description_
            OedpCustomException: _description_

        Returns:
            _type_: _description_
        """
        self.Oedp_logger.debug('Begin creating RIVF000')
        try:
            Total_RIVF000_Content = ''
            for DeliveryDocument in delivery_set['DeliveryDocuments']:
                for Line in DeliveryDocument['DeliveryLines']:
                    # Field 1. Fixed value as SR35 ; 4 characters
                    RIVF000_Content = ''
                    RIVF000_Content += 'SR35'
                    self.Oedp_logger.debug(f'Processing delivery line {Line}')
                    # ----------------- Dealer information -------------------------------------------
                    # Field 2. Market code ; 4 characters
                    RIVF000_Content += pad_str(MarketCodeTable.get(Line['MarketCode']), 4, ' ')
                    # Field 3.Recipient as SINCOM code ; 7 characters
                    RIVF000_Content += delivery_set['Recipient']
                    # Field 4. Delivery address as in CSPS ; 4 characters ; empty as not relevant in DISTRIGO business
                    RIVF000_Content += '0001'
                    # Field 5. Dealer mandate as in CSPS ; 2 characters ; empty as not relevant in DISTRIGO business
                    RIVF000_Content += get_space(2)
                    # Field 6. DAC commercial category ; 2 characters ; empty as not relevant in DISTRIGO business
                    RIVF000_Content += get_space(2)

                    # ----------------- Order information ---------------------------------------------
                    # Field 7. EPLUS order type ; 4 characters
                    RIVF000_Content += OrderTypeTableRIVF.get(Line['OrderType'])
                    # Field 8. Order number ; 7 characters
                    RIVF000_Content += Line['OrderNumber'][:7]
                    # Field 9. Customer order number ; 13 characters ; empty in example files
                    # DMS doc number is of length 10. Hence, padding 3 spaces
                    RIVF000_Content += pad_str(Line['DmsDocNumber'], 13, ' ')
                    # Field 10. Commercial order date ; 10 characters
                    RIVF000_Content += convert_to_rivf00_date(Line['OrderDate'])
                    # Field 11. Logistic order date ; 10 characters
                    RIVF000_Content += convert_to_rivf00_date(Line['DeliveryDate'])
                    # Field 12. Ordered part number ; 13 characters ; removing spaces, zero padding and truncating
                    if len(Line['OrderedPartNumber'].strip()) > 13:
                        self.Oedp_logger.warning(
                            f'Functional : Order part number exceeds length 13. Skipping record... ')
                        continue
                    RIVF000_Content += pad_str(Line['OrderedPartNumber'], 13, '0')[0:13]
                    # Field 13. Order line number ; 5 characters ; removing spaces and zero padding
                    RIVF000_Content += pad_str(Line['OrderLineNumber'], 5, '0')
                    # Field 14. Vendor office code ; 2 characters
                    RIVF000_Content += get_space(2)
                    # Field 15. Vendor code ; 8 characters
                    RIVF000_Content += get_space(8)
                    # Field 16. Bin location ; 8 characters
                    RIVF000_Content += get_space(8)
                    # Field 17. Ordered quantity ; 7 characters
                    RIVF000_Content += pad_str(Line['OrderedPartQuantity'], 7, '0')

                    # ----------------- Shipping information ----------------------------------------------
                    # Field 18. Supplied part number ; 13 characters ; removing spaces and zero padding
                    RIVF000_Content += pad_str(Line['ShippedPartNumber'], 13, '0')[0:13]
                    # Field 19. Shipping document number ; 10 characters ; removing spaces and zero padding
                    RIVF000_Content += pad_str(Line['InvoiceNumber'], 10, '0')
                    # Field 20. Shipping document date ; 10 characters
                    RIVF000_Content += convert_to_rivf00_date(Line['DeliveryDate'])
                    # Field 21. Box number ; 10 characters ; Set as empty
                    RIVF000_Content += pad_str(Line['PackageNumber'], 10, ' ')[0:10]
                    # Field 22. Trip number ; 10 characters ; Set as empty
                    RIVF000_Content += get_space(10)
                    # Field 23. Source warehouse code ; 2 characters ; Set as empty
                    RIVF000_Content += get_space(2)

                    # ----------------- Invoice information --------------------------------------------
                    # Field 24. Document type ; 1 character
                    RIVF000_Content += DocumentTypeTable.get(Line['DocumentType'])
                    # Field 25	Document number	length 13		Delivery note number, zero-padded
                    RIVF000_Content += pad_str(Line['DeliveryNoteNumber'], 13, '0')[0:13]
                    # Field 26. Invoice date ; 10 characters
                    RIVF000_Content += convert_to_rivf00_date(Line['DeliveryDate'])
                    # Field 27. Invoice quantity ; 9 characters
                    RIVF000_Content += pad_str(Line['ShippedPartQuantity'], 9, '0')
                    # Field 28. Contract number used in CSPS ; 9 characters ; Fixed value: ‘000000000’
                    RIVF000_Content += get_char('0', 9)
                    # Field 29. Price list currency ; 3 characters
                    RIVF000_Content += CurrencyTable.get(Line['Currency'])

                    # Field 30. List price ; 15 characters ; zero padding from 10 to 15 characters
                    RIVF000_Content += pad_str(Line['ListPrice'], 15, '0')
                    # Field 31. Discount rate ; 5 characters ; zero padding
                    RIVF000_Content += self.calculate_RIVF0_discount(Line['NetPrice'],
                                                                     Line['ListPrice'],
                                                                     Line['ShippedPartQuantity'])
                    # Field 32. Extra discount percentage ; 5 characters ; zeroes as not used
                    RIVF000_Content += get_char('0', 5)
                    # Field 33	Document currency code		236	3		Fixed value: ‘EUR’
                    RIVF000_Content += CurrencyTable.get(Line['Currency'])
                    # Field 34	Document exchange rate		239	10		Fixed value: ‘0000000001’
                    RIVF000_Content += '0000000001'
                    # Field 35	Discount grid		249	2		Fixed value: ‘  ‘
                    RIVF000_Content += get_space(2)
                    # Field 36	Discount code applied		251	13		Fixed value: ‘             ‘
                    RIVF000_Content += get_space(13)
                    # Field 37	VAT Code		264	1		Fixed value: ‘ ‘
                    RIVF000_Content += get_space(1)
                    # Field 38	VAT Rate		265	5		VAT rate from corresponding xQ0093 line
                    RIVF000_Content += Line['VAT_Rate'][:5]
                    # Field 39	Surcharge		270	15		Fixed value: ‘000000000000000’
                    RIVF000_Content += get_char('0', 15)
                    # Field 40	Make code of ordered part		285	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 41	Macro family of ordered part		287	1		Fixed value: ‘ ’
                    RIVF000_Content += get_space(1)
                    # Field 42	Family of ordered part		288	4		Fixed value: ‘    ’
                    RIVF000_Content += get_space(4)
                    # Field 43	Description code of ordered part		292	4		Fixed value: ‘    ’
                    RIVF000_Content += get_space(4)
                    # Field 44	Claim number		296	9		Fixed value: ‘         ’
                    RIVF000_Content += get_space(9)
                    # Field 45	Filler		311	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 46	Presence of a carcass		305	1		Fixed value: ‘ ’
                    RIVF000_Content += get_space(1)
                    # Field 47	Empty filler		306	5		Fixed value: ‘     ’
                    RIVF000_Content += get_space(5)
                    # Field 48	Weight (grams) of ordered part		311	14		Fixed value: ‘              ’
                    RIVF000_Content += get_space(14)
                    # Field 49	Tariff code of ordered part		325	9		Fixed value: ‘         ’
                    RIVF000_Content += get_space(9)
                    # Field 50	Make code of supplied part		334	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 51	Macro family of supplied part		336	1		Fixed value: ‘ ’
                    RIVF000_Content += get_space(1)
                    # Field 52	Family of supplied part		337	4		Fixed value: ‘    ’
                    RIVF000_Content += get_space(4)
                    # Field 53	Description code of supplied part		341	4		Fixed value: ‘    ’
                    RIVF000_Content += get_space(4)
                    # Field 54	Price list 05		345	15		Fixed value: ‘               ’
                    RIVF000_Content += get_space(15)
                    # Field 55	Weight of supplied part		360	14		Fixed value: ‘              ’
                    RIVF000_Content += get_space(14)
                    # Field 56	Tariff code of supplied part		374	9		Fixed value: ‘         ’
                    RIVF000_Content += get_space(9)
                    # Field 57	Reman code		383	1		Fixed value: ‘ ’
                    RIVF000_Content += get_space(1)
                    # Field 58	Tax type 		384	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 59	Tax code 		386	15		Fixed value: ‘               ’
                    RIVF000_Content += get_space(15)
                    # Field 60	Data elaboration 		401	10		Fixed value: ‘          ’
                    RIVF000_Content += get_space(10)
                    # Field 61	Complete order number 		411	17		Fixed value: ‘                 ’
                    RIVF000_Content += get_space(17)
                    # Field 62	Document type 		428	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 63	Corporation code 		430	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 64	Society code		432	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 65	Progressive number 		434	5		Fixed value: ‘     ’
                    RIVF000_Content += get_space(5)
                    # Field 66	Billing company		439	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 67	Billing company code		441	2		Fixed value: ‘  ’
                    RIVF000_Content += get_space(2)
                    # Field 68	Claim number 		443	7		Fixed value: ‘       ’
                    RIVF000_Content += get_space(7)
                    # Field 69	Filler 		450	1		Fixed value: ‘ ’
                    RIVF000_Content += get_space(1)
                    # Field 70	Record separator		451	2		Fixed value: ‘\r\n’
                    RIVF000_Content += get_eol()  # End of line
                    Total_RIVF000_Content += RIVF000_Content

        except KeyError as ke:
            raise OedpCustomException(f'Functional: CreateRIVF000_Content: KeyError while processing {Line}. {ke}')
        except Exception as e:
            raise OedpCustomException(f'Functional: CreateRIVF000_Content: Runtime error {e}.')

        return Total_RIVF000_Content

    def CreateRICEC150_Content(self, delivery_set):
        """_summary_

        Args:
            delivery_set (_type_): _description_

        Raises:
            OedpCustomException: _description_
            OedpCustomException: _description_

        Returns:
            _type_: _description_
        """
        Total_RICEC150_Content = ''
        try:
            for DeliveryDocument in delivery_set['DeliveryDocuments']:
                for Line in DeliveryDocument['DeliveryLines']:
                    RICEC150_Content = ''
                    # Field 1. Fixed value as 63 ; 2 characters
                    RICEC150_Content += '63'
                    # Field 2. Marketing unit ; 4 characters
                    RICEC150_Content += MarketCodeTable.get(Line['MarketCode'])
                    # Field 3. ePlus Customer code / recipient as SINCOM code ; 7 characters
                    RICEC150_Content += delivery_set['Recipient']
                    # Field 4. Management code ; 7 blank characters
                    RICEC150_Content += Line['ShipToParty'][:7]
                    # Field 5. Society ; 2 characters ; To be discussed
                    RICEC150_Content += SocietyCodeTable.get(Line['MarketCode'])
                    # Field 6. Order number ; 7 characters
                    RICEC150_Content += Line['OrderNumber'][:7]
                    # Field 7. Order line number ; 5 characters ; removing spaces and zero padding
                    RICEC150_Content += pad_str(Line['OrderLineNumber'], 5, '0')
                    # Field 8. Split order line number ; 5 characters ; Set as one
                    RICEC150_Content += '00001'
                    # Field 9. Commercial order date ; 6 characters YYMMDD
                    RICEC150_Content += convert_to_ricec150_date(Line['OrderDate'])
                    # Field 10. Ordered part number ; 13 characters ; removing spaces, zero padding and truncating
                    if len(Line['OrderedPartNumber'].strip()) > 13:
                        self.Oedp_logger.warning(f'Functional: Order part number exceeds length 13. Skipping record... ')
                        RICEC150_Content = ''
                        continue
                    RICEC150_Content += pad_str(Line['OrderedPartNumber'], 13, '0')[0:13]
                    # Field 11. Delivered quantity ; 8 characters
                    RICEC150_Content += pad_str(Line['ShippedPartQuantity'], 8, '0')
                    # Field 12. List price ; 9 characters ; zero padding from 9 characters
                    RICEC150_Content += pad_str(Line['ListPrice'], 9, '0')
                    # Field 13. EPLUS order type ; 2 characters
                    RICEC150_Content += OrderTypeTableRICEC.get(Line['OrderType'])
                    # Field 14. Ordered quantity ; 8 characters
                    RICEC150_Content += pad_str(Line['OrderedPartQuantity'], 8, '0')
                    # Field 15. Discount rate ; 6 characters ; zero padding ; 2 integers and four decimals
                    RICEC150_Content += self.calculate_RICEC_discount(Line['NetPrice'],
                                                                      Line['ListPrice'],
                                                                      Line['ShippedPartQuantity'])
                    # Field 16. Shipping document number ; 10 characters ; removing spaces and zero padding
                    RICEC150_Content += pad_str(Line['InvoiceNumber'], 10, '0')
                    # Field 17. Shipping document number ; 10 characters ; keeping the 10 left characters out of 20
                    RICEC150_Content += pad_str(Line['PackageNumber'], 10, ' ')[0:10]
                    # Field 18. Replacing item ; Fixed value: '             ' 13
                    RICEC150_Content += get_space(13)
                    # Field 19. Summary quantity ; Fixed value: '00000000' 8 zeroes
                    # As per new specs difference between filed 14 and 11
                    RICEC150_Content += pad_str(str(self.calculate_difference(Line['OrderedPartQuantity'],
                                                                              Line['ShippedPartQuantity'])), 8, '0')
                    # Field 20. VAT rate ; 4 characters
                    RICEC150_Content += Line['VAT_Rate'][1:5]
                    # Field 21. Reason code ; 2 blank characters as currently
                    RICEC150_Content += get_space(2)
                    # Field 22. Summary code ; 1 blank characters as currently
                    RICEC150_Content += get_space(1)
                    # Field 23. Split order line status ; 1 blank characters as currently
                    RICEC150_Content += 'D'
                    # Field 24. Customer order number ; 13 characters ; empty in example files
                    RICEC150_Content += get_space(13)
                    # Field 25. Processed quantity ; 8 zeroes
                    RICEC150_Content += get_char('0', 8)
                    # Field 26. Shipping document date ; 6 characters YYMMDD
                    RICEC150_Content += convert_to_ricec150_date(Line['DeliveryDate'])
                    #  Field 27. ; 4 characters ; Fixed value '0001'
                    # Should be shipping address
                    RICEC150_Content += '0001'
                    #  Field 28. ; 4 characters ;
                    # Customer location
                    RICEC150_Content += get_space(8)
                    #  Field 29. ; 34 characters ;
                    # new spec says filler of 27
                    RICEC150_Content += get_space(27)
                    # End of line
                    RICEC150_Content += get_eol()
                    Total_RICEC150_Content += RICEC150_Content
        except KeyError as ke:
            raise OedpCustomException(f'Functional: CreateRICEC150_Content: KeyError while processing {Line}. {ke}')
        except Exception as e:
            raise OedpCustomException(f'Functional: CreateRICEC150_Content: Runtime error {e}.')
        return Total_RICEC150_Content

    @staticmethod
    def calculate_discount(net_price, list_price, shipped_qty):
        """
        :param net_price:
        :param list_price:
        :param shipped_qty:
        :return:
        """
        return "%05d" % round(
            round((1 - (float(net_price) /
                        (float(list_price) * int(shipped_qty)))
                   ) * 100, 4)
            * 10000
        )

    @classmethod
    def calculate_RIVF0_discount(cls, net_price, list_price, shipped_qty):
        """_summary_

        Args:
            net_price (_type_): _description_
            list_price (_type_): _description_
            shipped_qty (_type_): _description_

        Returns:
            _type_: _description_
        """
        f_discount = (1 - round((convert_to_float_with_2_decimals(net_price) /
                                 (convert_to_float_with_2_decimals(list_price) * int(shipped_qty))), 5)) * 100000
        s_discount = str(f_discount)[:5]
        return s_discount

    @classmethod
    def calculate_RICEC_discount(cls, net_price, list_price, shipped_qty):
        """_summary_

        Args:
            net_price (_type_): _description_
            list_price (_type_): _description_
            shipped_qty (_type_): _description_

        Returns:
            _type_: _description_
        """
        f_discount = (1 - round((convert_to_float_with_2_decimals(net_price) /
                                 (convert_to_float_with_2_decimals(list_price) * int(shipped_qty))), 6)) * 1000000
        s_discount = str(f_discount)[:6]
        return s_discount

    @classmethod
    def calculate_difference(cls, order_qty, delivered_qty):
        """_summary_

        Args:
            order_qty (_type_): _description_
            delivered_qty (_type_): _description_

        Returns:
            _type_: _description_
        """
        _order_qty = int(order_qty)
        _delivered_qty = int(delivered_qty)
        _difference = _order_qty - _delivered_qty
        return _difference

