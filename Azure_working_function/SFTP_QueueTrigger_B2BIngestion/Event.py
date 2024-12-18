class Event:
    def __init__(self, pop_receipt, message_body, Blobcontainer, InputFileName, OutputFileName, MessageId, request_id):
        """_summary_
        event class to take the values of parameters
        """  
        self.pop_receipt = pop_receipt
        self.message_body = message_body
        self.Blobcontainer = Blobcontainer
        self.InputFileName = InputFileName
        self.OutputFileName = OutputFileName
        self.MessageId = MessageId
        self.request_id = request_id