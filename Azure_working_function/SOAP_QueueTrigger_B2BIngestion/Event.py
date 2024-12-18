class Event:
    def __init__(self, reciept_handle, storage_queue_message, MessageId, Blobcontainer, InputFileName, OutputFileName, request_id):
        """_summary_
        event class to take the values of parameters
        """  
        self.reciept_handle = reciept_handle
        self.storage_queue_message = storage_queue_message
        self.MessageId = MessageId
        self.Blobcontainer = Blobcontainer
        self.InputFileName = InputFileName
        self.OutputFileName = OutputFileName
        self.request_id = request_id