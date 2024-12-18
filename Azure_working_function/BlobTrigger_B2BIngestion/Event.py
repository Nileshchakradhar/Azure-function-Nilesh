class Event:
    """_summary_
    event class to take the values of parameters
    """    
    def __init__(self, container, folder_name, blob_name,  etag, request_id):
        self.container = container # the name of the Azure blob storage container where the file is stored
        self.folder_name = folder_name
        self.blob_name = blob_name # the name of the file in the Azure blob storage container
        self.etag = etag # a unique identifier for the file that changes when the file is modified
        self.request_id = request_id #request_id # the request ID of the event etag):
        

