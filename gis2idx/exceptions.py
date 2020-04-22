
class DirectoryNotFoundError(FileNotFoundError):
    "Raised if a directory is not found"

class NoGISFilesFoundException(DirectoryNotFoundError):
    "Raised if state GIS files aren't found"

class NoCSVFilesFoundException(FileNotFoundError):
    "Raised if the state CSV aren't found"