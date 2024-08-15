import os

class Reader:
    """Class used solely to read information from files"""
    
    def is_file(self, path) -> bool:
        return os.path.isfile(path)

    def is_dir(self, path) -> bool:
        return os.path.isdir(path)
    
    def read(self, path) -> str:
        with open(path) as file:
            return file.read()
        
    def get_contents_of_dir(self, path) -> list|None:
        if not self.is_dir(path):
            return None

        contents = os.listdir(path)
        if len(contents) == 0:
            return None
        
        return contents