from APIClientMock  import AIClientMock
from APIClient      import AIClient 
from Interface      import Interface
from FileReader     import Reader
import sys

def main():
    """Function used mainly for testing/developing purposes"""
    if 'debug' in sys.argv:
        api_client = AIClientMock()
    else:
        api_client =  AIClient()
    file_reader = Reader()
    interface = Interface(api_client, file_reader)
    interface.run()

if __name__ == "__main__":
    main()