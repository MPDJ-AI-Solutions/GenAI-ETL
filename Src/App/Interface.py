from APIClient import AIClient
import os

class Interface:
    """Class used to handle communication between program and user"""
    
    def wait_and_clear(self):
        input("\nPress Enter to continue...")
        self.clear()
    
    def clear(self):
        os.system('cls')

    def seperate(self, sign: str = "-", amount: int = 100):
        print(sign * amount)

    def instruction(self):
        self.clear()
        print("Hello user.")
        print("This application was made for a purpose of a university subject - Data Warehouses")
        self.seperate()
        print("Remember! To use OpenAI API you have to define OPENAI_API_KEY environment variable in your system.")
        print("Check documentation for more information.")
        self.seperate()
        print("You will be guided step by step through the application.")
        print("After all steps of ETL process, the output script will be located in main directory of the project, in file output.py")
        self.wait_and_clear()

    def define_structure(self):
        print("Step 0 - define structure of the database.")
        self.seperate()
        self.wait_and_clear()

    def define_extract(self):
        print("Step 1 - define extract rules.")
        self.seperate()
        self.wait_and_clear()

    def define_transform(self):
        print("Step 2 - define transform rules.")
        self.seperate()
        self.wait_and_clear()
    
    def define_load(self):
        print("Step 3 - define load rules.")
        self.seperate()
        self.wait_and_clear()

    def saving_information(self):
        print("The file was successfully created at location XXXXXX")
        print("To try again restart application")
        self.wait_and_clear()

    def start(self):
        self.instruction()
        self.define_structure()
        self.define_extract()
        self.define_transform()
        self.define_load()
        self.saving_information()


def app():
    interface = Interface()
    interface.start()

if __name__ == "__main__":
    app()