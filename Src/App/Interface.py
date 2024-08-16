from APIClient import AIClient
from APIClientMock import AIClientMock
from FileReader import Reader
import platform
import os

class Interface:
    """Class used to handle communication between program and user"""
    
    def __init__(self, api_client, file_reader):
        self.api_client = api_client
        self.file_reader = file_reader

        if platform.system() == "Windows":
            self.clear_string = 'cls'
        elif platform.system() == "Linux":
            self.clear_string = "clear"
        else:
            raise Exception("Not supported platform")


    def wait_and_clear(self):
        input("\nPress Enter to continue...")
        self.clear()

    
    def clear(self):
        os.system(self.clear_string)


    def seperate(self, sign: str = "-", amount: int = 100):
        print(sign * amount)


    def ask_yes_no(self):
        while(True):
            response = input()
            if response.lower() == 'yes' or response.lower() == 'y':
                return True
            elif response.lower() == 'no' or response.lower() == 'n':
                return False
            else:
                print("Wrong value. Try again.")


    def print_all_rules(self, rules):
        for index, rule in enumerate(rules):
            print(f"{index + 1}. {rule}")
        self.seperate()


    def get_new_rule(self):
        print("Enter rule: (enter string 'quit' to finish inserting rules)")
        return input()
    

    def verify_rules(self):
        print("If listed rules are valid, type in 'yes'. Otherwise type in 'no':")
        return self.ask_yes_no()
    

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

    def get_rules_for_step(self, step_number: int, step_name: str):
        while(True):
            rules = []
            while(True):
                print(f"Step {step_number} - define {step_name} rules.")
                self.seperate()
                
                if len(rules) != 0:
                    self.print_all_rules(rules)
                    
                new_rule = self.get_new_rule()
                self.clear()
                if new_rule.lower() == 'quit' or new_rule.lower() == 'q' :
                    break
                rules.append(new_rule)
            
            if len(rules) == 0:
                print("No rules were specified. Is it correct? ('yes'/'no')")
                response = self.ask_yes_no()
                if response:
                    return ""
                self.clear()
            else:
                print(f"Step {step_number} - define {step_name} rules.")
                self.seperate()
                self.print_all_rules(rules)
                if not self.verify_rules():
                    print("Added rules were set as invalid.")
                    print("The list will now be cleared and you will have to fill it again.")
                    self.wait_and_clear()
                else:
                    break
        
        print(f"Adding rules for {step_name} process is finished. Data will now be send to AI model.")
        return '\n'.join(rules)


    def define_structure(self):
        print("Step 0 - define structure of the database.")
        self.seperate()
        print("Type in path to sql script file that contains the structure of database:")
        while(True):
            path = input()
            if self.file_reader.is_file(path):
                break
            else:
                print("Given path is not correct. Try again.")

        sql_script = self.file_reader.read(path)

        print("File was read successfully. Data will now be send to AI model.")

        self.api_client.define_structure(sql_script)

        self.wait_and_clear()


    def define_extract(self):
        print("Step 1 - define extract rules.")
        self.seperate()
        print("Type in path to directory that contains files with data:")
        while(True):
            path = input()
            if not self.file_reader.is_dir(path):
                print("Given path is not correct. Try again.")
                continue

            data_files_list = self.file_reader.get_contents_of_dir(path)
            if data_files_list is not None:
                break
            else:
                print("There are no items on given path. Try again.")
   
        self.clear()

        data_files_string = '\n'.join(data_files_list)
        extract_rules_string = self.get_rules_for_step(1, "extract")

        extract_rules = f"{data_files_string}\n{extract_rules_string}"
        self.api_client.define_extract(extract_rules)

        self.wait_and_clear()


    def define_transform(self):
        transform_rules_string = self.get_rules_for_step(2, "transform")
        self.api_client.define_transform(transform_rules_string)
        self.wait_and_clear()
    

    def define_load(self):
        load_rules_string = self.get_rules_for_step(3, "load")
        self.api_client.define_load(load_rules_string)
        self.wait_and_clear()


    def saving_information(self):
        print("Type in full path to directory in which the output file will be saved.")
        print("Type in 'default' to save output file in default location (main directiory of project).")
        
        while(True):
            self.path = input()
            if self.file_reader.is_dir(self.path) or self.path == 'default':
                break
            else:
                print("Given path is not correct. Try again.")
        
        if self.path == 'default':
            self.api_client.save_results("output.py")
        else:
            self.api_client.save_results(self.path + "/output.py")

        print("The file was successfully created!")
        self.wait_and_clear()
        

    def debug_information(self):
        print("Do you want to debug your code? ('yes'/'no')")
        response = self.ask_yes_no()
        if response:
            self.seperate()
            print("Check your created file now. If you have any problems running it, paste error into command line and hit 'ENTER'.")
            print("The program will try to debug your code using openAI API.")
            
            error_string = input()
            self.api_client.debug(error_string)
            if self.path == 'default':
                self.api_client.save_results("output_debug.py")
            else:
                self.api_client.save_results(self.path + "/output_debug.py")
            
            print("\nGreat! Now your debugged file is saved on your computer. You can find it in the same location the 'output.py' file is")
            self.wait_and_clear()


    def restart_prompt(self):
        print("Do you want to prepare another ETL process? ('yes'/'no')")
        response = self.ask_yes_no()
        self.clear()
        return response


    def start(self):
        restart = True
        while(restart):
            self.instruction()
            self.define_structure()
            self.define_extract()
            self.define_transform()
            self.define_load()
            self.saving_information()
            self.debug_information()
            restart = self.restart_prompt()


    def end(self):
        print("Thank you for using our application!")
        print("The project was made by:")
        print("- Michal Pilch (mp300455@student.polsl.pl)")
        print("- Dawid Jeziorski (dj300758@student.polsl.pl)")
        
        print("\nYou can find source code on our GitHub repository:")
        print("https://github.com/MPDJ-AI-Solutions/GenAI-ETL")
        self.wait_and_clear()


    def run(self):
        self.start()
        self.end()


def main():
    """Function used mainly for testing/developing purposes"""
    api_client = AIClientMock()
    file_reader = Reader()
    interface = Interface(api_client, file_reader)
    interface.run()

if __name__ == "__main__":
    main()