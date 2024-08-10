from openai import OpenAI


class AIClient: 
    """Class wrapper for OpenAI API calls. Designed for ETL automating processes."""

    def __init__(self, log: bool = False):
        """Class initialization"""
        self.client = OpenAI()
        self.log = log


    def define_structure(self, structure: str):
        """Defines database structure. Used to initialize following prompts."""
        self.structure = structure
    
        new_prompt = """Create a Python script (.py) that creates tables in PostgreSQL using psycopg2 with 
        connection details (database name, user, password, address, and port) specified 
        in connection.cfg file. Tables structure is provided in the following SQL script:\n"""

        new_prompt += structure

        completion = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments."},
                {"role": "user", "content": new_prompt}
            ]
        )
          
        response = completion.choices[0].message.content
        if self.log:
            print(response)

        self.generated_code = response


    def define_extract(self, extract_rules: str):
        """Defines extract rules. Used to initialize following prompts."""
        self.extract_rules = extract_rules
        if self.structure is not None: 
            new_prompt = "Create a Python script (.py) that uses pandas to load data from:\n "
            new_prompt += extract_rules 
            new_prompt += """\nIf not specified use ',' as separator. Assume that data is stored in ./Data folder. 
            
            Consider already generated code:\n
            """ + self.generated_code

            completion = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments. Do not generate already generated code."},
                    {"role": "user", "content": new_prompt}
                ]
            )
            response = completion.choices[0].message.content
            if self.log:
                print(response)

            self.generated_code += "\n\n" + response
        else:
            raise Exception("Not specified database structure")


    def define_transform(self, transform_rules: str):
        """Defines transform rules. Used to initialize following prompts."""
        self.transform_rules = transform_rules
        if self.extract_rules is not None:
            new_prompt = "Create a Python script (.py) that uses pandas to follow provided rules to transform data. Rules:\n "
            new_prompt += transform_rules 
            new_prompt += "\nConsider already generated code:\n" + self.generated_code

            completion = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments. Do not generate already generated code."},
                    {"role": "user", "content": new_prompt}
                ]
            )
            response = completion.choices[0].message.content
            if self.log:
                print(response)

            self.generated_code += "\n\n" + response
        else:
            raise Exception("Not specified extract step.")

    
    def define_load(self, load_rules: str):
        """Defines load rules."""
        self.load_rules = load_rules
        if self.transform_rules is not None:
            new_prompt = "Create a Python script (.py) that uses pandas and psycopg2 to store dataframes values in the database defined in previous prompts (in already generated code). \n "
            new_prompt += load_rules 
            new_prompt += "\nConsider already generated code:\n" + self.generated_code

            completion = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments. Do not generate already generated code."},
                    {"role": "user", "content": new_prompt}
                ]
            )
            response = completion.choices[0].message.content
            if self.log:
                print(response)

            self.generated_code += "\n\n" + response
        else:
            raise Exception("Not specified transform step.")


    def save_results(self, file_path: str):
        """Saves generated code in specified file."""
        file = open(file_path, 'w')
        file.write(self.generated_code)
        file.close();


    def etl(self, db_structure: str, extract_rules: str, transform_rules: str, load_rules: str, file_path: str):
        """Wrapper for class methods."""
        self.define_structure(db_structure)
        self.define_extract(extract_rules)
        self.define_transform(transform_rules)
        self.define_load(load_rules)
        self.save_results(file_path)


    def debug(self, error: str):
        """Provides tool fo fixing code errors."""
        new_prompt = "Debug this error \n" + error + "\ncoming from this code: \n" + self.generated_code

        completion = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments. As output give entire code after fix."},
                    {"role": "user", "content": new_prompt}
                ]
            )
        response = completion.choices[0].message.content
        if self.log:
            print(response)

        self.generated_code = response
