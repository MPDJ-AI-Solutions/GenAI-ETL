class AIClientMock:
    """Class used for debugging purposes."""

    def __init__(self, log: bool = False):
        self.log = log


    def define_structure(self, structure: str):
        self.structure = structure

        self.generated_code = structure


    def define_extract(self, extract_rules: str):
        self.extract_rules = extract_rules
        if self.structure is not None: 
            self.generated_code += "\n\n" + extract_rules
        else:
            raise Exception("Not specified database structure")


    def define_transform(self, transform_rules: str):
        self.transform_rules = transform_rules
        if self.extract_rules is not None:
            self.generated_code += "\n\n" + transform_rules
        else:
            raise Exception("Not specified extract step.")

    
    def define_load(self, load_rules: str):
        self.load_rules = load_rules
        if self.transform_rules is not None:
            self.generated_code += "\n\n" + load_rules
        else:
            raise Exception("Not specified transform step.")


    def save_results(self, file_path: str):
        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(self.generated_code)