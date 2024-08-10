import anthropic


class ApiClient:
    client = anthropic.Anthropic()
    message_history = ""

    def define_database(self, structure: str):
        self.message_history = """Create a Python script (.py) that creates tables in PostgreSQL using psycopg2 with 
        connection details (database name, user, password, address, and port) specified in 
        connection.cfg file. Tables structure is provided in the following SQL script:\n
        """ + structure

        message = self.client.message.crate(
            model="claude-3-haiku-20240307",
            max_tokens=200,
            temperature=0,
            system="Generate code (in python) only.",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": self.message_history
                        }
                    ]
                }
            ]
        )

        message_history += message.content
        return message
    
    def define_files_to_load(self, files_desc: str):

        message = self.client.message.crate(
            model="claude-3-haiku-20240307",
            max_tokens=200,
            temperature=0,
            system="Generate code (in python) only.",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": 
                            """Create a Python script (.py) that uses pandas to load described files: 
                            """ + files_desc
                        }
                    ]
                }
            ]
        )

        return message
        
