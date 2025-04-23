import yaml

class SchemaEditor:
    def __init__(self, schema_path):
        self.schema_path = schema_path
        self.schema_data = None

    def read_schema(self):
        """Reads the schema file and stores it in the schema_data attribute."""
        try:
            with open(self.schema_path, 'r') as f:
                self.schema_data = yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: {self.schema_path} not found. Creating a new schema.")
            self.schema_data = {}

    def write_schema(self):
        """Writes the modified schema back to the file."""
        with open(self.schema_path, 'w') as f:
            yaml.dump(self.schema_data, f, default_flow_style=False)


