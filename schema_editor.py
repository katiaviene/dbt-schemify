import yaml
from collections import defaultdict

class CustomDumper(yaml.Dumper):
    def increase_indent(self, flow = False, indentless = False):
        return super().increase_indent(flow, False)



class SchemaEditor:
    def __init__(self, schema_path):
        self.schema_path = schema_path
        self.schema_data = None

    def read_schema(self):
        """Reads the schema file and stores it in the schema_data attribute."""
        try:
            with open(self.schema_path, 'r') as f:
                self.schema_data = yaml.safe_load(f)
                print(self.schema_data)
        except FileNotFoundError:
            print(f"Warning: {self.schema_path} not found. Creating a new schema.")
            self.schema_data = {}

    def flatten_schema(self, path=None, context=None):
        """Flattens the schema data into a list of dicts with the path, context, and value."""
        if path is None:
            path = []
        if context is None:
            context = {}

        flattened = []
        data = self.schema_data

        if isinstance(data, dict):
            # Capture "name" if found
            if "name" in data and not path:
                # Top-level model name
                context = {**context, "model_name": data["name"]}
            elif "name" in data and "columns" in path:
                # Inside columns
                context = {**context, "column_name": data["name"]}

            for key, value in data.items():
                new_path = path + [key]
                # Call flatten_schema recursively
                flattened.extend(self._flatten_subtree(value, new_path, context))

        elif isinstance(data, list):
            for item in data:
                # Process each item in the list
                flattened.extend(self._flatten_subtree(item, path, context))

        return flattened

    def _flatten_subtree(self, data, path, context):
        """Helper method for flattening each subtree recursively."""
        flattened = []

        if isinstance(data, dict):
            # Add model/column information to context
            if "name" in data:
                if "columns" in path:
                    context = {**context, "column_name": data["name"]}
                else:
                    context = {**context, "model_name": data["name"]}

            for key, value in data.items():
                new_path = path + [key]
                flattened.extend(self._flatten_subtree(value, new_path, context))

        elif isinstance(data, list):
            for item in data:
                flattened.extend(self._flatten_subtree(item, path, context))

        else:
            # It's a leaf node (value in the schema)
            flattened.append({
                "path": path,
                "context": context,
                "value": data
            })
        print(flattened)
        return flattened

    def gather_to_dict(self, flattened):
        """Gathers the flattened list of dicts back into a nested dictionary."""
        def set_nested_item(d, keys, value):
            # Iterate through all but the last key to create nested dictionaries
            for key in keys[:-1]:
                if isinstance(d, list):
                    # If we are dealing with a list, we need to add a new item if it doesn't exist
                    # First key is 'models', so handle it
                    if isinstance(d[0], dict):
                        d = d[0]  # point to the first dictionary in models, like 'orders' or 'customers'
                    else:
                        d.append({})  # append an empty dict
                        d = d[-1]
                elif key not in d:
                    d[key] = {}  # Create a new dictionary if the key doesn't exist
                d = d[key]  # Move deeper into the nested structure
            
            # Set the final key's value (handling lists)
            last_key = keys[-1]
            if last_key in d:
                if isinstance(d[last_key], list):
                    d[last_key].append(value)
                else:
                    d[last_key] = [d[last_key], value]
            else:
                d[last_key] = value

        result = {'version': 2, 'models': []}
        models_map = {}  # to handle models by name (avoiding duplication)

        for item in flattened:
            path = item['path']
            value = item['value']

            # First item in the path is always 'models', we'll check and create models if needed
            model_name = path[1]  # 'orders' or 'customers'
            if model_name not in models_map:
                models_map[model_name] = len(result['models'])  # Remember the index of this model
                result['models'].append({
                    'name': model_name,
                    'description': '',
                    'config': {'enabled': False, 'materialized': '', 'tags': []},
                    'columns': []
                })

            # Now we can build the rest of the structure under this model
            model_index = models_map[model_name]
            set_nested_item(result['models'][model_index], path, value)

        return result

    def write_schema(self):
        """Writes the modified schema back to the file using custom dumper."""
        with open(self.schema_path, 'w+', encoding='utf-8') as f:
            yaml.dump(self.schema_data, f, default_flow_style=False, Dumper=CustomDumper, sort_keys=False)
