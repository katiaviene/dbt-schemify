import yaml
import time
from dbt_ast import Node
import json


class CustomDumper(yaml.Dumper):
    def increase_indent(self, flow = False, indentless = False):
        return super().increase_indent(flow, False)
    def ignore_aliases(self, data):
        return True

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
        return self.schema_data

    def read_manifest(self, manifest_path):
        try: 
            with open(manifest_path, 'r') as f:
                self.schema_data = json.load(f)
        except FileNotFoundError:
            print(f"Warning: manifest not found")
            self.schema_data = {}
        return self.schema_data





    def build_node(self, cls, data):
        if not isinstance(data, dict):
            return data

        fields = {}
        for field in getattr(cls, "_fields", []):
            value = data.get(field)

            if isinstance(value, list):
                # Dispatch to correct Node class if specified
                item_cls = getattr(cls, "_field_types", {}).get(field)
                if item_cls:
                    value = [self.build_node(item_cls, v) for v in value]
            elif isinstance(value, dict):
                item_cls = getattr(cls, "_field_types", {}).get(field)
                if item_cls:
                    value = self.build_node(item_cls, value)

            fields[field] = value

        return cls(**fields)
    
    def node_to_dict(self, node):
        """Convert a Node object to a dictionary."""
        node_dict = {}
        for field in node._fields:
            value = getattr(node, field)
            if value:
                if isinstance(value, list):
                    node_dict[field] = [self.node_to_dict(item) if isinstance(item, Node) else item for item in value]
                elif isinstance(value, Node):
                    node_dict[field] = self.node_to_dict(value)
                else:
                    node_dict[field] = None if value == 'schemify' else value
        return node_dict

    def write_schema(self):
        """Writes the modified schema back to the file using custom dumper."""
        with open(self.schema_path, 'w+', encoding='utf-8') as f:
            yaml.dump(self.schema_data, f, default_flow_style=False, Dumper=CustomDumper, sort_keys=False,  allow_unicode=True, default_style=None)
