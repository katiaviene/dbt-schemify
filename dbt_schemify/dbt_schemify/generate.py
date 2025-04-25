from dbt_schemify.dbt_schemify.dbt_ast import DefaultColumnNode, DefaultModelNode, DefaultSchemaNode, Node
from dbt_schemify.dbt_schemify.schema_editor import CustomDumper
import yaml

def generate_default_schema():
    column1 = DefaultColumnNode()
    model = DefaultModelNode( columns=[column1])
    schema = DefaultSchemaNode(version="1.0", models=[model])

    return schema

def node_to_dict(node):
        """Convert a Node object to a dictionary."""
        node_dict = {}
        for field in node._fields:
            value = getattr(node, field)
            if value:
                if isinstance(value, list):
                    node_dict[field] = [node_to_dict(item) if isinstance(item, Node) else item for item in value]
                elif isinstance(value, Node):
                    node_dict[field] = node_to_dict(value)
                else:
                    node_dict[field] = None if value == 'schemify' else value
        return node_dict
    
def write_schema(schema_path, dict_data):
        """Writes the modified schema back to the file using custom dumper."""
        with open(schema_path, 'w+', encoding='utf-8') as f:
            yaml.dump(dict_data, f, default_flow_style=False, Dumper=CustomDumper, sort_keys=False,  allow_unicode=True, default_style=None)

