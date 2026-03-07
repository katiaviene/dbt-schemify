import yaml
from dbt_schemify.dbt_ast import DefaultColumnNode, DefaultModelNode, DefaultSchemaNode, Node
from dbt_schemify.schema_editor import CustomDumper


def generate_default_schema():
    column = DefaultColumnNode()
    model = DefaultModelNode(columns=[column])
    return DefaultSchemaNode(version="1.0", models=[model])


def node_to_dict(node):
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
    with open(schema_path, 'w+', encoding='utf-8') as f:
        yaml.dump(
            dict_data, f,
            default_flow_style=False,
            Dumper=CustomDumper,
            sort_keys=False,
            allow_unicode=True,
            default_style=None,
        )
