from schema_editor import SchemaEditor
from dbt_ast import SchemaNode, ManifestNode
from transformation import SchemaTransformer
from generate import generate_default_schema

editor = SchemaEditor('dbt_schemify/examples/schema.yml')
schema = editor.read_schema()
data = editor.read_manifest('dbt_schemify/examples/manifest.json')
ast_schema = editor.build_node(SchemaNode, schema)
ast = ManifestNode(**data)
print(ast_schema)
print(ast)



# transformer = SchemaTransformer()
# transformer.transform(ast)
# new_dict = editor.node_to_dict(ast)
# print(new_dict)
# editor.schema_data = new_dict

# editor.write_schema()
