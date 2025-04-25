from schema_editor import SchemaEditor
from dbt_ast import SchemaNode
from transformation import SchemaTransformer
from generate import generate_default_schema

editor = SchemaEditor('empty_schema.yml')
data = editor.read_schema()
ast = editor.build_node(SchemaNode, data)

transformer = SchemaTransformer()
transformer.transform(ast)
new_dict = editor.node_to_dict(ast)
print(new_dict)
editor.schema_data = new_dict

editor.write_schema()
# default = generate_default_schema()

# editor_2 = SchemaEditor('.schemify.yml')
# default = editor_2.node_to_dict(default)
# editor_2.schema_data = default
# editor_2.write_schema()

# print(default)
