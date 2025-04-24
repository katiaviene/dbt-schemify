from schema_editor import SchemaEditor
from dbt_ast import SchemaNode

editor = SchemaEditor('schema.yml')
data = editor.read_schema()
ast = editor.build_node(SchemaNode, data)

print(ast)
