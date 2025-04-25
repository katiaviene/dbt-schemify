from schema_editor import SchemaEditor
from dbt_ast import SchemaNode
from transformation import SchemaTransformer

editor = SchemaEditor('schema.yml')
data = editor.read_schema()
ast = editor.build_node(SchemaNode, data)
# Initialize the transformer
transformer = SchemaTransformer()

# Transform the root schema node
transformer.transform(ast)

print(ast)
