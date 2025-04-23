from schema_editor import SchemaEditor

schema_editor = SchemaEditor('schema.yml')

print(schema_editor.read_schema())