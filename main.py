from schema_editor import SchemaEditor

editor = SchemaEditor('schema.yml')

# Read schema
editor.read_schema()

# Flatten the schema
flattened_data = editor.flatten_schema()

# Gather the data back into the dict
reconstructed_schema = editor.gather_to_dict(flattened_data)

# Update schema_data with the reconstructed version
editor.schema_data = reconstructed_schema

# Write back to the file
editor.write_schema()

