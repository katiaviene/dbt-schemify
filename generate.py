from dbt_ast import DefaultColumnNode, DefaultModelNode, DefaultSchemaNode
def generate_default_schema():
    # Create ColumnNode with default data
    column1 = DefaultColumnNode()
    # Create ModelNode with default data and add columns to it
    model = DefaultModelNode( columns=[column1])

    # Create SchemaNode and add models to it
    schema = DefaultSchemaNode(version="1.0", models=[model])

    return schema