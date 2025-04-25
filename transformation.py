from dbt_ast import NodeTransformer
# Create a Transformer class that defines how to modify the nodes
class SchemaTransformer(NodeTransformer):
    def transform_ModelNode(self, node):
        # Modify the model description
        if node.description:
            node.description = node.description.lower()
        self.generic_transform(node)

    def transform_ColumnNode(self, node):
        # Modify the column data type
        if node.description:
            node.description = node.description.lower()
        self.generic_transform(node)

