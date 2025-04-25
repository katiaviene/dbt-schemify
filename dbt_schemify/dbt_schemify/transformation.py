from dbt_ast import NodeTransformer
from schema_editor import SchemaEditor
from dbt_ast import SchemaNode, ModelNode, ColumnNode

editor = SchemaEditor('.schemify.yml')
data = editor.read_schema()
template = editor.build_node(SchemaNode, data)

class SchemaTransformer(NodeTransformer):
    
    def transform(self, node):
        """Transform the node and check for missing fields from the template."""
        if isinstance(node, ModelNode):
            self._apply_model_fields(node)
        elif isinstance(node, ColumnNode):
            self._apply_column_fields(node)
        self.generic_transform(node)

    def _apply_model_fields(self, node):
        """Check and add missing fields for ModelNode."""
        required_fields = template.models[0].__dict__.items()
        
        for field, value in required_fields:
                if not hasattr(node, field)or getattr(node, field) is None:
                    print(f"Adding missing field: {field} to ModelNode value {value}")
                    setattr(node, field, value)

    def _apply_column_fields(self, node):
        """Check and add missing fields for ColumnNode."""
        required_fields = template.models[0].columns[0].__dict__.items()
        for field, value in required_fields:
            if value:
                if not hasattr(node, field) or getattr(node, field) is None :
                    print(f"Adding missing field: {field} to ColumnNode value {value}")
                    setattr(node, field, value)

