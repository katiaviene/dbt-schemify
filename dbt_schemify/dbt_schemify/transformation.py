from dbt_ast import NodeTransformer
from schema_editor import SchemaEditor
from dbt_ast import SchemaNode, ModelNode, ColumnNode, ManifestNode

editor = SchemaEditor('dbt_schemify/examples/.schemify.yml')
data = editor.read_schema()
template = editor.build_node(SchemaNode, data)
source_data = editor.read_manifest('dbt_schemify/examples/manifest.json')
source_ast = ManifestNode(**source_data)
# print(source_ast)

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
            if not hasattr(node, field) or getattr(node, field) is None:
                manifest_value = getattr(source_ast.nodes[0], field, None)
                if manifest_value is not None:
                    if isinstance(manifest_value, dict) and isinstance(value, dict):
                        combined_value = {**value, **manifest_value}
                        setattr(node, field, combined_value)
                    else:
                        final_value = manifest_value if manifest_value is not None else value
                        setattr(node, field, final_value)
                else:
                    setattr(node, field, value)

    def _apply_column_fields(self, node):
        """Check and add missing fields for ColumnNode."""
        required_fields = template.models[0].columns[0].__dict__.items()
        for field, value in required_fields:
            if value:
                if not hasattr(node, field) or getattr(node, field) is None :
                    setattr(node, field, value)

