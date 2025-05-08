from dbt_ast import NodeTransformer
from schema_editor import SchemaEditor
from dbt_ast import SchemaNode, ModelNode, ColumnNode, ManifestNode, Node

editor = SchemaEditor('dbt_schemify/examples/.schemify.yml')
data = editor.read_schema()
template = editor.build_node(SchemaNode, data)
print("template")
print(template)
source_data = editor.read_manifest('dbt_schemify/examples/manifest.json')
source_ast = ManifestNode(**source_data)
# print(source_ast)

class SchemaTransformer(NodeTransformer):
    
    def transform(self, node):
        """Transform the node and check for missing fields from the template."""
        if isinstance(node, ModelNode):
            self._apply_model_fields(node)
        # elif isinstance(node, ColumnNode):
        #     self._apply_column_fields(node)
        self.generic_transform(node)

    def _apply_model_fields(self, node):
        """Check and add missing fields for ModelNode."""
        required_fields = template.models[0].__dict__.items()
        for field, value in required_fields:
            if (not hasattr(node, field) or getattr(node, field) is None) and value is not None:
                print(value if field=="config" else None)
                manifest_value = None
                for manifest_node in source_ast.nodes:
                    if manifest_node.name == node.name:
                        manifest_value = getattr(manifest_node, field, None) 
                        break
                print(manifest_value if field=="config" else None)
                if manifest_value is not None:
                    if isinstance(manifest_value, dict) and isinstance(value, dict):
                        combined_value = {**value, **manifest_value}
                        setattr(node, field, combined_value)
                    else:
                        final_value = manifest_value if manifest_value is not None else value
                        setattr(node, field, final_value)
                else:
                    setattr(node, field, value)


