from dbt_schemify.dbt_ast import NodeTransformer, ModelNode

# NOTE: The active merge logic lives in merger.py.
# This module is kept for reference / backwards compatibility.


class SchemaTransformer(NodeTransformer):
    """
    Applies a template to an existing schema AST in-place.

    Deprecated in favour of merger.merge_schema(), which works on plain
    dicts and does not require the AST layer.
    """

    def __init__(self, template_node, source_ast):
        self.template = template_node
        self.source_ast = source_ast

    def transform(self, node):
        if isinstance(node, ModelNode):
            self._apply_model_fields(node)
        self.generic_transform(node)

    def _apply_model_fields(self, node):
        template_model = self.template.models[0]

        for field, tmpl_value in template_model.__dict__.items():
            if (not hasattr(node, field) or getattr(node, field) is None) and tmpl_value is not None:
                manifest_value = None
                for manifest_node in self.source_ast.nodes:
                    if manifest_node.name == node.name:
                        manifest_value = getattr(manifest_node, field, None)
                        break

                if manifest_value is not None:
                    if isinstance(manifest_value, dict) and isinstance(tmpl_value, dict):
                        combined = {k: manifest_value.get(k, v) for k, v in tmpl_value.items()}
                        setattr(node, field, combined)
                    else:
                        setattr(node, field, manifest_value)
                else:
                    setattr(node, field, tmpl_value)
