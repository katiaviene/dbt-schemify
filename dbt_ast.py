class Node:
    _fields = []
    _field_types = {}

    def __init__(self, **kwargs):
        for key in self._fields:
            setattr(self, key, kwargs.get(key))

    def __repr__(self):
        fields = ", ".join(f"{f}={repr(getattr(self, f))}" for f in self._fields)
        return f"{self.__class__.__name__}({fields})"

class DefaultColumnNode(Node):
    _fields = ["name", "data_type", "description", "data_tests"]
    _field_types = {
    }


class ColumnNode(Node):
    _fields = ["name", "data_type", "description", "meta", "data_tests", "config"]
    _field_types = {
    }


class ConfigNode(Node):
    _fields = ["enabled", "materialized", "tags", "schema", "database", "alias", "persist_docs", "contract", "full_refresh", "pre-hook", "post-hook"]
    _field_types = {}

class DefaultModelNode(Node):
    _fields = ["name", "description",  "data_tests",  "columns"]
    _field_types = {
        "columns": DefaultColumnNode
    }
    
class ModelNode(Node):
    _fields = ["name", "description", "meta", "config", "data_tests",  "columns", "docs"]
    _field_types = {
        "config": ConfigNode,
        "columns": ColumnNode

    }
    
class DefaultSchemaNode(Node):
    _fields = ["version", "models"]
    _field_types = {
        "models": DefaultModelNode 
    }
    
class SchemaNode(Node):
    _fields = ["version", "models"]
    _field_types = {
        "models": ModelNode 
    }
    

class NodeVisitor:
    def visit(self, node):
        """Visit the node."""
        method_name = f"visit_{node.__class__.__name__}"
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        """Visit all the child nodes."""
        for child_name, child in node.__dict__.items():
            if isinstance(child, Node):
                self.visit(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, Node):
                        self.visit(item)

class NodeTransformer:
    def transform(self, node):
        """Transform the node."""
        method_name = f"transform_{node.__class__.__name__}"
        transformer = getattr(self, method_name, self.generic_transform)
        return transformer(node)

    def generic_transform(self, node):
        """Transform all the child nodes."""
        for child_name, child in node.__dict__.items():
            if isinstance(child, Node):
                self.transform(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, Node):
                        self.transform(item)
