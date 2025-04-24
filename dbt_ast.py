class Node:
    _fields = []
    _field_types = {}

    def __init__(self, **kwargs):
        for key in self._fields:
            setattr(self, key, kwargs.get(key))

    def __repr__(self):
        fields = ", ".join(f"{f}={repr(getattr(self, f))}" for f in self._fields)
        return f"{self.__class__.__name__}({fields})"


    
class ColumnNode(Node):
    _fields = ["name", "data_type", "description", "meta", "data_tests", "config"]
    _field_types = {
    }

class ConfigNode(Node):
    _fields = ["enabled", "materialized", "tags", "schema", "database", "alias", "persist_docs", "contract", "full_refresh", "pre-hook", "post-hook"]
    _field_types = {}

class ModelNode(Node):
    _fields = ["name", "description", "meta", "config", "data_tests",  "columns", "docs"]
    _field_types = {
        "config": ConfigNode,
        "columns": ColumnNode
        
    }


class SchemaNode(Node):
    _fields = ["version", "models"]
    _field_types = {
        "models": ModelNode,
    }
