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
    _fields = ["name", "description", "tests"]
    _field_types = {}


class ModelNode(Node):
    _fields = ["name", "description", "columns"]
    _field_types = {
        "columns": ColumnNode,
    }


class SchemaNode(Node):
    _fields = ["version", "models"]
    _field_types = {
        "models": ModelNode,
    }
