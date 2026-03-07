SENTINEL = 'schemify'


def _empty(val):
    return val is None or val == '' or val == []


def _serialize(val):
    if isinstance(val, Node):
        return val.to_dict()
    elif isinstance(val, list):
        serialized = [_serialize(v) for v in val]
        return serialized if serialized else None
    else:
        return val


class Node:
    _fields = []      # field ordering hint for serialization
    _field_types = {} # field -> child Node class for auto-conversion

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            type_hint = self._field_types.get(key)
            if type_hint and value is not None:
                if isinstance(value, list):
                    setattr(self, key, [type_hint(**v) if isinstance(v, dict) else v for v in value])
                elif isinstance(value, dict):
                    if all(isinstance(v, dict) for v in value.values()):
                        setattr(self, key, [type_hint(**v) for v in value.values()])
                    else:
                        setattr(self, key, type_hint(**value))
                else:
                    setattr(self, key, value)
            else:
                setattr(self, key, value)
        # ensure declared fields have a default of None if not provided
        for key in self._fields:
            if not hasattr(self, key):
                setattr(self, key, None)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def field_items(self):
        """Yield (field, value) in _fields order first, then any extra fields."""
        seen = set()
        for field in self._fields:
            seen.add(field)
            yield field, getattr(self, field, None)
        for key, val in self.__dict__.items():
            if key not in seen and not key.startswith('_'):
                yield key, val

    def to_dict(self):
        result = {}
        for field, val in self.field_items():
            if not _empty(val):
                serialized = _serialize(val)
                if not _empty(serialized):
                    result[field] = serialized
        return result

    def __repr__(self):
        fields = ", ".join(f"{f}={repr(getattr(self, f, None))}" for f in self._fields)
        return f"{self.__class__.__name__}({fields})"


class ColumnNode(Node):
    _fields = ["name", "data_type", "description", "meta", "data_tests", "config"]
    _field_types = {}

class ConfigNode(Node):
    _fields = ["enabled", "materialized", "tags", "schema", "database", "alias",
               "persist_docs", "contract", "full_refresh", "pre-hook", "post-hook"]
    _field_types = {}

class ModelNode(Node):
    _fields = ["name", "description", "meta", "config", "data_tests", "columns", "docs"]
    _field_types = {
        "config": ConfigNode,
        "columns": ColumnNode,
    }

class DefaultColumnNode(Node):
    _fields = ["name", "data_type", "description", "data_tests"]
    _field_types = {}

class DefaultModelNode(Node):
    _fields = ["name", "description", "data_tests", "columns"]
    _field_types = {
        "columns": DefaultColumnNode
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

class ManifestNode(Node):
    _fields = ["nodes"]
    _field_types = {
        "nodes": ModelNode
    }


class NodeVisitor:
    def visit(self, node):
        method_name = f"visit_{node.__class__.__name__}"
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        for child_name, child in node.__dict__.items():
            if isinstance(child, Node):
                self.visit(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, Node):
                        self.visit(item)


class NodeTransformer:
    def transform(self, node):
        method_name = f"transform_{node.__class__.__name__}"
        transformer = getattr(self, method_name, self.generic_transform)
        return transformer(node)

    def generic_transform(self, node):
        for child_name, child in node.__dict__.items():
            if isinstance(child, Node):
                self.transform(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, Node):
                        self.transform(item)
