class BaseNode:
    def __init__(self, name, data_tests=None, description=None, meta=None):
        self.name = name
        self.description = description
        self.meta = meta or {}
        self.data_tests = data_tests or []

    def set_description(self, description):
        self.description = description

    def set_meta(self, key, value):
        self.meta[key] = value

    def get_meta(self, key, default=None):
        return self.meta.get(key, default)


class ColumnNode(BaseNode):
    def __init__(self, name, description=None, tests=None, meta=None):
        super().__init__(name, description, meta)



class ModelNode(BaseNode):
    def __init__(self, name, description=None, columns=None, meta=None, config=None):
        super().__init__(name, description, meta)
        self.columns = {col['name']: ColumnNode(**col) for col in (columns or [])}
        self.config = config or {}

    def add_column(self, column: ColumnNode):
        self.columns[column.name] = column

    def get_column(self, column_name):
        return self.columns.get(column_name)

    def set_config(self, key, value):
        self.config[key] = value

    def get_config(self, key, default=None):
        return self.config.get(key, default)

class SchemaNode:
    def __init__(self, models=None, version=None, default_meta=None):
        self.models = {model['name']: ModelNode(**model) for model in (models or [])}
        self.version = version
        self.default_meta = default_meta or {}

    def add_model(self, model: ModelNode):
        self.models[model.name] = model

    def get_model(self, model_name):
        return self.models.get(model_name)
