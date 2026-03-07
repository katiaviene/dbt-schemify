import pytest
from dbt_schemify.dbt_ast import (
    SENTINEL, _empty, _serialize,
    Node, ColumnNode, ModelNode, ConfigNode, SchemaNode,
)


class TestEmpty:
    def test_none(self):
        assert _empty(None)

    def test_empty_string(self):
        assert _empty('')

    def test_empty_list(self):
        assert _empty([])

    def test_zero(self):
        assert not _empty(0)

    def test_false(self):
        assert not _empty(False)

    def test_value(self):
        assert not _empty('hello')

    def test_dict(self):
        assert not _empty({})


class TestNodeBasics:
    def test_stores_all_kwargs(self):
        node = Node(foo='bar', baz=42)
        assert node.foo == 'bar'
        assert node.baz == 42

    def test_extra_fields_stored(self):
        """Fields not in _fields are still stored."""
        col = ColumnNode(name='x', custom_field='y')
        assert col.custom_field == 'y'

    def test_declared_fields_default_to_none(self):
        col = ColumnNode(name='x')
        assert col.description is None
        assert col.data_type is None

    def test_get_returns_value(self):
        col = ColumnNode(name='x', description='desc')
        assert col.get('description') == 'desc'

    def test_get_returns_default(self):
        col = ColumnNode(name='x')
        assert col.get('description') is None
        assert col.get('missing', 'fallback') == 'fallback'


class TestFieldItems:
    def test_declared_fields_come_first(self):
        col = ColumnNode(name='x', description='d', extra='e')
        fields = [f for f, _ in col.field_items()]
        assert fields.index('name') < fields.index('extra')
        assert fields.index('description') < fields.index('extra')

    def test_all_fields_present(self):
        col = ColumnNode(name='x', description='d', extra='e')
        fields = [f for f, _ in col.field_items()]
        assert 'name' in fields
        assert 'description' in fields
        assert 'extra' in fields


class TestToDict:
    def test_basic_serialization(self):
        col = ColumnNode(name='col1', description='A column')
        d = col.to_dict()
        assert d['name'] == 'col1'
        assert d['description'] == 'A column'

    def test_none_fields_excluded(self):
        col = ColumnNode(name='col1')
        d = col.to_dict()
        assert 'description' not in d
        assert 'data_type' not in d

    def test_empty_string_excluded(self):
        col = ColumnNode(name='col1', description='')
        d = col.to_dict()
        assert 'description' not in d

    def test_nested_node_serialized(self):
        model = ModelNode(name='m', config=ConfigNode(enabled=True))
        d = model.to_dict()
        assert d['config'] == {'enabled': True}

    def test_list_of_nodes_serialized(self):
        model = ModelNode(name='m', columns=[ColumnNode(name='c1'), ColumnNode(name='c2')])
        d = model.to_dict()
        assert d['columns'] == [{'name': 'c1'}, {'name': 'c2'}]

    def test_extra_fields_included(self):
        col = ColumnNode(name='x', my_extra='val')
        d = col.to_dict()
        assert d['my_extra'] == 'val'


class TestTypedFieldConversion:
    def test_columns_converted_to_column_nodes(self):
        model = ModelNode(name='m', columns=[{'name': 'c1', 'description': 'desc'}])
        assert isinstance(model.columns[0], ColumnNode)
        assert model.columns[0].name == 'c1'

    def test_config_converted_to_config_node(self):
        model = ModelNode(name='m', config={'enabled': True})
        assert isinstance(model.config, ConfigNode)
        assert model.config.enabled is True

    def test_schema_node_models_converted(self):
        schema = SchemaNode(version=2, models=[{'name': 'm1'}])
        assert isinstance(schema.models[0], ModelNode)
        assert schema.models[0].name == 'm1'
