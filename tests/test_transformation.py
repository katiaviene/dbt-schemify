import pytest
from dbt_schemify.dbt_ast import ModelNode, ColumnNode, SchemaNode
from dbt_schemify.transformation import SchemifyTransformer


def make_transformer(template_dict, existing=None, manifest_nodes=None, db_cols=None):
    template_model = ModelNode(**template_dict)
    existing_by_name = {
        m['name']: ModelNode(**m)
        for m in (existing or [])
    }
    return SchemifyTransformer(
        template_model,
        existing_by_name,
        manifest_nodes or [],
        db_cols or {},
    )


# ---------------------------------------------------------------------------
# _merge_dict
# ---------------------------------------------------------------------------

class TestMergeDict:
    def setup_method(self):
        self.t = make_transformer({'name': 'schemify'})

    def test_existing_takes_priority(self):
        result = self.t._merge_dict(
            {'key': 'schemify'},
            {'key': 'existing'},
            {'key': 'source'},
        )
        assert result['key'] == 'existing'

    def test_sentinel_filled_from_source(self):
        result = self.t._merge_dict(
            {'key': 'schemify'},
            {},
            {'key': 'from_manifest'},
        )
        assert result['key'] == 'from_manifest'

    def test_sentinel_empty_source_omitted(self):
        result = self.t._merge_dict({'key': 'schemify'}, {}, {})
        assert 'key' not in result

    def test_static_default_used(self):
        result = self.t._merge_dict({'key': 'default_val'}, {}, {})
        assert result['key'] == 'default_val'

    def test_nested_dict_merged(self):
        result = self.t._merge_dict(
            {'meta': {'owner': 'analytics'}},
            {'meta': {'owner': 'existing_owner'}},
            {},
        )
        assert result['meta']['owner'] == 'existing_owner'

    def test_keys_not_in_template_excluded(self):
        result = self.t._merge_dict(
            {'a': 'val'},
            {'b': 'should_not_appear'},
            {'c': 'also_not'},
        )
        assert 'b' not in result
        assert 'c' not in result


# ---------------------------------------------------------------------------
# _merge_column
# ---------------------------------------------------------------------------

class TestMergeColumn:
    def setup_method(self):
        self.template_col = ColumnNode(
            name='schemify',
            description='schemify',
            data_type='schemify',
            meta={'gdpr_tags': 'schemify'},
        )
        self.t = make_transformer({'name': 'schemify'})

    def test_name_from_db(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {'name': 'db_col', 'data_type': 'TEXT'},
            {},
        )
        assert result.name == 'db_col'

    def test_name_from_manifest_when_no_db(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {},
            {'name': 'manifest_col', 'description': 'desc'},
        )
        assert result.name == 'manifest_col'

    def test_data_type_from_db(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {'name': 'col', 'data_type': 'INTEGER'},
            {'name': 'col', 'data_type': 'TEXT'},
        )
        assert result.data_type == 'INTEGER'

    def test_data_type_fallback_to_manifest(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {'name': 'col'},
            {'name': 'col', 'data_type': 'TEXT'},
        )
        assert result.data_type == 'TEXT'

    def test_description_from_manifest_sentinel(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {'name': 'col'},
            {'name': 'col', 'description': 'manifest desc'},
        )
        assert result.description == 'manifest desc'

    def test_existing_value_preserved(self):
        existing_col = ColumnNode(name='col', description='existing desc')
        result = self.t._merge_column(
            self.template_col,
            existing_col,
            {'name': 'col', 'data_type': 'TEXT'},
            {'name': 'col', 'description': 'manifest desc'},
        )
        assert result.description == 'existing desc'

    def test_meta_dict_merged(self):
        result = self.t._merge_column(
            self.template_col,
            None,
            {'name': 'col'},
            {'name': 'col', 'meta': {'gdpr_tags': 'PII'}},
        )
        assert result.get('meta') == {'gdpr_tags': 'PII'}

    def test_fields_not_in_template_excluded(self):
        template_col = ColumnNode(name='schemify', description='schemify')
        result = self.t._merge_column(
            template_col,
            None,
            {'name': 'col'},
            {'name': 'col', 'description': 'desc', 'data_type': 'TEXT'},
        )
        assert result.get('data_type') is None  # not in template


# ---------------------------------------------------------------------------
# _merge_columns
# ---------------------------------------------------------------------------

class TestMergeColumns:
    def setup_method(self):
        self.template_col = ColumnNode(name='schemify', description='schemify', data_type='schemify')
        self.t = make_transformer({'name': 'schemify'})

    def test_db_columns_used_when_available(self):
        db_cols = [{'name': 'col_a', 'data_type': 'TEXT'}]
        result = self.t._merge_columns(self.template_col, [], db_cols, [])
        assert len(result) == 1
        assert result[0].name == 'col_a'
        assert result[0].data_type == 'TEXT'

    def test_manifest_fallback_when_no_db(self):
        manifest_cols = [
            {'name': 'col_a', 'description': 'desc a'},
            {'name': 'col_b', 'description': 'desc b'},
        ]
        result = self.t._merge_columns(self.template_col, [], [], manifest_cols)
        assert len(result) == 2
        assert result[0].description == 'desc a'

    def test_existing_preserved_when_no_db_or_manifest(self):
        existing = [ColumnNode(name='col_a', description='existing')]
        result = self.t._merge_columns(self.template_col, existing, [], [])
        assert len(result) == 1
        assert result[0].name == 'col_a'

    def test_existing_not_in_db_preserved(self):
        existing = [
            ColumnNode(name='col_a'),
            ColumnNode(name='col_b'),
        ]
        db_cols = [{'name': 'col_a', 'data_type': 'TEXT'}]
        result = self.t._merge_columns(self.template_col, existing, db_cols, [])
        names = [c.name for c in result]
        assert 'col_a' in names
        assert 'col_b' in names


# ---------------------------------------------------------------------------
# run() — full integration
# ---------------------------------------------------------------------------

class TestRun:
    def _transformer(self, template_dict, existing=None, manifest_nodes=None, db_cols=None):
        return make_transformer(template_dict, existing, manifest_nodes, db_cols)

    def test_produces_schema_node(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify'},
            manifest_nodes=[{'name': 'my_model', 'description': 'Model desc', 'columns': {}}],
        )
        result = t.run()
        assert isinstance(result, SchemaNode)

    def test_model_name_set_from_manifest(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify'},
            manifest_nodes=[{'name': 'my_model', 'description': 'desc', 'columns': {}}],
        )
        result = t.run()
        assert result.models[0].name == 'my_model'

    def test_sentinel_description_filled(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify'},
            manifest_nodes=[{'name': 'my_model', 'description': 'From manifest', 'columns': {}}],
        )
        result = t.run()
        assert result.models[0].description == 'From manifest'

    def test_existing_description_preserved(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify'},
            existing=[{'name': 'my_model', 'description': 'Existing desc'}],
            manifest_nodes=[{'name': 'my_model', 'description': 'Manifest desc', 'columns': {}}],
        )
        result = t.run()
        assert result.models[0].description == 'Existing desc'

    def test_static_template_field_applied(self):
        t = self._transformer(
            {'name': 'schemify', 'meta': {'owner': 'analytics'}},
            manifest_nodes=[{'name': 'my_model', 'columns': {}}],
        )
        result = t.run()
        assert result.models[0].get('meta') == {'owner': 'analytics'}

    def test_existing_model_not_in_manifest_preserved(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify'},
            existing=[
                {'name': 'model_a', 'description': 'existing'},
                {'name': 'model_b', 'description': 'also existing'},
            ],
            manifest_nodes=[{'name': 'model_a', 'description': 'from manifest', 'columns': {}}],
        )
        result = t.run()
        names = [m.name for m in result.models]
        assert 'model_a' in names
        assert 'model_b' in names

    def test_columns_populated_from_manifest(self):
        t = self._transformer(
            {
                'name': 'schemify',
                'columns': [{'name': 'schemify', 'description': 'schemify'}],
            },
            manifest_nodes=[{
                'name': 'my_model',
                'columns': {
                    'col_a': {'name': 'col_a', 'description': 'Col A desc'},
                    'col_b': {'name': 'col_b', 'description': 'Col B desc'},
                },
            }],
        )
        result = t.run()
        cols = result.models[0].columns
        assert len(cols) == 2
        names = [c.name for c in cols]
        assert 'col_a' in names
        assert 'col_b' in names

    def test_columns_populated_from_db(self):
        t = self._transformer(
            {
                'name': 'schemify',
                'columns': [{'name': 'schemify', 'data_type': 'schemify'}],
            },
            manifest_nodes=[{'name': 'my_model', 'columns': {}}],
            db_cols={'my_model': [
                {'name': 'col_a', 'data_type': 'INTEGER'},
                {'name': 'col_b', 'data_type': 'TEXT'},
            ]},
        )
        result = t.run()
        cols = result.models[0].columns
        assert len(cols) == 2
        assert cols[0].data_type == 'INTEGER'
        assert cols[1].data_type == 'TEXT'

    def test_to_dict_roundtrip(self):
        t = self._transformer(
            {'name': 'schemify', 'description': 'schemify', 'meta': {'owner': 'analytics'}},
            manifest_nodes=[{'name': 'my_model', 'description': 'Desc', 'columns': {}}],
        )
        result = t.run()
        d = result.to_dict()
        assert d['version'] == 2
        assert d['models'][0]['name'] == 'my_model'
        assert d['models'][0]['description'] == 'Desc'
        assert d['models'][0]['meta'] == {'owner': 'analytics'}
