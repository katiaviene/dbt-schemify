from pathlib import Path

from dbt_schemify.main import _apply_selector, _group_nodes_by_dir


def _node(name, tags=None, original_file_path=None):
    return {
        'name': name,
        'tags': tags or [],
        'original_file_path': original_file_path or f'models/{name}.sql',
    }


# ---------------------------------------------------------------------------
# _apply_selector
# ---------------------------------------------------------------------------

class TestApplySelector:
    def test_match_by_name(self):
        nodes = [_node('orders'), _node('customers')]
        result = _apply_selector(nodes, ['orders'])
        assert len(result) == 1
        assert result[0]['name'] == 'orders'

    def test_match_by_tag(self):
        nodes = [
            _node('orders', tags=['marketing']),
            _node('customers', tags=['finance']),
        ]
        result = _apply_selector(nodes, ['tag:marketing'])
        assert len(result) == 1
        assert result[0]['name'] == 'orders'

    def test_no_match_returns_empty(self):
        nodes = [_node('orders'), _node('customers')]
        result = _apply_selector(nodes, ['unknown'])
        assert result == []

    def test_no_tag_match_returns_empty(self):
        nodes = [_node('orders', tags=['finance'])]
        result = _apply_selector(nodes, ['tag:marketing'])
        assert result == []

    def test_mixed_name_and_tag(self):
        nodes = [
            _node('orders', tags=['marketing']),
            _node('customers', tags=['finance']),
            _node('products'),
        ]
        result = _apply_selector(nodes, ['tag:finance', 'products'])
        names = [n['name'] for n in result]
        assert 'customers' in names
        assert 'products' in names
        assert 'orders' not in names

    def test_node_with_multiple_tags(self):
        nodes = [_node('orders', tags=['marketing', 'finance'])]
        result = _apply_selector(nodes, ['tag:finance'])
        assert len(result) == 1

    def test_node_not_duplicated_when_matches_multiple_selectors(self):
        nodes = [_node('orders', tags=['marketing'])]
        result = _apply_selector(nodes, ['orders', 'tag:marketing'])
        assert len(result) == 1

    def test_all_nodes_returned_when_all_match(self):
        nodes = [_node('a', tags=['x']), _node('b', tags=['x'])]
        result = _apply_selector(nodes, ['tag:x'])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _group_nodes_by_dir
# ---------------------------------------------------------------------------

class TestGroupNodesByDir:
    def test_single_model_grouped_correctly(self):
        nodes = [_node('orders', original_file_path='models/marketing/orders.sql')]
        groups = _group_nodes_by_dir(nodes, project_dir='.')
        assert len(groups) == 1
        schema_path = next(iter(groups))
        assert schema_path == Path('.') / 'models' / 'marketing' / 'schema.yml'

    def test_two_models_same_dir_in_one_group(self):
        nodes = [
            _node('orders', original_file_path='models/marketing/orders.sql'),
            _node('campaigns', original_file_path='models/marketing/campaigns.sql'),
        ]
        groups = _group_nodes_by_dir(nodes, project_dir='.')
        assert len(groups) == 1
        assert len(next(iter(groups.values()))) == 2

    def test_two_models_different_dirs_in_separate_groups(self):
        nodes = [
            _node('orders', original_file_path='models/marketing/orders.sql'),
            _node('costs', original_file_path='models/finance/costs.sql'),
        ]
        groups = _group_nodes_by_dir(nodes, project_dir='.')
        assert len(groups) == 2

    def test_schema_path_uses_project_dir(self):
        nodes = [_node('orders', original_file_path='models/orders.sql')]
        groups = _group_nodes_by_dir(nodes, project_dir='/my/project')
        schema_path = next(iter(groups))
        assert schema_path == Path('/my/project') / 'models' / 'schema.yml'

    def test_node_without_original_file_path_skipped(self):
        nodes = [{'name': 'broken', 'tags': []}]  # no original_file_path
        groups = _group_nodes_by_dir(nodes, project_dir='.')
        assert len(groups) == 0
