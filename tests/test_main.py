from pathlib import Path

from dbt_schemify.main import (
    _apply_selector,
    _group_nodes_by_dir,
    _group_nodes_by_model,
    _load_config,
    _resolve,
    CONFIG_FILE,
    DEFAULT_CONFIG,
)


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


# ---------------------------------------------------------------------------
# _group_nodes_by_model
# ---------------------------------------------------------------------------

class TestGroupNodesByModel:
    def test_each_model_gets_own_file(self):
        nodes = [
            _node('orders', original_file_path='models/marketing/orders.sql'),
            _node('campaigns', original_file_path='models/marketing/campaigns.sql'),
        ]
        groups = _group_nodes_by_model(nodes, project_dir='.')
        assert len(groups) == 2

    def test_file_named_after_model(self):
        nodes = [_node('orders', original_file_path='models/marketing/orders.sql')]
        groups = _group_nodes_by_model(nodes, project_dir='.')
        schema_path = next(iter(groups))
        assert schema_path.name == 'orders.yml'

    def test_file_placed_in_model_directory(self):
        nodes = [_node('orders', original_file_path='models/marketing/orders.sql')]
        groups = _group_nodes_by_model(nodes, project_dir='.')
        schema_path = next(iter(groups))
        assert schema_path == Path('.') / 'models' / 'marketing' / 'orders.yml'

    def test_each_group_contains_one_node(self):
        nodes = [
            _node('orders', original_file_path='models/orders.sql'),
            _node('customers', original_file_path='models/customers.sql'),
        ]
        groups = _group_nodes_by_model(nodes, project_dir='.')
        for node_list in groups.values():
            assert len(node_list) == 1

    def test_node_without_original_file_path_skipped(self):
        nodes = [{'name': 'broken', 'tags': []}]
        groups = _group_nodes_by_model(nodes, project_dir='.')
        assert len(groups) == 0


# ---------------------------------------------------------------------------
# Single-model dispatch helper
# ---------------------------------------------------------------------------

class TestSingleModelDispatch:
    """When exactly one node is selected, the output file is named after the model."""

    def test_single_node_produces_model_named_file(self):
        nodes = [_node('orders', original_file_path='models/finance/orders.sql')]
        # Single model → should use _group_nodes_by_model logic (named file)
        groups = _group_nodes_by_model(nodes, project_dir='.')
        assert len(groups) == 1
        schema_path = next(iter(groups))
        assert schema_path.name == 'orders.yml'
        assert schema_path == Path('.') / 'models' / 'finance' / 'orders.yml'

    def test_single_node_vs_multi_dir_differ(self):
        """Single-node path differs from multi-node default (schema.yml)."""
        nodes = [_node('orders', original_file_path='models/finance/orders.sql')]
        by_model = _group_nodes_by_model(nodes, project_dir='.')
        by_dir = _group_nodes_by_dir(nodes, project_dir='.')
        model_path = next(iter(by_model))
        dir_path = next(iter(by_dir))
        assert model_path.name == 'orders.yml'
        assert dir_path.name == 'schema.yml'


# ---------------------------------------------------------------------------
# _resolve (CLI > config > hardcoded default)
# ---------------------------------------------------------------------------

class TestResolve:
    def test_cli_wins_over_config_and_default(self):
        assert _resolve('cli', 'cfg', 'default') == 'cli'

    def test_config_wins_over_default_when_cli_is_none(self):
        assert _resolve(None, 'cfg', 'default') == 'cfg'

    def test_hardcoded_default_used_when_both_none(self):
        assert _resolve(None, None, 'default') == 'default'

    def test_false_cli_value_not_treated_as_none(self):
        # False is a valid CLI value (e.g. --each not passed gives None, not False)
        assert _resolve(False, True, True) is False

    def test_zero_cli_value_not_treated_as_none(self):
        assert _resolve(0, 99, 99) == 0


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_creates_config_file_if_missing(self, tmp_path):
        cfg = _load_config(tmp_path)
        assert (tmp_path / CONFIG_FILE).exists()
        # Returns a dict
        assert isinstance(cfg, dict)

    def test_created_file_contains_expected_keys(self, tmp_path):
        _load_config(tmp_path)
        text = (tmp_path / CONFIG_FILE).read_text()
        assert 'each' in text
        assert 'no_db' in text
        assert 'manifest' in text

    def test_default_sentinel_normalised_to_none(self, tmp_path):
        cfg = _load_config(tmp_path)
        # 'default' strings in DEFAULT_CONFIG should become None
        assert cfg.get('manifest') is None
        assert cfg.get('profile') is None
        assert cfg.get('target') is None

    def test_bool_defaults_preserved(self, tmp_path):
        cfg = _load_config(tmp_path)
        assert cfg.get('each') is False
        assert cfg.get('no_db') is False

    def test_reads_existing_config(self, tmp_path):
        (tmp_path / CONFIG_FILE).write_text('each: true\nno_db: false\n')
        cfg = _load_config(tmp_path)
        assert cfg.get('each') is True
        assert cfg.get('no_db') is False

    def test_custom_value_not_normalised(self, tmp_path):
        (tmp_path / CONFIG_FILE).write_text('profile: my_profile\n')
        cfg = _load_config(tmp_path)
        assert cfg.get('profile') == 'my_profile'

    def test_default_config_content_matches_constant(self, tmp_path):
        _load_config(tmp_path)
        written = (tmp_path / CONFIG_FILE).read_text()
        assert written == DEFAULT_CONFIG
