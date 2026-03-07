"""
schemify CLI

Usage:
  schemify                         # auto-discover: writes schema.yml next to each model
  schemify --schema path/schema.yml  # explicit: write all selected models into one file

All options:
  --schema         PATH   Path to schema.yml to write (optional; default: auto-discover from manifest)
  --manifest       PATH   Path to manifest.json (default: ./target/manifest.json)
  --template       PATH   Path to .schemify.yml template (default: ./.schemify.yml)
  --project-dir    DIR    dbt project root (default: current directory)
  --profile        NAME   dbt profile name (default: read from dbt_project.yml)
  --target         NAME   dbt target (default: profile default)
  --profiles-dir   DIR    Directory containing profiles.yml (default: ~/.dbt/)
  -s / --select    SELECTOR  Filter models (space-separated). Supports model names and tag:value.
                             Examples: -s my_model   -s tag:marketing   -s tag:finance orders
  --each                  Write one <model_name>.yml per model instead of one schema.yml per folder
  --no-db                 Skip database connection; no column fetching
  --info                  Show resolved paths and configuration, then exit
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import yaml

from dbt_schemify.schema_editor import CustomDumper
from dbt_schemify.dbt_ast import ModelNode
from dbt_schemify.transformation import SchemifyTransformer

CONFIG_FILE = '.schemify-config.yml'

DEFAULT_TEMPLATE = """\
version: '1.0'
models:
  - name: schemify
    description: schemify
    meta:
      owner: analytics
    config:
      enabled: true
    columns:
      - name: schemify
        data_type: schemify

"""

DEFAULT_CONFIG = """\
# schemify configuration
# Values here are used as defaults on every run.
# Command-line arguments always take priority over this file.
# Use 'default' for paths/names to let schemify auto-resolve them.

# Output mode
each: false          # write one <model>.yml per model instead of schema.yml per folder
no_db: false         # skip database connection; no column fetching

# Paths ('default' = auto-resolved)
manifest: default    # manifest.json path;      auto: <project-dir>/target/manifest.json
template: default    # .schemify.yml path;       auto: <project-dir>/.schemify.yml
profiles_dir: default  # profiles.yml directory; auto: ~/.dbt/

# dbt connection ('default' = auto-resolved)
profile: default     # dbt profile name;  auto: read from dbt_project.yml
target: default      # dbt target name;   auto: profile default
"""


def _read_dbt_project(project_dir):
    path = Path(project_dir) / 'dbt_project.yml'
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def _read_manifest_nodes(manifest_path):
    with open(manifest_path) as f:
        manifest = json.load(f)
    return [
        node
        for node in manifest.get('nodes', {}).values()
        if node.get('resource_type') == 'model'
    ]


def _apply_selector(manifest_nodes, selectors):
    """Filter nodes by dbt-style selectors: model names or tag:<value>."""
    result = []
    for node in manifest_nodes:
        for sel in selectors:
            if sel.startswith('tag:'):
                tag = sel[4:]
                if tag in (node.get('tags') or []):
                    result.append(node)
                    break
            else:
                if node['name'] == sel:
                    result.append(node)
                    break
    return result


def _group_nodes_by_dir(manifest_nodes, project_dir):
    """Group manifest nodes by the directory of their SQL file.
    Returns dict: schema_path -> list of nodes.
    """
    groups = defaultdict(list)
    for node in manifest_nodes:
        file_path = node.get('original_file_path')
        if not file_path:
            print(f"Warning: no original_file_path for model '{node.get('name')}', skipping.", file=sys.stderr)
            continue
        schema_path = Path(project_dir) / Path(file_path).parent / 'schema.yml'
        groups[schema_path].append(node)
    return groups


def _group_nodes_by_model(manifest_nodes, project_dir):
    """One schema file per model: <model_dir>/<model_name>.yml"""
    groups = {}
    for node in manifest_nodes:
        file_path = node.get('original_file_path')
        if not file_path:
            print(f"Warning: no original_file_path for model '{node.get('name')}', skipping.", file=sys.stderr)
            continue
        schema_path = Path(project_dir) / Path(file_path).parent / f"{node['name']}.yml"
        groups[schema_path] = [node]
    return groups


def _write_schema(schema_path, nodes, template_model, db_cols_by_model):
    existing_schema = {}
    if schema_path.exists():
        with open(schema_path) as f:
            existing_schema = yaml.safe_load(f) or {}

    existing_by_name = {
        m['name']: ModelNode(**m)
        for m in existing_schema.get('models', [])
    }
    transformer = SchemifyTransformer(
        template_model,
        existing_by_name,
        nodes,
        db_cols_by_model,
    )
    result = transformer.run(existing_version=existing_schema.get('version', 2))

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with open(schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(result.to_dict(), f, default_flow_style=False, Dumper=CustomDumper,
                  sort_keys=False, allow_unicode=True)
    print(f"Schema written to {schema_path}")


def _fetch_db_columns(manifest_nodes, config):
    from dbt_schemify.db_connector import get_columns
    db_cols = {}
    for node in manifest_nodes:
        name = node['name']
        try:
            cols = get_columns(config, node.get('database'), node.get('schema'), name)
            db_cols[name] = cols
            print(f"  {name}: {len(cols)} columns")
        except Exception as exc:
            print(f"  Warning: could not fetch columns for '{name}': {exc}", file=sys.stderr)
    return db_cols


def _init(project_dir, template_path):
    """Create config and template files for first-time setup."""
    config_path = Path(project_dir) / CONFIG_FILE
    any_created = False

    if not config_path.exists():
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(DEFAULT_CONFIG)
        print(f"Created config:   {config_path}")
        any_created = True
    else:
        print(f"Config already exists:   {config_path}")

    if not template_path.exists():
        template_path.parent.mkdir(parents=True, exist_ok=True)
        with open(template_path, 'w', encoding='utf-8') as f:
            f.write(DEFAULT_TEMPLATE)
        print(f"Created template: {template_path}")
        any_created = True
    else:
        print(f"Template already exists: {template_path}")

    if any_created:
        print("\nEdit the files above to configure your setup, then run 'schemify'.")
    else:
        print("\nAll files already exist. Run 'schemify' to generate schemas.")


def _load_config(project_dir):
    """Read .schemify-config.yml. Returns None if file doesn't exist.
    Returns dict with 'default' normalised to None.
    """
    config_path = Path(project_dir) / CONFIG_FILE
    if not config_path.exists():
        return None
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    # Normalise the 'default' sentinel string to None everywhere
    return {k: (None if v == 'default' else v) for k, v in raw.items()}


def _confirm(prompt, auto_yes=False):
    """Prompt user for Y/n confirmation. Returns True if confirmed."""
    if auto_yes:
        print(f"{prompt} [Y/n] Y")
        return True
    try:
        answer = input(f"{prompt} [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ('', 'y', 'yes')


def _check_schema_pattern_conflicts(groups, mode):
    """Check for schema pattern mismatches in target directories.

    mode: 'dir' (schema.yml per folder) or 'model' (per-model .yml files).
    Returns list of (dir_path, description) tuples.
    """
    conflicts = []

    if mode == 'dir':
        # Build map: dir → model names being written there
        dir_to_nodes = defaultdict(list)
        for schema_path, nodes in groups.items():
            dir_to_nodes[schema_path.parent].extend(nodes)
        for dir_path, nodes in dir_to_nodes.items():
            if not dir_path.exists():
                continue
            model_names = {n['name'] for n in nodes}
            conflicting = [f for f in dir_path.glob('*.yml') if f.stem in model_names]
            if conflicting:
                names = ', '.join(f.name for f in sorted(conflicting))
                conflicts.append((dir_path, f"per-model files exist: {names}"))
    else:  # mode == 'model'
        dirs_seen = set()
        for schema_path in groups:
            dir_path = schema_path.parent
            if dir_path in dirs_seen:
                continue
            dirs_seen.add(dir_path)
            if not dir_path.exists():
                continue
            if (dir_path / 'schema.yml').exists():
                conflicts.append((dir_path, "schema.yml (per-folder pattern) exists"))

    return conflicts


def _resolve(cli_val, config_val, hardcoded):
    """Priority: CLI arg > config file > hardcoded default."""
    if cli_val is not None:
        return cli_val
    if config_val is not None:
        return config_val
    return hardcoded


def main():
    parser = argparse.ArgumentParser(
        prog='schemify',
        description='Generate and update dbt schema.yml files.',
    )
    parser.add_argument('--schema',
                        help='Path to schema.yml to write. If omitted, schema.yml is created next to each model.')
    parser.add_argument('--manifest',
                        help='Path to manifest.json. Default: <project-dir>/target/manifest.json')
    parser.add_argument('--template',
                        help='Path to .schemify.yml template. Default: <project-dir>/.schemify.yml')
    parser.add_argument('--project-dir', default='.',
                        help='dbt project root directory. Default: current directory.')
    parser.add_argument('--profile',
                        help='dbt profile name. Default: read from dbt_project.yml.')
    parser.add_argument('--target',
                        help='dbt target (e.g. dev, prod). Default: profile default.')
    parser.add_argument('--profiles-dir',
                        help='Directory containing profiles.yml. Default: ~/.dbt/')
    parser.add_argument('-s', '--select', nargs='+', metavar='SELECTOR',
                        help='Filter models. Supports model names and tag:value (e.g. -s tag:marketing orders).')
    # Use default=None so we can distinguish "user passed this flag" from "not passed"
    # (needed to let config-file values win when the flag is absent)
    parser.add_argument('--each', action='store_true', default=None,
                        help='Write one <model_name>.yml per model instead of one schema.yml per folder.')
    parser.add_argument('--no-db', action='store_true', default=None,
                        help='Skip database connection and column fetching.')
    parser.add_argument('--info', action='store_true',
                        help='Show resolved paths and configuration, then exit.')
    parser.add_argument('--init', action='store_true',
                        help='Create default .schemify-config.yml and .schemify.yml, then exit.')
    parser.add_argument('-y', '--yes', action='store_true',
                        help='Skip confirmation prompts (useful for CI).')
    parser.add_argument('--debug-db', action='store_true',
                        help='Show resolved DB connection config and test the connection, then exit.')

    args = parser.parse_args()
    project_dir = Path(args.project_dir).resolve()

    # Resolve template path early (needed for --init before config exists)
    template_path_early = Path(args.template).resolve() if args.template else project_dir / '.schemify.yml'

    # --- --init: create files and exit ---
    if args.init:
        _init(project_dir, template_path_early)
        return

    # --- Config file (must exist; tell user to run --init if missing) ---
    cfg = _load_config(project_dir)
    if cfg is None:
        config_path = project_dir / CONFIG_FILE
        print(f"No config file found at {config_path}.", file=sys.stderr)
        print("Run 'schemify --init' to create it.", file=sys.stderr)
        sys.exit(1)

    # --- Resolve all options: CLI > config > hardcoded default ---
    each   = _resolve(args.each,       cfg.get('each'),         False)
    no_db  = _resolve(args.no_db,      cfg.get('no_db'),        False)
    manifest_arg   = args.manifest    or cfg.get('manifest')    # None → auto
    template_arg   = args.template    or cfg.get('template')    # None → auto
    profiles_dir_arg = args.profiles_dir or cfg.get('profiles_dir')  # None → auto
    profile_arg    = args.profile     or cfg.get('profile')     # None → auto
    target_arg     = args.target      or cfg.get('target')      # None → auto

    template_path  = Path(template_arg).resolve() if template_arg else template_path_early
    manifest_path  = Path(manifest_arg).resolve() if manifest_arg else project_dir / 'target' / 'manifest.json'
    profiles_dir   = Path(profiles_dir_arg).resolve() if profiles_dir_arg else Path.home() / '.dbt'

    dbt_project  = _read_dbt_project(project_dir)
    profile_name = profile_arg or dbt_project.get('profile', '<not set>')

    # --- --info mode ---
    if args.info:
        config_path = project_dir / CONFIG_FILE
        print(f"project-dir  : {project_dir}")
        print(f"config       : {config_path}  {'(exists)' if config_path.exists() else '(missing)'}")
        print(f"template     : {template_path}  {'(exists)' if template_path.exists() else '(missing)'}")
        print(f"manifest     : {manifest_path}  {'(exists)' if manifest_path.exists() else '(missing)'}")
        print(f"profiles-dir : {profiles_dir}  {'(exists)' if profiles_dir.exists() else '(missing)'}")
        print(f"profile      : {profile_name}")
        print(f"target       : {target_arg or '(default)'}")
        print(f"each         : {each}")
        print(f"no-db        : {no_db}")
        return

    # --- --debug-db mode ---
    if args.debug_db:
        resolved_profile = profile_arg or dbt_project.get('profile')
        if not resolved_profile:
            print(
                "Error: no profile name found. Pass --profile or set 'profile' in dbt_project.yml.",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            from dbt_schemify.db_connector import read_connection_config, debug_connection
            db_config = read_connection_config(resolved_profile, target_arg, profiles_dir_arg)
            print(f"Profile      : {resolved_profile}")
            print(f"Target       : {target_arg or '(default)'}")
            print(f"Profiles dir : {profiles_dir}")
            print()
            debug_connection(db_config)
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        return

    # --- Template ---
    if not template_path.exists():
        print(f"Error: template not found: {template_path}", file=sys.stderr)
        print("Run 'schemify --init' to create a default template.", file=sys.stderr)
        sys.exit(1)

    with open(template_path) as f:
        template = yaml.safe_load(f) or {}

    # --- Manifest ---
    if not manifest_path.exists():
        print(
            f"Error: manifest.json not found at {manifest_path}.\n"
            "Run 'dbt compile' (or 'dbt run') first to generate it.",
            file=sys.stderr,
        )
        sys.exit(1)
    manifest_nodes = _read_manifest_nodes(manifest_path)

    # --- Filter models ---
    if args.select:
        manifest_nodes = _apply_selector(manifest_nodes, args.select)
        if not manifest_nodes:
            print(f"Warning: no models matched selector: {args.select}", file=sys.stderr)

    # --- Compute output groups (before DB fetch, so user confirms first) ---
    template_model = ModelNode(**(template.get('models') or [{}])[0])

    if args.schema:
        mode = 'explicit'
        groups = {Path(args.schema): manifest_nodes}
    elif each or len(manifest_nodes) == 1:
        mode = 'model'
        groups = _group_nodes_by_model(manifest_nodes, project_dir)
    else:
        mode = 'dir'
        groups = _group_nodes_by_dir(manifest_nodes, project_dir)

    if not groups:
        print("No models found to process.", file=sys.stderr)
        sys.exit(1)

    # --- Confirmation prompt ---
    print("The following schema files will be created or updated:")
    for schema_path, nodes in groups.items():
        model_names = ', '.join(n['name'] for n in nodes)
        print(f"  {schema_path}  ({model_names})")
    if not _confirm("Proceed?", auto_yes=args.yes):
        print("Aborted.")
        sys.exit(0)

    # --- Pattern conflict check ---
    if mode in ('dir', 'model'):
        conflicts = _check_schema_pattern_conflicts(groups, mode)
        if conflicts:
            print("Warning: schema pattern mismatch detected:")
            for dir_path, desc in conflicts:
                print(f"  {dir_path}: {desc}")
            mode_hint = "each: false" if mode == 'dir' else "each: true"
            print(f"Tip: update '{mode_hint}' in {project_dir / CONFIG_FILE} to match your existing files.")
            if not _confirm("Continue anyway?", auto_yes=args.yes):
                print("Aborted.")
                sys.exit(0)

    # --- DB columns ---
    db_cols_by_model = {}
    if not no_db:
        resolved_profile = profile_arg or dbt_project.get('profile')

        if not resolved_profile:
            print(
                "Warning: no profile name found. Pass --profile or set 'profile' in dbt_project.yml. "
                "Skipping DB connection.",
                file=sys.stderr,
            )
        else:
            try:
                from dbt_schemify.db_connector import read_connection_config
                db_config = read_connection_config(resolved_profile, target_arg, profiles_dir_arg)
                print(f"Connecting to {db_config.get('type')} ({resolved_profile}/{target_arg or 'default'})...")
                db_cols_by_model = _fetch_db_columns(manifest_nodes, db_config)
            except Exception as exc:
                print(f"Warning: DB connection failed — {exc}", file=sys.stderr)
                print("Proceeding without column information.", file=sys.stderr)

    # --- Write ---
    for schema_path, nodes in groups.items():
        _write_schema(schema_path, nodes, template_model, db_cols_by_model)


if __name__ == '__main__':
    main()
