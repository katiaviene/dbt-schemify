"""
dbt-schemify CLI

Usage:
  dbt-schemify --schema models/example/schema.yml

All options:
  --schema         PATH   Path to schema.yml to generate/update (required)
  --manifest       PATH   Path to manifest.json (default: ./target/manifest.json)
  --template       PATH   Path to .schemify.yml template (default: ./.schemify.yml)
  --project-dir    DIR    dbt project root (default: current directory)
  --profile        NAME   dbt profile name (default: read from dbt_project.yml)
  --target         NAME   dbt target (default: profile default)
  --profiles-dir   DIR    Directory containing profiles.yml (default: ~/.dbt/)
  --models         NAME   Only process these model names (space-separated)
  --no-db                 Skip database connection; no column fetching
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

from dbt_schemify.schema_editor import CustomDumper
from dbt_schemify.merger import merge_schema


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


def main():
    parser = argparse.ArgumentParser(
        prog='dbt-schemify',
        description='Generate and update dbt schema.yml files.',
    )
    parser.add_argument('--schema', required=True,
                        help='Path to schema.yml to generate or update.')
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
    parser.add_argument('--models', nargs='+', metavar='MODEL',
                        help='Only process these model names.')
    parser.add_argument('--no-db', action='store_true',
                        help='Skip database connection and column fetching.')

    args = parser.parse_args()
    project_dir = Path(args.project_dir)

    # --- Template ---
    template_path = Path(args.template) if args.template else project_dir / '.schemify.yml'
    if not template_path.exists():
        print(f"Error: template not found: {template_path}", file=sys.stderr)
        print("Create a .schemify.yml in your project root or pass --template.", file=sys.stderr)
        sys.exit(1)
    with open(template_path) as f:
        template = yaml.safe_load(f) or {}

    # --- Manifest ---
    manifest_path = Path(args.manifest) if args.manifest else project_dir / 'target' / 'manifest.json'
    if not manifest_path.exists():
        print(
            f"Error: manifest.json not found at {manifest_path}.\n"
            "Run 'dbt compile' (or 'dbt run') first to generate it.",
            file=sys.stderr,
        )
        sys.exit(1)
    manifest_nodes = _read_manifest_nodes(manifest_path)

    # --- Filter models ---
    if args.models:
        keep = set(args.models)
        manifest_nodes = [n for n in manifest_nodes if n['name'] in keep]
        if not manifest_nodes:
            print(f"Warning: none of the specified models found in manifest: {args.models}", file=sys.stderr)

    # --- Existing schema ---
    schema_path = Path(args.schema)
    existing_schema = {}
    if schema_path.exists():
        with open(schema_path) as f:
            existing_schema = yaml.safe_load(f) or {}

    # --- DB columns ---
    db_cols_by_model = {}
    if not args.no_db:
        dbt_project = _read_dbt_project(project_dir)
        profile_name = args.profile or dbt_project.get('profile')

        if not profile_name:
            print(
                "Warning: no profile name found. Pass --profile or set 'profile' in dbt_project.yml. "
                "Skipping DB connection.",
                file=sys.stderr,
            )
        else:
            try:
                from dbt_schemify.db_connector import read_connection_config
                config = read_connection_config(profile_name, args.target, args.profiles_dir)
                print(f"Connecting to {config.get('type')} ({profile_name}/{args.target or 'default'})...")
                db_cols_by_model = _fetch_db_columns(manifest_nodes, config)
            except Exception as exc:
                print(f"Warning: DB connection failed — {exc}", file=sys.stderr)
                print("Proceeding without column information.", file=sys.stderr)

    # --- Merge & write ---
    result = merge_schema(template, existing_schema, manifest_nodes, db_cols_by_model)
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with open(schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(result, f, default_flow_style=False, Dumper=CustomDumper,
                  sort_keys=False, allow_unicode=True)
    print(f"Schema written to {schema_path}")


if __name__ == '__main__':
    main()
