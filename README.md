# dbt-schemify

Generate and update dbt `schema.yml` files automatically from a template, your dbt manifest, and live database columns.

## How it works

Three sources are merged in priority order (highest first):

1. **Existing `schema.yml`** — values already there are never overwritten
2. **`manifest.json`** — fills fields marked with the `schemify` sentinel
3. **`.schemify.yml` template** — defines which fields to include and their static defaults

The sentinel value `schemify` in the template means "auto-populate this field from the manifest or database".

## Installation

```bash
pip install dbt-schemify
```

With your database adapter:

```bash
pip install "dbt-schemify[snowflake]"
pip install "dbt-schemify[postgres]"
pip install "dbt-schemify[bigquery]"
pip install "dbt-schemify[duckdb]"
```

## Quick start

**1. Create a `.schemify.yml` template** in your dbt project root:

```yaml
version: '1.0'
models:
  - name: schemify
    description: schemify      # filled from manifest
    meta:
      owner: analytics         # static default applied to all models
    config:
      enabled: true            # static default
    columns:
      - name: schemify
        data_type: schemify    # filled from DB
        description: schemify  # left empty for humans to fill in
        meta:
          gdpr_tags: schemify
```

**2. Compile your dbt project** to get a manifest:

```bash
dbt compile
```

**3. Run dbt-schemify:**

```bash
dbt-schemify --schema models/my_folder/schema.yml
```

Reads `.schemify.yml`, `target/manifest.json`, and `~/.dbt/profiles.yml` automatically.

## Usage

```
dbt-schemify --schema PATH [options]

Options:
  --schema PATH          Path to schema.yml to generate or update (required)
  --manifest PATH        Path to manifest.json
                         Default: <project-dir>/target/manifest.json
  --template PATH        Path to .schemify.yml template
                         Default: <project-dir>/.schemify.yml
  --project-dir DIR      dbt project root. Default: current directory
  --profile NAME         dbt profile name. Default: read from dbt_project.yml
  --target NAME          dbt target (e.g. dev, prod). Default: profile default
  --profiles-dir DIR     Directory containing profiles.yml. Default: ~/.dbt/
  --models MODEL ...     Only process specific model names
  --no-db                Skip database connection; no column fetching
```

## Examples

```bash
# Generate schema without DB (manifest data only)
dbt-schemify --schema models/example/schema.yml --no-db

# Generate schema with DB columns
dbt-schemify --schema models/example/schema.yml --profile my_profile --target dev

# Only specific models
dbt-schemify --schema models/example/schema.yml --models my_model_a my_model_b --no-db

# Custom paths
dbt-schemify \
  --schema models/example/schema.yml \
  --manifest target/manifest.json \
  --template .schemify.yml
```

## Merge rules

| Situation | Result |
|-----------|--------|
| Field exists in `schema.yml` | **Kept as-is** |
| Field is `schemify` sentinel + value in manifest | Filled from manifest |
| Field is `schemify` sentinel + column `data_type` | Filled from DB |
| Field has a static value in template | Used as default |
| Field not in template | **Not included in output** |
| Column in DB but not in existing schema | Added using template column structure |
| Column in existing schema but not in DB | **Preserved** |
