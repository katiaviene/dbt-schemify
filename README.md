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

**1. Compile your dbt project** to get a manifest:

```bash
dbt compile
```

**2. Run schemify:**

```bash
schemify
```

If `.schemify.yml` doesn't exist yet, schemify creates one with sensible defaults — edit it, then re-run.

Reads `.schemify.yml`, `target/manifest.json`, and `~/.dbt/profiles.yml` automatically.
Writes `schema.yml` next to each model's SQL file (grouped by folder).

## Usage

```
schemify [options]

Options:
  --schema PATH          Write all models into a single schema.yml at PATH.
                         If omitted, a schema.yml is created next to each model's SQL file.
  --manifest PATH        Path to manifest.json
                         Default: <project-dir>/target/manifest.json
  --template PATH        Path to .schemify.yml template
                         Default: <project-dir>/.schemify.yml
  --project-dir DIR      dbt project root. Default: current directory
  --profile NAME         dbt profile name. Default: read from dbt_project.yml
  --target NAME          dbt target (e.g. dev, prod). Default: profile default
  --profiles-dir DIR     Directory containing profiles.yml. Default: ~/.dbt/
  -s / --select          Filter models by name or tag. Space-separated.
                         Examples: -s orders   -s tag:marketing   -s tag:finance orders
  --each                 Write one <model_name>.yml per model instead of one schema.yml per folder
  --no-db                Skip database connection; no column fetching
  --info                 Show resolved paths and configuration, then exit
```

> `dbt-schemify` also works as an alias for backward compatibility.

## Examples

```bash
# Auto-discover: write schema.yml next to every model's SQL file
schemify

# Check which paths schemify is using
schemify --info

# Only models with a specific tag (one schema.yml per directory)
schemify -s tag:marketing

# Only specific models by name
schemify -s orders customers

# Single model — automatically gets its own <model_name>.yml
schemify -s orders

# Mix names and tags
schemify -s tag:finance orders

# All matching models into one explicit file
schemify --schema models/marketing/schema.yml -s tag:marketing

# One file per model named after the model (e.g. orders.yml, customers.yml)
schemify --each

# Without DB connection (manifest data only)
schemify --no-db

# Custom paths
schemify \
  --manifest target/manifest.json \
  --template .schemify.yml
```

## Default template

If `.schemify.yml` is missing, schemify creates this template automatically:

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
