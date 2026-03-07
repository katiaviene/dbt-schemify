"""
Core merge logic for dbt-schemify.

Three sources, with priority (highest first):
  1. existing schema.yml  — never overwrite values already there
  2. manifest.json        — use for 'schemify' sentinel fields
  3. .schemify.yml template — defines structure + static defaults

'schemify' is the sentinel string meaning "fill this from manifest/DB".
Fields absent from the template are not included in the output.
"""

SENTINEL = 'schemify'


def _empty(value):
    return value is None or value == '' or value == []


def _merge_dict(template_dict, existing_dict, source_dict):
    """Merge a dict-typed field (e.g. config, meta).
    Only keys present in template_dict are included in the output."""
    if not isinstance(template_dict, dict):
        return template_dict

    existing_dict = existing_dict or {}
    source_dict = source_dict or {}
    result = {}

    for key, tmpl_val in template_dict.items():
        existing_val = existing_dict.get(key)
        source_val = source_dict.get(key)

        if not _empty(existing_val):
            if isinstance(tmpl_val, dict) and isinstance(existing_val, dict):
                result[key] = _merge_dict(tmpl_val, existing_val, source_val or {})
            else:
                result[key] = existing_val
        elif tmpl_val == SENTINEL:
            if not _empty(source_val):
                result[key] = source_val
        elif isinstance(tmpl_val, dict):
            merged = _merge_dict(tmpl_val, {}, source_val or {})
            if merged:
                result[key] = merged
        elif not _empty(tmpl_val):
            result[key] = tmpl_val

    return result


def _merge_column(template_col, existing_col, db_col, source_col=None):
    """
    Build one column dict.
      template_col  – column prototype from .schemify.yml
      existing_col  – column dict from existing schema.yml (may be {})
      db_col        – {'name': ..., 'data_type': ...} from DB (may be {})
      source_col    – column dict from manifest (may be {}) — used for sentinel fields
    """
    existing_col = existing_col or {}
    db_col = db_col or {}
    source_col = source_col or {}
    result = {}

    for field, tmpl_val in template_col.items():
        if field == 'name':
            result['name'] = db_col.get('name') or source_col.get('name') or existing_col.get('name')
            continue

        existing_val = existing_col.get(field)

        if not _empty(existing_val):
            result[field] = existing_val
        elif tmpl_val == SENTINEL:
            # data_type: prefer DB, fall back to manifest
            if field == 'data_type':
                val = db_col.get('data_type') or source_col.get('data_type')
            else:
                val = source_col.get(field)
            if not _empty(val):
                result[field] = val
            # else: left for humans to fill in
        elif isinstance(tmpl_val, dict):
            merged = _merge_dict(tmpl_val, existing_val or {}, source_col.get(field) or {})
            if merged:
                result[field] = merged
        elif not _empty(tmpl_val):
            result[field] = tmpl_val

    return result


def _merge_columns(template_col, existing_cols, db_cols, manifest_cols=None):
    """
    Merge the column list for one model.
    Priority: DB columns → manifest columns → existing columns only.
    """
    existing_by_name = {c['name'].lower(): c for c in (existing_cols or [])}
    manifest_by_name = {c['name'].lower(): c for c in (manifest_cols or [])}
    result = []

    if db_cols:
        db_seen = set()
        for db_col in db_cols:
            key = db_col['name'].lower()
            db_seen.add(key)
            result.append(_merge_column(
                template_col,
                existing_by_name.get(key, {}),
                db_col,
                manifest_by_name.get(key, {}),
            ))
        # Preserve columns in existing schema that the DB didn't return
        for col in (existing_cols or []):
            if col['name'].lower() not in db_seen:
                result.append(col)

    elif manifest_cols:
        manifest_seen = set()
        for manifest_col in manifest_cols:
            key = manifest_col['name'].lower()
            manifest_seen.add(key)
            result.append(_merge_column(
                template_col,
                existing_by_name.get(key, {}),
                {},
                manifest_col,
            ))
        # Preserve existing columns not in manifest
        for col in (existing_cols or []):
            if col['name'].lower() not in manifest_seen:
                result.append(col)

    else:
        result = list(existing_cols or [])

    return result


def merge_model(template_model, existing_model, manifest_node, db_cols):
    """Build the final dict for one model."""
    existing_model = existing_model or {}
    manifest_node = manifest_node or {}
    result = {}

    result['name'] = manifest_node.get('name') or existing_model.get('name')

    template_col = None
    if template_model.get('columns'):
        template_col = template_model['columns'][0]

    for field, tmpl_val in template_model.items():
        if field == 'name':
            continue

        if field == 'columns':
            if template_col is not None:
                # Extract manifest columns (stored as a dict keyed by name)
                raw = manifest_node.get('columns', {})
                manifest_cols = list(raw.values()) if isinstance(raw, dict) else (raw or [])
                cols = _merge_columns(
                    template_col,
                    existing_model.get('columns', []),
                    db_cols,
                    manifest_cols,
                )
                if cols:
                    result['columns'] = cols
            continue

        existing_val = existing_model.get(field)
        source_val = manifest_node.get(field)

        if not _empty(existing_val):
            if isinstance(tmpl_val, dict) and isinstance(existing_val, dict):
                result[field] = _merge_dict(tmpl_val, existing_val, source_val or {})
            else:
                result[field] = existing_val
        elif tmpl_val == SENTINEL:
            if not _empty(source_val):
                result[field] = source_val
        elif isinstance(tmpl_val, dict):
            merged = _merge_dict(tmpl_val, existing_val or {}, source_val or {})
            if merged:
                result[field] = merged
        elif not _empty(tmpl_val):
            result[field] = tmpl_val

    return result


def merge_schema(template, existing_schema, manifest_nodes, db_cols_by_model):
    """Build the final schema dict, ready to be written to schema.yml."""
    template_model = (template.get('models') or [{}])[0]
    existing_models = {m['name']: m for m in existing_schema.get('models', [])}

    result_models = []
    processed = set()

    for node in manifest_nodes:
        name = node['name']
        merged = merge_model(
            template_model,
            existing_models.get(name, {}),
            node,
            db_cols_by_model.get(name, []),
        )
        result_models.append(merged)
        processed.add(name)

    # Keep models from existing schema that aren't in the manifest
    for name, existing in existing_models.items():
        if name not in processed:
            result_models.append(existing)

    return {
        'version': existing_schema.get('version', 2),
        'models': result_models,
    }
