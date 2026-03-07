from dbt_schemify.dbt_ast import SENTINEL, _empty, Node, SchemaNode, ModelNode, ColumnNode


class SchemifyTransformer:
    """
    Merges template, existing schema, manifest, and DB column data into a new SchemaNode.

    Priority (highest first):
      1. Existing schema.yml — values already there are never overwritten
      2. manifest.json       — fills 'schemify' sentinel fields
      3. .schemify.yml template — defines structure + static defaults

    DB column data is used as the authoritative source for data_type.
    Fields absent from the template are not included in the output.
    """

    def __init__(self, template_model, existing_by_name, manifest_nodes, db_cols_by_model):
        """
        template_model    – ModelNode (prototype from .schemify.yml)
        existing_by_name  – dict: model name -> ModelNode (from existing schema.yml)
        manifest_nodes    – list of manifest node dicts (from manifest.json)
        db_cols_by_model  – dict: model name -> [{'name': ..., 'data_type': ...}]
        """
        self.template_model = template_model
        self.existing_by_name = existing_by_name
        self.manifest_nodes = manifest_nodes
        self.db_cols_by_model = db_cols_by_model

    def run(self, existing_version=2):
        """Return a SchemaNode with all models merged."""
        result_models = []
        processed = set()

        for manifest_node in self.manifest_nodes:
            name = manifest_node['name']
            existing = self.existing_by_name.get(name)
            db_cols = self.db_cols_by_model.get(name, [])
            merged = self._merge_model(existing, manifest_node, db_cols)
            result_models.append(merged)
            processed.add(name)

        # Preserve models from existing schema not in manifest
        for name, existing in self.existing_by_name.items():
            if name not in processed:
                result_models.append(existing)

        return SchemaNode(version=existing_version, models=result_models)

    def _merge_model(self, existing, manifest_node, db_cols):
        existing = existing or ModelNode()
        manifest_node = manifest_node or {}
        name = manifest_node.get('name') or existing.get('name')

        result_kwargs = {'name': name}

        # Get the column prototype from template
        tmpl_cols = self.template_model.get('columns') or []
        template_col = tmpl_cols[0] if tmpl_cols else None

        for field, tmpl_val in self.template_model.field_items():
            if field == 'name':
                continue

            if field == 'columns':
                if template_col is not None:
                    raw = manifest_node.get('columns', {})
                    manifest_cols = list(raw.values()) if isinstance(raw, dict) else (raw or [])
                    cols = self._merge_columns(
                        template_col,
                        existing.get('columns') or [],
                        db_cols,
                        manifest_cols,
                    )
                    if cols:
                        result_kwargs['columns'] = cols
                continue

            existing_val = existing.get(field)
            source_val = manifest_node.get(field)

            if not _empty(existing_val):
                if isinstance(tmpl_val, dict) and isinstance(existing_val, dict):
                    result_kwargs[field] = self._merge_dict(tmpl_val, existing_val, source_val or {})
                elif isinstance(tmpl_val, Node) and isinstance(existing_val, Node):
                    result_kwargs[field] = self._merge_dict(
                        tmpl_val.to_dict(), existing_val.to_dict(), source_val or {}
                    )
                else:
                    result_kwargs[field] = existing_val
            elif tmpl_val == SENTINEL:
                if not _empty(source_val):
                    result_kwargs[field] = source_val
            elif isinstance(tmpl_val, (dict, Node)):
                d = tmpl_val.to_dict() if isinstance(tmpl_val, Node) else tmpl_val
                merged = self._merge_dict(d, {}, source_val or {})
                if merged:
                    result_kwargs[field] = merged
            elif not _empty(tmpl_val):
                result_kwargs[field] = tmpl_val

        return ModelNode(**result_kwargs)

    def _merge_columns(self, template_col, existing_cols, db_cols, manifest_cols):
        def _name(col):
            return (col.get('name') if isinstance(col, Node) else col.get('name', '')).lower()

        existing_by_name = {_name(c): c for c in (existing_cols or [])}
        manifest_by_name = {c['name'].lower(): c for c in (manifest_cols or [])}
        result = []

        if db_cols:
            db_seen = set()
            for db_col in db_cols:
                key = db_col['name'].lower()
                db_seen.add(key)
                result.append(self._merge_column(
                    template_col,
                    existing_by_name.get(key),
                    db_col,
                    manifest_by_name.get(key, {}),
                ))
            # Preserve existing columns the DB didn't return
            for col in (existing_cols or []):
                if _name(col) not in db_seen:
                    result.append(col)

        elif manifest_cols:
            manifest_seen = set()
            for manifest_col in manifest_cols:
                key = manifest_col['name'].lower()
                manifest_seen.add(key)
                result.append(self._merge_column(
                    template_col,
                    existing_by_name.get(key),
                    {},
                    manifest_col,
                ))
            # Preserve existing columns not in manifest
            for col in (existing_cols or []):
                if _name(col) not in manifest_seen:
                    result.append(col)

        else:
            result = list(existing_cols or [])

        return result

    def _merge_column(self, template_col, existing_col, db_col, source_col):
        db_col = db_col or {}
        source_col = source_col or {}

        def _get(field):
            if existing_col is None:
                return None
            return existing_col.get(field) if isinstance(existing_col, Node) else existing_col.get(field)

        result_kwargs = {
            'name': db_col.get('name') or source_col.get('name') or _get('name')
        }

        for field, tmpl_val in template_col.field_items():
            if field == 'name':
                continue

            existing_val = _get(field)

            if not _empty(existing_val):
                result_kwargs[field] = existing_val
            elif tmpl_val == SENTINEL:
                val = db_col.get('data_type') if field == 'data_type' else source_col.get(field)
                if _empty(val) and field == 'data_type':
                    val = source_col.get('data_type')
                if not _empty(val):
                    result_kwargs[field] = val
            elif isinstance(tmpl_val, (dict, Node)):
                d = tmpl_val.to_dict() if isinstance(tmpl_val, Node) else tmpl_val
                merged = self._merge_dict(d, existing_val or {}, source_col.get(field) or {})
                if merged:
                    result_kwargs[field] = merged
            elif not _empty(tmpl_val):
                result_kwargs[field] = tmpl_val

        return ColumnNode(**result_kwargs)

    def _merge_dict(self, template_dict, existing_dict, source_dict):
        """Merge a dict-typed field. Only keys present in template are included."""
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
                    result[key] = self._merge_dict(tmpl_val, existing_val, source_val or {})
                else:
                    result[key] = existing_val
            elif tmpl_val == SENTINEL:
                if not _empty(source_val):
                    result[key] = source_val
            elif isinstance(tmpl_val, dict):
                merged = self._merge_dict(tmpl_val, {}, source_val or {})
                if merged:
                    result[key] = merged
            elif not _empty(tmpl_val):
                result[key] = tmpl_val

        return result
