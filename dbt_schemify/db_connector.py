import yaml
from pathlib import Path

_SECRET_KEYS = {'password', 'private_key_passphrase', 'token', 'refresh_token'}


def _mask_secrets(config):
    """Return a copy of config with secret fields replaced by '***'."""
    return {k: ('***' if k in _SECRET_KEYS and v else v) for k, v in config.items()}


def find_profiles_yml(profiles_dir=None):
    """Locate profiles.yml from explicit dir, default dbt home, or cwd."""
    candidates = []
    if profiles_dir:
        candidates.append(Path(profiles_dir) / 'profiles.yml')
    candidates.append(Path.home() / '.dbt' / 'profiles.yml')
    candidates.append(Path('profiles.yml'))

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "profiles.yml not found. Tried: ~/.dbt/profiles.yml, ./profiles.yml. "
        "Use --profiles-dir to specify its location."
    )


def read_connection_config(profile_name, target=None, profiles_dir=None):
    """Return the connection dict for the given profile + target."""
    path = find_profiles_yml(profiles_dir)
    with open(path) as f:
        profiles = yaml.safe_load(f)

    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found in {path}")

    profile = profiles[profile_name]
    target = target or profile.get('target')
    outputs = profile.get('outputs', {})

    if target not in outputs:
        raise ValueError(f"Target '{target}' not found in profile '{profile_name}'")

    return outputs[target]


def debug_connection(config):
    """Print masked connection config and test the connection."""
    adapter = config.get('type', '').lower()
    print(f"Adapter  : {adapter}")
    for k, v in _mask_secrets(config).items():
        if k != 'type' and v is not None:
            print(f"  {k}: {v}")
    print()

    _test = {
        'postgres':  _test_postgres,
        'redshift':  _test_postgres,
        'snowflake': _test_snowflake,
        'bigquery':  _test_bigquery,
        'duckdb':    _test_duckdb,
    }
    if adapter not in _test:
        print(f"Connection test not supported for adapter '{adapter}'.")
        return
    try:
        print("Testing connection...")
        _test[adapter](config)
        print("Connection OK.")
    except Exception as exc:
        print(f"Connection FAILED: {exc}")


def _test_postgres(config):
    try:
        import psycopg2
    except ImportError:
        raise ImportError("Install psycopg2: pip install psycopg2-binary")
    conn = psycopg2.connect(
        host=config.get('host', 'localhost'),
        port=config.get('port', 5432),
        dbname=config.get('dbname') or config.get('database'),
        user=config.get('user'),
        password=config.get('password'),
    )
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
    conn.close()


def _test_snowflake(config):
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError(
            "Install snowflake-connector-python: pip install snowflake-connector-python"
        )
    connect_kwargs = {
        'account':   config.get('account'),
        'user':      config.get('user'),
        'warehouse': config.get('warehouse'),
        'database':  config.get('database'),
        'schema':    config.get('schema'),
        'role':      config.get('role'),
    }
    if config.get('authenticator'):
        connect_kwargs['authenticator'] = config['authenticator']
    elif config.get('private_key_path'):
        connect_kwargs['private_key_path'] = config['private_key_path']
        if config.get('private_key_passphrase'):
            connect_kwargs['private_key_passphrase'] = config['private_key_passphrase']
    elif config.get('password'):
        connect_kwargs['password'] = config['password']
    conn = snowflake.connector.connect(**connect_kwargs)
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
    conn.close()


def _test_bigquery(config):
    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "Install google-cloud-bigquery: pip install google-cloud-bigquery"
        )
    client = bigquery.Client(project=config.get('project'))
    list(client.list_datasets())


def _test_duckdb(config):
    try:
        import duckdb
    except ImportError:
        raise ImportError("Install duckdb: pip install duckdb")
    conn = duckdb.connect(config.get('path', ':memory:'))
    conn.execute('SELECT 1')
    conn.close()


def get_columns(config, database, schema, table):
    """Fetch column list from the database for a model table."""
    adapter = config.get('type', '').lower()
    dispatch = {
        'postgres': _columns_postgres,
        'redshift': _columns_postgres,
        'snowflake': _columns_snowflake,
        'bigquery': _columns_bigquery,
        'duckdb': _columns_duckdb,
    }
    if adapter not in dispatch:
        raise ValueError(
            f"Unsupported adapter '{adapter}'. "
            f"Supported: {', '.join(dispatch)}"
        )
    return dispatch[adapter](config, database, schema, table)


def _columns_postgres(config, database, schema, table):
    try:
        import psycopg2
    except ImportError:
        raise ImportError("Install psycopg2: pip install psycopg2-binary")

    if not schema:
        raise ValueError(
            f"No schema found in manifest for model '{table}'. "
            "Run 'dbt compile' to refresh the manifest."
        )

    conn = psycopg2.connect(
        host=config.get('host', 'localhost'),
        port=config.get('port', 5432),
        dbname=database or config.get('dbname') or config.get('database'),
        user=config.get('user'),
        password=config.get('password'),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            return [{'name': row[0], 'data_type': row[1]} for row in cur.fetchall()]
    finally:
        conn.close()


def _columns_snowflake(config, database, schema, table):
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError(
            "Install snowflake-connector-python: pip install snowflake-connector-python"
        )

    if not schema:
        raise ValueError(
            f"No schema found in manifest for model '{table}'. "
            "Run 'dbt compile' to refresh the manifest."
        )
    if not database:
        raise ValueError(
            f"No database found in manifest for model '{table}'. "
            "Run 'dbt compile' to refresh the manifest."
        )

    connect_kwargs = {
        'account':   config.get('account'),
        'user':      config.get('user'),
        'warehouse': config.get('warehouse'),
        'database':  database,
        'schema':    schema,
        'role':      config.get('role'),
    }
    if config.get('authenticator'):
        connect_kwargs['authenticator'] = config['authenticator']
    elif config.get('private_key_path'):
        connect_kwargs['private_key_path'] = config['private_key_path']
        if config.get('private_key_passphrase'):
            connect_kwargs['private_key_passphrase'] = config['private_key_passphrase']
    elif config.get('password'):
        connect_kwargs['password'] = config['password']

    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'DESCRIBE TABLE "{database.upper()}"."{schema.upper()}"."{table.upper()}"'
            )
            return [{'name': row[0], 'data_type': row[1]} for row in cur.fetchall()]
    finally:
        conn.close()


def _columns_bigquery(config, database, schema, table):
    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "Install google-cloud-bigquery: pip install google-cloud-bigquery"
        )

    if not schema:
        raise ValueError(
            f"No dataset (schema) found in manifest for model '{table}'. "
            "Run 'dbt compile' to refresh the manifest."
        )

    # BigQuery: project = database from manifest, dataset = schema from manifest
    project = database or config.get('project')
    client = bigquery.Client(project=project)
    bq_table = client.get_table(f"{project}.{schema}.{table}")
    return [{'name': f.name, 'data_type': f.field_type} for f in bq_table.schema]


def _columns_duckdb(config, database, schema, table):
    try:
        import duckdb
    except ImportError:
        raise ImportError("Install duckdb: pip install duckdb")

    if not schema:
        raise ValueError(
            f"No schema found in manifest for model '{table}'. "
            "Run 'dbt compile' to refresh the manifest."
        )

    db_path = config.get('path', ':memory:')
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
        return [{'name': row[0], 'data_type': row[1]} for row in rows]
    finally:
        conn.close()
