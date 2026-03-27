# sitecustomize.py - carregado automaticamente pelo Python ao iniciar
# Patch: dbt-spark não passa database ao PyHive; injeta database=creds.schema
def _apply_dbt_spark_patch():
    import re
    import dbt.adapters.spark.connections as conn_mod
    from dbt.adapters.spark.connections import SparkConnectionMethod

    _original_open = conn_mod.SparkConnectionManager.open

    def _patched_open(cls, connection):
        creds = connection.credentials
        if creds.method == SparkConnectionMethod.THRIFT and creds.schema:
            from pyhive import hive

            _orig = hive.connect

            def _hive_connect_with_db(*args, **kwargs):
                # PyHive expects only namespace/database (no catalog prefix).
                # If schema comes as catalog.schema, keep only final namespace.
                schema_name = str(creds.schema or "raw").split(".")[-1]
                kwargs["database"] = schema_name
                conn = _orig(*args, **kwargs)

                # Workaround for Iceberg v2:
                # "SHOW TABLE EXTENDED is not supported for v2 tables"
                # dbt-spark uses SHOW TABLE EXTENDED during relation introspection.
                # If that fails, fallback to SHOW TABLES and adapt row shape.
                try:
                    _orig_cursor = conn.cursor

                    def _cursor_wrapper(*cargs, **ckw):
                        cur = _orig_cursor(*cargs, **ckw)
                        _orig_execute = cur.execute

                        def _execute_wrapper(query, *eargs, **ekw):
                            try:
                                return _orig_execute(query, *eargs, **ekw)
                            except Exception as e:
                                q = str(query or "")
                                msg = str(e).lower()
                                if (
                                    re.search(r"show\s+table\s+extended", q, re.I)
                                    and "not supported for v2 tables" in msg
                                ):
                                    m = re.search(
                                        r"show\s+table\s+extended\s+in\s+(\S+)\s+like\s+(.+)",
                                        q,
                                        re.I,
                                    )
                                    if m:
                                        schema = m.group(1)
                                        like_expr = m.group(2).strip()
                                        alt_query = f"SHOW TABLES IN {schema} LIKE {like_expr}"
                                        res = _orig_execute(alt_query, *eargs, **ekw)

                                        def _patch_rows(fn):
                                            def _wrapped(*fargs, **fkw):
                                                rows = fn(*fargs, **fkw)
                                                if rows is None:
                                                    return rows
                                                if isinstance(rows, tuple):
                                                    return rows + ("",) if len(rows) == 3 else rows
                                                if isinstance(rows, list):
                                                    out = []
                                                    for r in rows:
                                                        if isinstance(r, tuple) and len(r) == 3:
                                                            out.append(r + ("",))
                                                        else:
                                                            out.append(r)
                                                    return out
                                                return rows

                                            return _wrapped

                                        if hasattr(cur, "fetchall"):
                                            cur.fetchall = _patch_rows(cur.fetchall)
                                        if hasattr(cur, "fetchone"):
                                            cur.fetchone = _patch_rows(cur.fetchone)
                                        return res
                                raise

                        cur.execute = _execute_wrapper
                        return cur

                    conn.cursor = _cursor_wrapper
                except Exception:
                    pass

                return conn

            hive.connect = _hive_connect_with_db
            try:
                return _original_open.__func__(cls, connection)
            finally:
                hive.connect = _orig
        return _original_open.__func__(cls, connection)

    conn_mod.SparkConnectionManager.open = classmethod(_patched_open)


_apply_dbt_spark_patch()
