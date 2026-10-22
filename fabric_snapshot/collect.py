"""Collectors: fill a Snapshot from Spark, from a SQL connection, or from the Fabric REST API.

    snap = take(spark=spark, schemas=["dbo", "shop"], lakehouse="lh_sales", count=True)   # inside a Fabric notebook
    snap = take(execute=run_sql, database="wh_main", schema="dbo", count=True)   # any DB-API / SQL endpoint
    snap = take(workspaces=["<ws id>", ...], token=token)                         # item inventory only, no tables

They compose: take(spark=..., workspaces=[...], token=...) gives tables + items in one snapshot.
Row counts are a full COUNT(*) per table — on a big lakehouse that is the slow part (13 s for 9
small tables warm, 5 s without); count=False keeps columns + last_modified, which already catches
schema drift and every Delta write. Parquet tables report no last_modified: only the row count sees
a change there.
"""
from __future__ import annotations

from .core import Snapshot, TableState

FABRIC = "https://api.fabric.microsoft.com"


def take(spark=None, schemas: list[str] | None = None, lakehouse: str = "lakehouse", execute=None,
         database: str | None = None, schema: str = "dbo", workspaces: list[str] | None = None,
         token: str | None = None, workspace_label: str = "", count: bool = False,
         databases: list[str] | None = None) -> Snapshot:
    """spark=: schemas= are the lakehouse's schemas (["dbo", "shop"]; on a schema-enabled lakehouse each is a Spark
    database); lakehouse= is the name that goes into the key. `databases=` is the old spelling of schemas=."""
    schemas = schemas or databases
    snap = Snapshot(Snapshot.now(), scope={"schemas": schemas, "lakehouse": lakehouse, "database": database,
                                           "schema": schema, "workspaces": workspaces, "count": count})
    if spark is not None:
        collect_spark(snap, spark, schemas, lakehouse=lakehouse, count=count, workspace=workspace_label)
    if execute is not None:
        collect_sql(snap, execute, database or "db", schema, count=count, workspace=workspace_label)
    if workspaces:
        if not token:
            raise ValueError("workspaces= needs token=")
        collect_items(snap, workspaces, token)
    return snap


def collect_spark(snap: Snapshot, spark, schemas: list[str] | None = None, lakehouse: str = "lakehouse",
                  count: bool = False, workspace: str = "") -> None:
    """schemas: ["dbo", "shop"] — the schema name is what listTables wants on a schema-enabled lakehouse; two- and
    three-part names fail. currentDatabase() returns an internal name there, so it never goes into a key."""
    dbs = schemas or ["dbo"]
    for db in dbs:
        for t in spark.catalog.listTables(db):
            if t.tableType and t.tableType.upper() == "TEMPORARY":
                continue
            cols = tuple((c.name, c.dataType) for c in spark.catalog.listColumns(t.name, db))
            full = f"{db}.{t.name}"
            last = None
            try:
                row = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0].asDict()
                lm = row.get("lastModified")
                last = lm.isoformat(timespec="seconds") if hasattr(lm, "isoformat") else (str(lm) if lm else None)
            except Exception:
                pass
            n = spark.table(full).count() if count else None
            snap.add(TableState(workspace, lakehouse, db, t.name, cols, n, last))


COLUMNS_SQL = """SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' ORDER BY TABLE_NAME, ORDINAL_POSITION"""


def collect_sql(snap: Snapshot, execute, database: str, schema: str = "dbo", count: bool = False,
                workspace: str = "") -> None:
    """execute(sql) -> rows (tuples or dict-like). Works on a SQL analytics endpoint, a warehouse, a SQL database."""
    by_table: dict[str, list[tuple[str, str]]] = {}
    for r in execute(COLUMNS_SQL.format(schema=schema)):
        r = _row(r, ["table_name", "column_name", "data_type", "ordinal_position"])
        by_table.setdefault(r["table_name"], []).append((r["column_name"], str(r["data_type"]).lower()))
    for table, cols in by_table.items():
        n = None
        if count:
            n = int(list(_row(next(iter(execute(f"SELECT COUNT(*) AS n FROM [{schema}].[{table}]"))), ["n"]).values())[0])
        snap.add(TableState(workspace, database, schema, table, tuple(cols), n, None))


def collect_items(snap: Snapshot, workspaces: list[str], token: str) -> None:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    for ws in workspaces:
        url = f"{FABRIC}/v1/workspaces/{ws}/items"
        while url:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            body = r.json()
            for it in body.get("value", []):
                snap.items[it["id"]] = {"workspace": ws, "name": it.get("displayName"), "type": it.get("type")}
            url = body.get("continuationUri")


def _row(row, names):
    if hasattr(row, "keys"):
        lower = {k.lower(): row[k] for k in row.keys()}
        return {n: lower.get(n) for n in names}
    if hasattr(row, "asDict"):
        return _row(row.asDict(), names)
    return dict(zip(names, list(row) + [None] * (len(names) - len(row))))
