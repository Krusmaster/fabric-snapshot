# fabric-snapshot

**What was in the workspace before, what is in it after, and the one line between.**

**The problem.** Something ran on your Fabric workspace — an agent, a migration, a night of
pipelines, a colleague with admin rights — and now you need to say what it changed. Not "it
looks fine". Which tables, which columns, how many rows, which items appeared. And the question
that actually matters: *did it touch anything it was not allowed to touch?*

Fabric has no built-in answer. The lineage view shows relationships, not changes. The monitoring
hub shows runs, not their effect. So people eyeball the lakehouse and say "looks the same".

**What this does.** Takes a snapshot — per table the column list, the row count if you ask for
it, the last-modified time if the engine reports it; per workspace the item inventory — and diffs
two snapshots into added / removed / schema changed / rows changed / touched, plus items. Tables
you mark as *protected* get their own line.

```python
import fabric_snapshot as fs

before = fs.take(spark=spark, schemas=["dbo", "shop"], lakehouse="lh_sales", count=True)
before.save("Files/snapshots/before.json")

# ... the agent runs, the migration runs, the night passes ...

after = fs.take(spark=spark, schemas=["dbo", "shop"], lakehouse="lh_sales", count=True)
d = fs.diff(before, after, protect=["*_prod", "dim_*"])
print(d.summary())
```

```
0 table changes in 2 workspaces and 965 tables; 22 protected tables untouched
```

That line is what makes an agent experiment permissible, and what you paste into the ticket when
it was not an agent. When there *are* changes, `d.markdown()` lists them, protected ones first,
and `d.exit_code()` is 0 / 1 / 2 for clean / changed / a protected table changed — so it works as
a pipeline gate.

## Three ways in

| You have | Call |
| --- | --- |
| A Fabric notebook | `fs.take(spark=spark, schemas=["dbo", "shop"], lakehouse="lh_sales", count=True)` — columns, Delta `lastModified`, optional COUNT(*). On a schema-enabled lakehouse each schema is a Spark database; pass the schema names, not `lakehouse.schema` |
| A SQL connection (endpoint, warehouse, SQL database) | `fs.take(execute=run_sql, database="wh", schema="dbo", count=True)` |
| Just the item inventory | `fs.take(workspaces=["<ws id>"], token=token)` — needs `requests`, `pip install fabric-snapshot[rest]` |

They compose: `take(spark=..., workspaces=[...], token=...)` is tables and items in one snapshot.
`count=True` is a full `COUNT(*)` per table and is the slow part (13 s for 9 small tables in a warm
session, 5 s without); without it you still catch every schema change and, on Delta, every write.
Parquet tables carry no last-modified, so only the row count sees a change there.

## CLI

```
fabric-snapshot take --out before.json --odbc "<connection string>" --database wh --schema dbo --count
fabric-snapshot diff before.json after.json --protect "*_prod" --markdown report.md
```

Exit code 2 means a protected table changed. Put it in the pipeline after the step you do not
fully trust.

## Why it is this small

A snapshot is JSON you can read, diff with `git diff`, and keep for a year. No database, no
service, no agent. The row count and the column list are what a person would check by hand; this
only makes sure they check all 965.

## How you know it is right

Take two snapshots ten seconds apart with nothing running. The diff must be empty. Then append one
row to one table: the diff must name that table and nothing else.

## Status

v0.1.1 ran in a Fabric sandbox on a schema-enabled lakehouse (2026-10-22, [issue #1](https://github.com/Krusmaster/fabric-snapshot/issues/1)):
schemas passed as `schemas=`, a quiet ten seconds diffs empty, a one-row append names that table
alone (+1), a created item shows in `items_added`. Delta tables carry last-modified; parquet
tables only row counts. Shortcuts not yet run. The first run also found that keys were written as
schema/dbo/table — fixed in v0.1.1.

## Works on

| | |
| --- | --- |
| Fabric notebook (Spark) | `spark=` |
| Databricks | `spark=` — plain Spark catalog + `DESCRIBE DETAIL` |
| Any SQL store | `execute=` |
| Python | 3.10+, no required dependencies |

## License

MIT
