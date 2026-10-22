import json

import pytest

from fabric_snapshot import Snapshot, TableState, diff, take
from fabric_snapshot.cli import main


def T(table, cols, rows=None, last=None, db="lh_sales", ws="ws1"):
    return TableState(ws, db, "dbo", table, tuple(cols), rows, last)


@pytest.fixture
def before():
    s = Snapshot("2026-10-21T19:53:00+00:00")
    s.add(T("orders", [("order_id", "int"), ("customer_id", "int")], 100, "2026-10-21T09:07:00"))
    s.add(T("customers", [("customer_id", "int"), ("name", "string")], 10, "2026-10-21T09:07:00"))
    s.add(T("stock_prod", [("plant", "string"), ("qty", "decimal(15,2)")], 500, "2026-10-21T09:07:00"))
    s.add(T("old_table", [("id", "int")], 1))
    s.items = {"i1": {"workspace": "ws1", "name": "lh_sales", "type": "Lakehouse"}}
    return s


def test_clean_diff(before):
    after = Snapshot.from_dict(before.to_dict())
    d = diff(before, after, protect=["*_prod"])
    assert d.is_clean and d.exit_code() == 0
    assert d.summary() == "0 table changes in 1 workspaces and 4 tables; 1 protected tables untouched"


def test_every_kind_of_change(before):
    after = Snapshot("2026-10-21T20:00:00+00:00", items=dict(before.items))
    after.add(T("orders", [("order_id", "int"), ("customer_id", "int"), ("status", "string")], 100, "2026-10-21T19:55:00"))  # schema
    after.add(T("customers", [("customer_id", "int"), ("name", "string")], 12, "2026-10-21T19:56:00"))                       # rows
    after.add(T("stock_prod", [("plant", "string"), ("qty", "decimal(15,2)")], 500, "2026-10-21T19:57:00"))                   # touched, protected
    after.add(T("staging_new", [("a", "int")], 0))                                                                           # added
    after.items["i2"] = {"workspace": "ws1", "name": "nb_agent", "type": "Notebook"}                                         # item added
    d = diff(before, after, protect=["*_prod"])
    kinds = {c.key.rsplit("/", 1)[-1]: (c.kind, c.detail) for c in d.changes}
    assert kinds["orders"] == ("schema", "+status")
    assert kinds["customers"] == ("rows", "10 → 12 (+2)")
    assert kinds["stock_prod"][0] == "touched" and d.protected_violations[0].key.endswith("stock_prod")
    assert kinds["staging_new"] == ("added", "1 columns, 0 rows")
    assert kinds["old_table"] == ("removed", "")
    assert [i["name"] for i in d.items_added] == ["nb_agent"] and d.items_removed == []
    assert d.exit_code() == 2
    assert "1 protected tables — 1 CHANGED" in d.summary()
    md = d.markdown()
    assert "### Protected tables changed" in md and "`ws1/lh_sales/dbo/stock_prod`" in md and "### Items added" in md


def test_schema_detail_and_retype(before):
    after = Snapshot.from_dict(before.to_dict())
    after.add(T("orders", [("order_id", "bigint"), ("customer_nr", "int")], 100, "2026-10-21T09:07:00"))
    d = diff(before, after)
    assert d.changes[0].detail == "+customer_nr -customer_id order_id: int→bigint"


def test_ignore_touched_and_protect_by_full_key(before):
    after = Snapshot.from_dict(before.to_dict())
    after.add(T("stock_prod", [("plant", "string"), ("qty", "decimal(15,2)")], 500, "2026-10-21T23:00:00"))
    assert diff(before, after, ignore_touched=True).is_clean
    d = diff(before, after, protect=["ws1/lh_sales/dbo/stock_*"])
    assert d.protected_total == 1 and d.exit_code() == 2


def test_roundtrip_json(tmp_path, before):
    p = before.save(tmp_path / "s.json")
    loaded = Snapshot.load(p)
    assert loaded.to_dict() == before.to_dict()
    assert json.loads(p.read_text())["version"] == 1


def test_collect_sql_with_fake_execute():
    rows = {
        "INFORMATION_SCHEMA": [("orders", "order_id", "int", 1), ("orders", "total", "decimal", 2), ("dim", "k", "varchar", 1)],
    }

    def execute(sql):
        if "INFORMATION_SCHEMA" in sql:
            return rows["INFORMATION_SCHEMA"]
        if "COUNT(*)" in sql:
            return [{"n": 42 if "[orders]" in sql else 3}]
        raise AssertionError(sql)

    snap = take(execute=execute, database="wh", schema="dbo", count=True)
    assert snap.tables["/wh/dbo/orders"].columns == (("order_id", "int"), ("total", "decimal"))
    assert snap.tables["/wh/dbo/orders"].row_count == 42 and snap.tables["/wh/dbo/dim"].row_count == 3
    assert "2 tables" in snap.summary()


def test_cli_diff_exit_codes(tmp_path, before):
    b = before.save(tmp_path / "b.json")
    after = Snapshot.from_dict(before.to_dict())
    after.add(T("stock_prod", [("plant", "string"), ("qty", "decimal(15,2)")], 501, "2026-10-21T23:00:00"))
    a = after.save(tmp_path / "a.json")
    assert main(["diff", str(b), str(a)]) == 1
    assert main(["diff", str(b), str(a), "--protect", "*_prod", "--markdown", str(tmp_path / "d.md")]) == 2
    assert "CHANGED" in (tmp_path / "d.md").read_text()
    assert main(["diff", str(b), str(b)]) == 0
