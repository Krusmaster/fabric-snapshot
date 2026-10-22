"""Snapshot of what is in a workspace, and the diff between two snapshots.

A snapshot is small and boring on purpose: per table, the column list (name + type), the row
count if you asked for it, and the last-modified time if the engine knows it; per workspace, the
item inventory. Two snapshots diff into: tables added / removed / schema changed / rows changed /
touched, items added / removed, and — the part you actually want — which *protected* tables
changed. "0 changes in 15 workspaces and 965 tables, all 22 protected tables untouched" is one
line, and it is the line that makes an agent run, a migration or a night of pipelines auditable.

Pure Python here. Collectors (Spark, SQL endpoint, Fabric REST) live in collect.py.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TableState:
    workspace: str              # workspace id or name — whatever you collected with
    database: str               # lakehouse / warehouse / SQL database name
    schema: str
    table: str
    columns: tuple[tuple[str, str], ...]        # ((name, type), ...) in ordinal order
    row_count: int | None = None
    last_modified: str | None = None            # ISO string if the engine reports it (Delta: DESCRIBE DETAIL)

    @property
    def key(self) -> str:
        return f"{self.workspace}/{self.database}/{self.schema}/{self.table}"

    @property
    def schema_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.columns).encode()).hexdigest()[:12]


@dataclass
class Snapshot:
    taken_at: str
    tables: dict[str, TableState] = field(default_factory=dict)
    items: dict[str, dict] = field(default_factory=dict)      # item id -> {"workspace", "name", "type"}
    scope: dict = field(default_factory=dict)                 # what was asked for (workspaces, databases, count=...)

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def add(self, t: TableState) -> None:
        self.tables[t.key] = t

    @property
    def workspaces(self) -> set[str]:
        return {t.workspace for t in self.tables.values()} | {i["workspace"] for i in self.items.values()}

    def summary(self) -> str:
        return f"{len(self.workspaces)} workspaces, {len(self.items)} items, {len(self.tables)} tables ({self.taken_at})"

    # --- persistence -------------------------------------------------------------------------
    def to_dict(self) -> dict:
        return {"version": SCHEMA_VERSION, "taken_at": self.taken_at, "scope": self.scope, "items": self.items,
                "tables": [asdict(t) | {"columns": [list(c) for c in t.columns]} for t in self.tables.values()]}

    @classmethod
    def from_dict(cls, d: dict) -> "Snapshot":
        s = cls(d["taken_at"], scope=d.get("scope", {}), items=d.get("items", {}))
        for t in d["tables"]:
            t = dict(t)
            t["columns"] = tuple(tuple(c) for c in t["columns"])
            s.add(TableState(**t))
        return s

    def save(self, path: str | Path) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=1), encoding="utf-8")
        return p

    @classmethod
    def load(cls, path: str | Path) -> "Snapshot":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


@dataclass
class Change:
    key: str
    kind: str                   # added | removed | schema | rows | touched
    detail: str = ""
    protected: bool = False


@dataclass
class Diff:
    before: str
    after: str
    changes: list[Change] = field(default_factory=list)
    items_added: list[dict] = field(default_factory=list)
    items_removed: list[dict] = field(default_factory=list)
    workspaces: int = 0
    tables: int = 0
    protected_total: int = 0

    @property
    def is_clean(self) -> bool:
        return not self.changes and not self.items_added and not self.items_removed

    @property
    def protected_violations(self) -> list[Change]:
        return [c for c in self.changes if c.protected]

    def summary(self) -> str:
        n = len(self.changes)
        head = f"{n} table change{'s' if n != 1 else ''} in {self.workspaces} workspaces and {self.tables} tables"
        if self.items_added or self.items_removed:
            head += f"; items +{len(self.items_added)} / -{len(self.items_removed)}"
        if self.protected_total:
            v = len(self.protected_violations)
            head += f"; {self.protected_total} protected tables " + ("untouched" if v == 0 else f"— {v} CHANGED")
        return head

    def markdown(self) -> str:
        out = [f"**{self.summary()}**", "", f"before `{self.before}` → after `{self.after}`", ""]
        if self.protected_violations:
            out += ["### Protected tables changed", ""] + [f"- `{c.key}` — {c.kind}: {c.detail}" for c in self.protected_violations] + [""]
        if self.changes:
            out += ["| table | change | detail |", "| --- | --- | --- |"]
            out += [f"| `{c.key}` | {c.kind} | {c.detail} |" for c in self.changes if not c.protected]
            out.append("")
        for label, items in (("Items added", self.items_added), ("Items removed", self.items_removed)):
            if items:
                out += [f"### {label}", ""] + [f"- {i['type']} `{i['name']}` ({i['workspace']})" for i in items] + [""]
        return "\n".join(out).rstrip() + "\n"

    def exit_code(self) -> int:
        """0 clean, 1 changes, 2 a protected table changed."""
        return 2 if self.protected_violations else (0 if self.is_clean else 1)


def _is_protected(key: str, patterns: list[str]) -> bool:
    table = key.rsplit("/", 1)[-1]
    return any(fnmatch.fnmatch(table.lower(), p.lower()) or fnmatch.fnmatch(key.lower(), p.lower()) for p in patterns)


def diff(before: Snapshot, after: Snapshot, protect: list[str] | None = None, ignore_touched: bool = False) -> Diff:
    """protect: glob patterns on the table name (or the full key) that must not change — `*_prod`, `dbo.dim_*`.
    ignore_touched: don't report tables whose last_modified moved but columns and rows are the same."""
    protect = protect or []
    d = Diff(before.taken_at, after.taken_at)
    keys = set(before.tables) | set(after.tables)
    d.workspaces = len(before.workspaces | after.workspaces)
    d.tables = len(keys)
    d.protected_total = sum(1 for k in keys if _is_protected(k, protect))

    for k in sorted(keys):
        b, a = before.tables.get(k), after.tables.get(k)
        prot = _is_protected(k, protect)
        if b is None:
            d.changes.append(Change(k, "added", f"{len(a.columns)} columns" + (f", {a.row_count} rows" if a.row_count is not None else ""), prot))
            continue
        if a is None:
            d.changes.append(Change(k, "removed", "", prot))
            continue
        if b.columns != a.columns:
            bc, ac = dict(b.columns), dict(a.columns)
            added = [c for c in ac if c not in bc]
            removed = [c for c in bc if c not in ac]
            retyped = [f"{c}: {bc[c]}→{ac[c]}" for c in bc if c in ac and bc[c] != ac[c]]
            parts = ([f"+{','.join(added)}"] if added else []) + ([f"-{','.join(removed)}"] if removed else []) + retyped
            d.changes.append(Change(k, "schema", " ".join(parts) or "column order", prot))
            continue
        if b.row_count is not None and a.row_count is not None and b.row_count != a.row_count:
            d.changes.append(Change(k, "rows", f"{b.row_count} → {a.row_count} ({a.row_count - b.row_count:+d})", prot))
            continue
        if not ignore_touched and b.last_modified and a.last_modified and b.last_modified != a.last_modified:
            d.changes.append(Change(k, "touched", f"last modified {b.last_modified} → {a.last_modified}, same columns" +
                                    ("" if a.row_count is None else " and rows"), prot))

    for iid, it in after.items.items():
        if iid not in before.items:
            d.items_added.append(it)
    for iid, it in before.items.items():
        if iid not in after.items:
            d.items_removed.append(it)
    return d
