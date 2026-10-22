"""fabric-snapshot CLI.

  fabric-snapshot take --out before.json --odbc "<conn>" --database wh --schema dbo [--count]
  fabric-snapshot take --out before.json --workspaces <ws1>,<ws2>            (items only; token via az login)
  fabric-snapshot diff before.json after.json [--protect "*_prod" --protect "dim_*"] [--markdown out.md]
     exit 0 = clean, 1 = changes, 2 = a protected table changed
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from .collect import take, FABRIC
from .core import Snapshot, diff


def _token() -> str:
    r = subprocess.run(["az", "account", "get-access-token", "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()


def cmd_take(a) -> int:
    execute = None
    if a.odbc:
        import pyodbc
        conn = pyodbc.connect(a.odbc)

        def execute(sql):
            cur = conn.cursor(); cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    ws = a.workspaces.split(",") if a.workspaces else None
    snap = take(execute=execute, database=a.database, schema=a.schema, workspaces=ws,
                token=_token() if ws else None, count=a.count)
    snap.save(a.out)
    print(snap.summary(), "->", a.out)
    return 0


def cmd_diff(a) -> int:
    d = diff(Snapshot.load(a.before), Snapshot.load(a.after), protect=a.protect, ignore_touched=a.ignore_touched)
    print(d.summary())
    for c in d.changes:
        print(("  ! " if c.protected else "  ") + f"{c.key}  {c.kind}  {c.detail}")
    if a.markdown:
        Path(a.markdown).write_text(d.markdown(), encoding="utf-8")
    return d.exit_code()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="fabric-snapshot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("take"); s.add_argument("--out", required=True); s.add_argument("--odbc"); s.add_argument("--database", default="db")
    s.add_argument("--schema", default="dbo"); s.add_argument("--workspaces"); s.add_argument("--count", action="store_true"); s.set_defaults(fn=cmd_take)
    s = sub.add_parser("diff"); s.add_argument("before"); s.add_argument("after"); s.add_argument("--protect", action="append", default=[])
    s.add_argument("--ignore-touched", action="store_true"); s.add_argument("--markdown"); s.set_defaults(fn=cmd_diff)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
