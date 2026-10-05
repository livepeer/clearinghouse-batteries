#!/usr/bin/env python3
"""Compare complete logical database contents, including WAL-visible commits."""
import hashlib
import json
from pathlib import Path
import sqlite3
import shutil
import sys

DATABASES = ("clearinghouse.db", "minikafka_+meta.db",
             "minikafka_livepeer-signing_0.db", "signer-events.sqlite")

def fingerprint(path):
    if not path.is_file():
        raise RuntimeError(f"missing database: {path}")
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise RuntimeError(f"integrity_check failed: {path}")
        if db.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError(f"foreign_key_check failed: {path}")
        schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master "
                            "WHERE sql IS NOT NULL ORDER BY type,name").fetchall()
        result = {"schema": hashlib.sha256(repr(schema).encode()).hexdigest(), "tables": {}}
        for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            quoted = '"' + table.replace('"', '""') + '"'
            rows = sorted(repr(row) for row in db.execute(f"SELECT * FROM {quoted}"))
            result["tables"][table] = {"rows": len(rows), "sha256": hashlib.sha256("\n".join(rows).encode()).hexdigest()}
        return result

def snapshot(root):
    actual = {p.name for p in Path(root).iterdir() if p.suffix in (".db", ".sqlite")}
    if actual != set(DATABASES):
        raise RuntimeError(f"unexpected database set in {root}: {sorted(actual)}")
    return {name: fingerprint(Path(root) / name) for name in DATABASES}

if __name__ == "__main__":
    if sys.argv[1:] == ["compare"]:
        source, restored = snapshot("/data"), snapshot("/restore")
        if source != restored:
            raise SystemExit("FAIL: restored schemas or table contents differ from stopped source")
        print(json.dumps(restored, indent=2))
        print("PASS: all schemas, balances, checkpoints, Kafka records and outbox rows match")
    elif sys.argv[1:] == ["install"]:
        snapshot("/restore")
        if any(Path("/data").iterdir()):
            raise SystemExit("destination state volume must be empty")
        for name in DATABASES:
            destination = Path("/data") / name
            shutil.copyfile(Path("/restore") / name, destination)
            destination.chmod(0o600)
        if snapshot("/data") != snapshot("/restore"):
            raise SystemExit("installed recovery set differs")
        print("PASS: recovery set installed into empty state volume")
    elif sys.argv[1:] == ["check"]:
        print(json.dumps(snapshot("/restore"), indent=2))
    else:
        raise SystemExit("usage: databases.py compare|check|install")
