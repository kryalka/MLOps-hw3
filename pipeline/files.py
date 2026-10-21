import json
import shutil
from pathlib import Path


def atomic_write(path, writer):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    writer(temporary)
    temporary.replace(path)


def write_json(path, payload):
    atomic_write(path, lambda p: p.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    ))


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_frame(path, frame):
    atomic_write(path, lambda p: frame.to_parquet(p, index=False))


def copy_atomic(source, destination):
    atomic_write(destination, lambda p: shutil.copyfile(source, p))
