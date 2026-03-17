from __future__ import annotations

import glob as _glob
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import S4DB


def compact(db: "S4DB") -> None:
    old_paths = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
    old_filenames = [os.path.basename(p) for p in old_paths]

    # Build latest view: key -> value or None (tombstone)
    latest: dict[str, str | None] = {}
    for path in old_paths:
        with open(path, "rb") as fh:
            data = fh.read()
        from ._format import iter_file_entries, FLAG_TOMBSTONE
        for _offset, _length, key, value, flags in iter_file_entries(data):
            if flags == FLAG_TOMBSTONE:
                latest[key] = None
            else:
                latest[key] = value

    live_entries = [(k, v) for k, v in latest.items() if v is not None]

    # Remove old local files before writing so _write_entries starts a new file
    for path in old_paths:
        os.remove(path)

    # Reset index entries (keep next_file_num so new files get fresh numbers)
    db._index.entries.clear()

    # Write compacted files
    if live_entries:
        db._write_entries([(k, v, False) for k, v in live_entries])

    # Delete old files from S3
    for filename in old_filenames:
        db.storage.delete(filename)
