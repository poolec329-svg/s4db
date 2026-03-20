from __future__ import annotations

import glob as _glob
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import S4DB

from ._format import HEADER_SIZE, FLAG_TOMBSTONE, pack_file_header, unpack_file_header, stream_file_entries

_INDEX_FILENAME = "index.idx"


def compact(db: "S4DB") -> None:
    """Rewrites all data files to remove stale and deleted entries.

    Reads every entry from every existing data file and retains only live entries —
    those whose (file_num, offset) still matches the in-memory index. Tombstones and
    superseded values are dropped. Writes compacted output into new numbered files
    respecting db.max_file_size. After writing, updates the index and saves it, then
    removes the old local files and deletes the old S3 objects before uploading the
    new files and index.

    Mutates db._index in place. Raises if any I/O fails mid-compaction.
    """
    old_paths = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
    old_filenames = [os.path.basename(p) for p in old_paths]

    # Read file_num from each old file header
    old_file_nums: dict[str, int] = {}
    for path in old_paths:
        with open(path, "rb") as fh:
            _, file_num = unpack_file_header(fh.read(HEADER_SIZE))
        old_file_nums[path] = file_num

    new_file_num = db._index.next_file_num
    new_paths: list[str] = []
    out_fh = None
    cur_file_num: int = 0

    def open_new_file() -> None:
        """Closes the current output file (if any) and opens the next one with a fresh header."""
        nonlocal out_fh, cur_file_num, new_file_num
        if out_fh is not None:
            out_fh.close()
        cur_file_num = new_file_num
        new_file_num += 1
        path = os.path.join(db.local_dir, f"data_{cur_file_num:06d}.s4db")
        new_paths.append(path)
        out_fh = open(path, "wb")
        out_fh.write(pack_file_header(cur_file_num))

    new_index_entries: list[tuple[str, int, int, int]] = []  # key, file_num, offset, length

    try:
        for path in old_paths:
            file_num = old_file_nums[path]
            with open(path, "rb") as in_fh:
                for entry_offset, raw, key, flags in stream_file_entries(in_fh):
                    if flags == FLAG_TOMBSTONE:
                        continue
                    idx_entry = db._index.get(key)
                    if idx_entry is None or idx_entry.file_num != file_num or idx_entry.offset != entry_offset:
                        continue
                    # Live entry — write to compacted file
                    if out_fh is None:
                        open_new_file()
                    if out_fh.tell() > HEADER_SIZE and out_fh.tell() + len(raw) > db.max_file_size:
                        open_new_file()
                    out_offset = out_fh.tell()
                    out_fh.write(raw)
                    new_index_entries.append((key, cur_file_num, out_offset, len(raw)))
    finally:
        if out_fh is not None:
            out_fh.close()

    # Update index with new locations
    db._index.entries.clear()
    for key, fn, offset, length in new_index_entries:
        db._index.put(key, fn, offset, length)
    db._index.next_file_num = new_file_num
    db._save_index()

    # Remove old local files
    for path in old_paths:
        os.remove(path)

    # Delete old S3 files
    for filename in old_filenames:
        db.storage.delete(filename)

    # Upload new data files and index to S3
    for path in new_paths:
        db.storage.upload(path, os.path.basename(path))
    local_index = os.path.join(db.local_dir, _INDEX_FILENAME)
    db.storage.upload(local_index, _INDEX_FILENAME)
