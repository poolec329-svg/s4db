# s4db

A lightweight key-value database backed by S3. Keys and values are strings. Values are snappy-compressed and packed into numbered binary data files stored locally and synced to S3.

## How it works

s4db uses `local_dir` as the working store and S3 as durable remote storage. The two are kept in sync automatically on every write, but you control when full bulk syncs happen via `download()` and `upload()`.

**Index** — an `index.json` file maps every live key to its data file number, byte offset, and entry length. It is loaded into memory at startup and updated after every write.

**Startup** — the index is loaded from `local_dir/index.json` if present, otherwise downloaded from S3.

**Writes** (`put`, `delete`) — entries are appended to the latest data file in `local_dir`. When the file reaches `max_file_size` a new one is started. After each write the updated data file and index are pushed to S3 automatically.

**Reads** (`get`) — the index is checked first. If the data file is present in `local_dir` the value is read directly from disk. If the file is not local, only the exact bytes for that entry are fetched from S3 using a range request — the full file is never downloaded unless you call `download()`.

**Bulk sync** — `download()` pulls all data files and the index from S3 into `local_dir`. `upload()` pushes all local data files and the index to S3. Use these for initial setup, disaster recovery, or moving data between environments.

**Compaction** — reads all local data files, keeps only the latest value for each live key, discards tombstones, rewrites the live data into new files, and deletes the old files from both `local_dir` and S3.

**Rebuild index** — scans local data files in order and reconstructs the index from scratch. Run `download()` first if the local files are out of date.

## Installation

```bash
pip install s4db
```

Requires `python-snappy`, which links against the native Snappy library.

```bash
# macOS
brew install snappy

# Ubuntu / Debian
apt-get install libsnappy-dev
```

## Usage

### Opening a database

```python
from s4db import S4DB

db = S4DB(
    local_dir="/data/my-db",         # local directory for data files and index
    bucket="my-bucket",              # S3 bucket
    prefix="my-db/",                 # S3 key prefix
    max_file_size=64 * 1024 * 1024,  # optional, default 64 MB
    region_name="us-east-1",         # any extra kwargs go to boto3.client("s3", ...)
)
```

On init, s4db loads the index from `local_dir` if it exists, otherwise downloads it from S3. To also pull all data files locally, call `db.download()` after init.

### Writing

```python
db.put({"key1": "value1", "key2": "value2"})
```

Appends entries to the latest data file in `local_dir`. When the file reaches `max_file_size` it is closed and a new one is started. The updated file and index are pushed to S3 before the call returns.

### Reading

```python
value = db.get("key1")   # returns the string value, or None if not found
```

Looks up the key in the index, then:

- If the data file exists in `local_dir` — reads the exact bytes from disk.
- If the data file is not local — fetches only the bytes for that entry from S3 using a range request.

The full data file is never downloaded implicitly. Call `download()` to bring all files local.

### Deleting

```python
db.delete(["key1", "key2"])
```

Appends tombstone entries to the latest data file, removes the keys from the in-memory index, and pushes to S3. Only keys that currently exist in the index are written.

### Syncing with S3

```python
db.download()   # pull all data files and the index from S3 into local_dir
db.upload()     # push all local data files and the index to S3
```

`download()` is the right starting point when pointing a fresh `local_dir` at an existing S3 database. After `download()`, all reads are served from disk. `upload()` is useful after bulk operations like `compact()` or `rebuild_index()` if you want to force a full re-sync.

### Compaction

```python
db.compact()
```

Reads all local data files, keeps only the latest value for each live key, discards tombstones, writes the result into new numbered files in `local_dir`, uploads them to S3, and deletes the old files from both places. Run `download()` first if the local directory may be out of date.

### Rebuilding the index

```python
db.rebuild_index()
```

Scans every local data file in order and reconstructs the index from scratch. Saves the new index locally and uploads it to S3. Useful for recovery if the index file is lost or corrupted — run `download()` first to ensure all data files are present locally.

### Context manager

```python
with S4DB("/data/my-db", "my-bucket", "my-db/") as db:
    db.put({"k": "v"})
    print(db.get("k"))
```

## S3 layout

Given `bucket="my-bucket"` and `prefix="my-db/"`:

```
my-bucket/
  my-db/
    index.json
    data_000001.s4db
    data_000002.s4db
    ...
```

## Data file format

Each `.s4db` file starts with a 9-byte header followed by a sequence of entries.

```
File header:
  magic     4 bytes   b"S4DB"
  version   1 byte    0x01
  file_num  4 bytes   uint32, big-endian

Entry:
  flags     1 byte    0x00 = normal, 0x01 = tombstone
  key_len   4 bytes   uint32, big-endian
  value_len 4 bytes   uint32, big-endian  (0 for tombstones)
  key       key_len bytes, UTF-8
  value     value_len bytes, snappy-compressed  (absent for tombstones)
  crc32     4 bytes   uint32, big-endian, over all preceding entry bytes
```

## Index file format

`index.json` is stored at `{prefix}index.json` alongside the data files.

```json
{
  "version": 1,
  "next_file_num": 4,
  "entries": {
    "key1": [1, 9, 58],
    "key2": [3, 67, 42]
  }
}
```

Each entry value is `[file_num, byte_offset, entry_length]`. Deleted keys are removed from the index.

## Dependencies

- [boto3](https://github.com/boto/boto3) >= 1.26
- [python-snappy](https://github.com/andrix/python-snappy) >= 0.6

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3, no real AWS credentials required.

## License

MIT
