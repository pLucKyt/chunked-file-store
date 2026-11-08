# Chunked file store

Splits a file into fixed size chunks, hashes and compresses each chunk to disk, and
puts the file back together on request. Written for the parallel algorithms course.

Each chunk is hashed before it is compressed. When a chunk is read back it gets
decompressed and hashed again, and the two digests are compared, so a chunk that was
damaged on disk is caught instead of silently returned.

The reader/writer lock is mine, not from a library. `read_change_lock` in `run.py` is
built on one mutex and two condition variables, one for readers and one for writers,
with counters for each.

Memory use is capped by `memory_limit` in `config.yaml`. A writer that would push the
total over the cap waits on a condition variable until something frees up, so writers
slow down instead of the process growing without a limit.

## Running

    python run.py

Commands are `put <file>`, `get <id>`, `delete <id>` and `list`.

Note: it reads `config.yaml` with a Windows path separator, so as written it runs on
Windows.
