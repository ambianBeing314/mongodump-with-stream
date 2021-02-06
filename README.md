# mongodump-with-stream

A boilerplate example to show a good way to read/fetch/do million or more records using nodejs streams instead of doing in memory

### Benchmarks

- **Read the documents in memory and dumping the clone (Bad)**

  - Number of records: ~1 Million
  - Memory consumption: 1.5GB
  - Time taken: ~35secs

- **Read the documents using stream and do batch dump with a threshold of ~8k records (Good)**

  - Number of records: ~1 Million
  - Memory consumption: ~400MB
  - Time taken: ~18secs
  - Note: The memory cosumption is still large because we have to keep interval-data in memory. For writable stream e.g; write data to file - (practical use case) `stream.pipe(writable)` would consume a lot less.
