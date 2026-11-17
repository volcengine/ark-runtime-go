# Store installation and maintenance

Preserve processing state across restarts without changing the database on normal
startup. Install each production Store explicitly; ordinary Open/Run does not
create or upgrade its structures.

| Implementation | Scope | Requirements |
|---|---|---|
| embedded | One process | Exclusive bbolt file lock; Install before Open |
| SQLite | Processes on one host | WAL, busy_timeout, and `_txlock=immediate`; avoid shared filesystems without suitable locking |
| MySQL | Multiple instances | Shared database and namespace/prefix; transactions, row locks, and leases preserve ordering |

Register the database driver when embedding SQL. The handle returned by
`store/sql.Open` owns the sql.DB lifecycle; do not reuse that sql.DB for unrelated
components. Stop Runtime before closing the Store.

The initial public API supports the built-in stores, not third-party
implementations of internal record and transaction ports. Inject encryption
through `store.Codec`, or use `store.NewAESGCM`. Codec operations run outside
persistence transactions and must not fall back to plaintext on failure.

`store.NewAESGCM` is a single-key convenience implementation. Replacing it with a
new key does not make existing ciphertext readable with that key. For rotation,
inject a `store.Codec` that seals with the active key and selects a retained
decryption key by `Envelope.KeyID` in `Open`. Keep old keys available for as long
as retained data or backups need them; the Store does not manage KMS or key
lifecycle and does not rewrite immutable ciphertext during rotation.

Encrypted payloads include message content, frozen turn inputs, session creation
requests, delivery bodies, and approval previews and denial reasons. An existing
encrypted database containing older plaintext interactions needs an explicit
migration before those interactions can be read; normal Open does not silently
convert them or fall back to plaintext.

Retention removes settled short-lived records and their unreachable encrypted
payloads. Prune rebuilds payload links from live records, including opaque records,
and preserves session snapshots and their payloads independently of turn retention.
A missing or unreadable envelope on a known encrypted record stops collection.
An opaque future record without an inspectable envelope conservatively pins
collection; its recovery data is retained. Unknown children also pin their parent
before cascading deletion. This is logical database
deletion; WAL files, database backups, and storage-level erasure require separate
operational policies. MySQL encrypted writers share a maintenance lock; collection
takes it exclusively so a concurrent write cannot lose its payload.

## User authorization

User authorization requires encrypted payload storage. Install and open the
database with the same Codec and a stable PayloadNamespace.
Set RequirePayloadEncryption when sensitive inputs and outputs must be protected.
New databases created by the built-in `Install` functions already include the
User authorization structures. Normal Open/Run does not install missing
structures or overwrite existing data.

Existing databases must already satisfy the required structure and payload
policy. Normal startup does not convert existing application data.

## Tests

Run the following commands from the `channel/` directory. `make test-store`
runs the default storage suite without requiring a MySQL server. For the
separately tagged MySQL suite, start an isolated MySQL instance and replace
the example DSN with its actual test credentials:

```sh
make test-store
CHANNEL_TEST_MYSQL_DSN='root:fixture@tcp(127.0.0.1:3306)/channel_test?parseTime=true' \
  go test -race -count=1 -tags=mysqlintegration -timeout=20m ./store/sql
```

Use an isolated MySQL test database: tests create and modify namespaces. The
independent-worker matrix requires a loopback TCP endpoint and a database name
ending in `_test`. The in-memory SQLite Store is an internal testing facility,
not a production option.

The cross-process matrix builds independent workers from the same candidate SDK
and checks takeover, fencing, stable UUIDs, snapshots, and encryption. It does not
download historical internal versions or claim compatibility with them. Verify
the worker build and version handshake without a database using:

```sh
go test -tags=mysqlintegration ./store/sql -run '^TestCandidateWorkerBuilds$'
```
