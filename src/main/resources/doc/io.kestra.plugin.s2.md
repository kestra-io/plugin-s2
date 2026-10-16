# How to use the S2 plugin

[S2](https://s2.dev) is a managed API for durable, ordered, real-time streams. This plugin lets a flow create and delete streams, append and read records, and start executions when new records arrive, without writing HTTP calls by hand.

## Authentication

Every task and trigger needs these properties:

- `accessToken` (required): an S2 access token, sent as a bearer token. It is a secret, so store it as a Kestra secret and reference it with `{{ secret('S2_ACCESS_TOKEN') }}`. See [secrets](https://kestra.io/docs/concepts/secret).
- `basin` (required): the name of the S2 basin that holds your streams. Basin names are 8 to 48 characters of lowercase letters, numbers and hyphens, and cannot begin or end with a hyphen.
- `basinEndpoint` (optional): the basin API URL. The default is `https://{basin}.b.s2.dev/v1`, where `{basin}` is replaced by the `basin` value. If the URL has no `{basin}` placeholder, the basin is sent in the `S2-Basin` header instead. This is how you point the plugin at a custom endpoint or at [s2-lite](https://github.com/s2-streamstore/s2), for example `http://localhost:8080/v1`.

To avoid repeating these properties on every task, Kestra 1.x lets you set them once with plugin defaults. [`pluginDefaults` was removed in Kestra 2.0](https://kestra.io/docs/migration-guide/v2.0.0/plugin-defaults-removed): Enterprise Edition uses [Policies](https://kestra.io/docs/enterprise/governance/policies) instead, and in the open-source edition you set the properties on each task.

## Tasks

| Task | What it does | Required | Optional | Outputs |
| --- | --- | --- | --- | --- |
| `CreateStream` | Creates a stream | `stream` | | `name`, `createdAt`, `cipher`, `deletedAt` |
| `DeleteStream` | Requests deletion of a stream and its records | `stream` | | none |
| `ListStreams` | Lists streams in the basin | | `prefix`, `startAfter`, `limit` (0 to 1000, default 1000) | `streams`, `hasMore` |
| `Append` | Appends records to a stream | `stream`, `records` | | `start`, `end`, `tail` |
| `Read` | Reads records from a stream | `stream` | `seqNum`, `timestamp`, `tailOffset`, `fetchType`, `maxRecords` | `records`, `record`, `uri`, `size`, `tail` |

### Append

`records` is either an inline list or the `kestra://` URI of a file in internal storage. A request holds 1 to 1000 records. Each record has a `body`, and optionally `headers` (a list of name and value pairs) and a `timestamp`. The outputs are the sequence number of the first appended record (`start`), the number just after the last one (`end`), and the number the next record will get (`tail`).

### Read

Set at most one of `seqNum`, `timestamp` or `tailOffset` to choose where reading starts. With none of them, `Read` returns the latest `maxRecords` records. `maxRecords` is between 1 and 1000 and defaults to 100. `fetchType` is `FETCH` (records in `records`), `FETCH_ONE` (a single record in `record`), or `STORE` (the default, records saved to internal storage and returned as `uri`). `size` is the number of records read and `tail` is the sequence number the next record will get.

### CreateStream

Creates a new stream in the configured basin. Set `stream` to the name of the stream to create.

Outputs: `name` (the stream name), `createdAt` (the stream creation time), `cipher` (the stream's cipher information), and `deletedAt` (the deletion timestamp, if available).

### DeleteStream

Requests deletion of the specified stream and its records from the configured basin. S2 completes the deletion asynchronously. Set `stream` to the name of the stream to delete.

This task has no output.

### ListStreams

Lists streams in the configured basin. Use `prefix` to filter stream names by prefix, `startAfter` to continue listing after a specified stream name, and `limit` to control the maximum number of streams returned (0 to 1000, default 1000).

Outputs: `streams` (the list of returned streams) and `hasMore` (whether more streams are available).

## Triggers

Both triggers use the connection properties from the Authentication section, and both need `stream`. Both can start from an initial position with `seqNum`, `timestamp` or `tailOffset` (only one of them). That position is used only until the trigger has delivered a record.

Both triggers save the last delivered sequence number in the namespace KV store. After a restart they resume right after it, so a delivered record is never fired twice. When nothing is saved yet and no initial position is set, they start at the tail of the stream and skip the records that already exist.

### Trigger

Polls the stream every `interval` (default one minute) and starts one execution with all new records, up to `maxRecords` per poll (1 to 1000, default 100).

Outputs: `records` (the new records), `size` (how many), and `tail`.

### RealtimeTrigger

Keeps a Server-Sent Events (SSE) connection open and starts one execution for each record as soon as it arrives.

Outputs for each record: `seqNum`, `timestamp`, `body` and `headers`. For example, `{{ trigger.seqNum }}` and `{{ trigger.body }}`.