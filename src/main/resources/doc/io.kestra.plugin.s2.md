# How to use the S2 plugin

Manage S2 streams and process stream records through Kestra workflows. The plugin supports stream management, record operations, and polling or realtime triggers for incoming records.

## Tasks

- `CreateStream` creates an S2 stream. `DeleteStream` deletes a stream, and `ListStreams` lists available streams.
- `Append` appends records to an S2 stream, while `Read` reads records from a stream.

## Triggers

- `Trigger` polls an S2 stream for records and starts a Kestra flow execution when records are available.
- `RealtimeTrigger` consumes S2 stream events through Server Sent Events (SSE) and starts flow executions for incoming records.
