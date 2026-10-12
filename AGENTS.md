# Kestra S2 Plugin

## What

- Provides plugin components under io.kestra.plugin.s2.
- Tasks: `CreateStream`, `DeleteStream`, `ListStreams`, `Append`, and `Read`.
- Triggers: `Trigger` and `RealtimeTrigger`.
- Shared task and trigger behavior lives in AbstractS2Task and AbstractS2Trigger.

## Why

- Teams need to integrate S2 streams into Kestra workflows to manage streams, append records, and read stream data.
- Workflows can react to incoming S2 records through polling or realtime event consumption.
- The plugin provides a consistent way to incorporate S2 stream operations and event driven workflows into Kestra.

## How

### Architecture

Single-module plugin. Source package:

- `io.kestra.plugin.s2`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- io.kestra.plugin.s2.CreateStream
- io.kestra.plugin.s2.DeleteStream
- io.kestra.plugin.s2.ListStreams
- io.kestra.plugin.s2.Append
- io.kestra.plugin.s2.Read
- io.kestra.plugin.s2.Trigger
- io.kestra.plugin.s2.RealtimeTrigger
- io.kestra.plugin.s2.AbstractS2Task
- io.kestra.plugin.s2.AbstractS2Trigger

### Project Structure

```
plugin-s2/
├── src/main/java/io/kestra/plugin/s2/
├── src/test/java/io/kestra/plugin/s2/
├── build.gradle
└── README.md
```

## Local rules

- Base documentation on implemented classes and verified behavior, not template README content.
- Follow existing Kestra conventions for task and trigger implementations.
- Add or update tests when changing task or trigger behavior.
- Keep polling and realtime trigger behavior clearly distinguished in documentation.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
