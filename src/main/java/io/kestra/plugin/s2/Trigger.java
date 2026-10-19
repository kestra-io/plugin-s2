package io.kestra.plugin.s2;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Flow when new S2 records arrive",
    description = """
        Polls an S2 stream at a fixed interval and starts one Execution with the new records.
        The last processed sequence position is saved in the namespace KV store to prevent records from being delivered twice.
        Use the [RealtimeTrigger](https://kestra.io/plugins/plugin-s2/triggers/io.kestra.plugin.s2.realtimetrigger) to start one Execution per record as soon as it arrives.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Start a flow when new records are appended to an S2 stream.",
            code = """
                id: s2_polling_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Received {{ trigger.size }} new records"

                triggers:
                  - id: poll
                    type: io.kestra.plugin.s2.Trigger
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                    interval: PT30S
                    maxRecords: 100
                """
        )
    }
)
public class Trigger extends AbstractS2Trigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

    @Schema(
        title = "Interval",
        description = "Duration between polls for new records."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Maximum records",
        description = "Maximum number of records to process in one polling cycle, between 1 and 1000."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<@Min(1) @Max(1000) Integer> maxRecords = Property.ofValue(100);

    @NotNull
    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to poll."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @Schema(
        title = "Initial sequence number",
        description = "Initial position: start from the first record whose sequence number is greater than or equal to this value. Only used until the trigger has delivered a record."
    )
    @PluginProperty(group = "main")
    private Property<Long> seqNum;

    @Schema(
        title = "Initial timestamp",
        description = "Initial position: start from the first record whose timestamp is greater than or equal to this value. Only used until the trigger has delivered a record."
    )
    @PluginProperty(group = "main")
    private Property<Long> timestamp;

    @Schema(
        title = "Initial tail offset",
        description = "Initial position: start this many records before the tail of the stream. Only used until the trigger has delivered a record."
    )
    @PluginProperty(group = "main")
    private Property<Long> tailOffset;

    @Override
    public Duration getInterval() {
        return interval;
    }

    private int resolveMaxRecords(RunContext runContext) throws Exception {
        var value = runContext.render(this.maxRecords)
            .as(Integer.class)
            .orElse(100);

        if (value < 1 || value > 1000) {
            throw new IllegalArgumentException(
                "'maxRecords' must be between 1 and 1000"
            );
        }

        return value;
    }

    private Read.Output readRecords(
        RunContext runContext,
        String stream,
        Property<Long> seqNum,
        Property<Long> timestamp,
        Property<Long> tailOffset,
        int maxRecords
    ) throws Exception {
        var readTask = Read.builder()
            .id(this.id)
            .accessToken(this.getAccessToken())
            .basin(this.getBasin())
            .basinEndpoint(this.getBasinEndpoint())
            .stream(Property.ofValue(stream))
            .seqNum(seqNum)
            .timestamp(timestamp)
            .tailOffset(tailOffset)
            .maxRecords(Property.ofValue(maxRecords))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        return readTask.run(runContext);
    }

    private void startAtTail(
        RunContext runContext,
        String stream,
        String key
    ) throws Exception {
        var output = readRecords(
            runContext,
            stream,
            null,
            null,
            Property.ofValue(0L),
            1
        );

        if (output.getTail() == null) {
            throw new IllegalStateException(
                "S2 did not return a tail sequence number for stream: " + stream
            );
        }

        writeLastSeen(runContext, key, output.getTail() - 1);
    }

    @Override
    public Optional<Execution> evaluate(
        ConditionContext conditionContext,
        TriggerContext context
    ) throws Exception {

        var runContext = conditionContext.getRunContext();

        var stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("'stream' is required"));

        int maxRecords = resolveMaxRecords(runContext);

        Optional<Long> initialSeqNum = runContext.render(this.seqNum).as(Long.class);
        Optional<Long> initialTimestamp = runContext.render(this.timestamp).as(Long.class);
        Optional<Long> initialTailOffset = runContext.render(this.tailOffset).as(Long.class);

        long selectors = Stream.of(initialSeqNum, initialTimestamp, initialTailOffset)
            .filter(Optional::isPresent)
            .count();

        if (selectors > 1) {
            throw new IllegalArgumentException(
                "Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set"
            );
        }

        String key = buildWatermarkKey(runContext, stream);

        Read.Output output;

        if (hasLastSeen(runContext, key)) {
            long lastSeen = readLastSeen(runContext, key);
            output = readRecords(
                runContext,
                stream,
                Property.ofValue(lastSeen + 1),
                null,
                null,
                maxRecords
            );
        } else if (selectors == 1) {
            output = readRecords(
                runContext,
                stream,
                this.seqNum,
                this.timestamp,
                this.tailOffset,
                maxRecords
            );
        } else {
            startAtTail(runContext, stream, key);
            return Optional.empty();
        }

        if (output.getRecords().isEmpty()) {
            return Optional.empty();
        }

        var lastRecord = output.getRecords().getLast();

        long lastSequence = lastRecord.getSeqNum();

        Output triggerOutput = Output.builder()
            .records(output.getRecords())
            .size(output.getSize())
            .tail(output.getTail())
            .build();

        var execution = TriggerService.generateExecution(
            this,
            conditionContext,
            context,
            triggerOutput
        );

        writeLastSeen(runContext, key, lastSequence);

        return Optional.of(execution);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Records",
            description = "The records returned during this polling cycle."
        )
        private final List<Read.Record> records;

        @Schema(
            title = "Size",
            description = "Number of records returned."
        )
        private final Integer size;

        @Schema(
            title = "Tail",
            description = "Sequence number that will be assigned to the next record on the stream."
        )
        private final Long tail;
    }
}