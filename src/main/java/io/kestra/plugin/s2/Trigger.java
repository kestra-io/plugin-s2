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
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Flow when new S2 records arrive",
    description = """
        Polls an S2 stream at a fixed interval and starts one Execution with the new records.
        The last delivered sequence number is saved in the namespace KV store, so records are not delivered twice.
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
public class Trigger extends AbstractS2Trigger implements PollingTriggerInterface, TriggerOutput {

    @Schema(
        title = "Interval",
        description = "Duration between polls for new records."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Maximum records",
        description = "Maximum number of records to process in one polling cycle."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<Integer> maxRecords = Property.ofValue(100);

    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to poll."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @Override
    public Duration getInterval() {
        return interval;
    }

    private int resolveMaxRecords(RunContext runContext) throws Exception {
        int value = runContext.render(this.maxRecords)
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
        long startSeqNum
    ) throws Exception {
        Read readTask = Read.builder()
            .id(this.id)
            .accessToken(this.getAccessToken())
            .basin(this.getBasin())
            .accountEndpoint(this.getAccountEndpoint())
            .basinEndpoint(this.getBasinEndpoint())
            .stream(Property.ofValue(stream))
            .seqNum(Property.ofValue(startSeqNum))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .maxRecords(Property.ofValue(resolveMaxRecords(runContext)))
            .build();

        return readTask.run(runContext);
    }

    @Override
    public Optional<Execution> evaluate(
        ConditionContext conditionContext,
        TriggerContext context
    ) throws Exception {

        RunContext runContext = conditionContext.getRunContext();

        String stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("'stream' is required"));

        String key = buildWatermarkKey(runContext, stream);

        long lastSeen = readLastSeen(runContext, key);

        long startSeqNum = lastSeen + 1;

        Read.Output output = readRecords(
            runContext,
            stream,
            startSeqNum
        );

        if (output.getRecords().isEmpty()) {
            return Optional.empty();
        }

        Read.Record lastRecord = output.getRecords()
            .get(output.getRecords().size() - 1);

        long lastSequence = lastRecord.getSeqNum();

        Execution execution = TriggerService.generateExecution(
            this,
            conditionContext,
            context,
            output
        );

        writeLastSeen(runContext, key, lastSequence);

        return Optional.of(execution);
    }

}