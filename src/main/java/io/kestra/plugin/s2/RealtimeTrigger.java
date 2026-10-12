package io.kestra.plugin.s2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.HttpSseEvent;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Flow for each S2 record",
    description = """
        Opens a long-lived Server-Sent Events (SSE) read session on an S2 stream and starts one Execution per record.
        The last delivered sequence number is saved in the namespace KV store after each batch, so after a restart reading resumes right after the last delivered record without re-firing it.
        Use `seqNum`, `timestamp`, or `tailOffset` to choose the initial position; only one of them can be set and it is only used until the trigger has delivered a record.
        When none is set, the initial position is the tail of the stream and only records appended after the session opens are emitted.
        Prefer the polling [Trigger](https://kestra.io/plugins/plugin-s2/triggers/io.kestra.plugin.s2.trigger) for interval-based reads.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Start a flow for each record appended to an S2 stream.",
            code = """
                id: s2_realtime_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Record {{ trigger.seqNum }}: {{ trigger.body }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.s2.RealtimeTrigger
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                """
        )
    }
)
public class RealtimeTrigger extends AbstractS2Trigger
    implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {

    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to listen to."
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

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<HttpClient> client = new AtomicReference<>();

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String streamName = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("'stream' is required"));

        String watermarkKey = buildWatermarkKey(runContext, streamName);
        long lastSeen = readLastSeen(runContext, watermarkKey);

        HttpRequest request = createSseRequest(runContext, streamName, lastSeen);

        return Flux.from(publisher(runContext, request, watermarkKey))
            .map(record -> TriggerService.generateRealtimeExecution(
                this,
                conditionContext,
                context,
                Output.builder()
                    .seqNum(record.getSeqNum())
                    .timestamp(record.getTimestamp())
                    .body(record.getBody())
                    .headers(record.getHeaders())
                    .build()
            ));
    }

    public Publisher<Read.Record> publisher(RunContext runContext, HttpRequest request) {
        return publisher(runContext, request, null);
    }

    public Publisher<Read.Record> publisher(RunContext runContext, HttpRequest request, String watermarkKey) {
        return Flux.create(sink -> {
            Logger logger = runContext.logger();
            sink.onDispose(this::closeClient);

            try {
                HttpClient httpClient = createHttpClient(runContext);
                this.client.set(httpClient);

                try (httpClient) {
                    if (!isActive.get()) {
                        return;
                    }

                    logger.debug("Starting S2 realtime trigger triggerId={}", this.id);

                    HttpResponse<Void> response = httpClient.sseRequest(
                        request,
                        String.class,
                        event -> {
                            Long lastSeqNum = handleEvent(event, sink);
                            if (lastSeqNum != null && watermarkKey != null) {
                                saveWatermark(runContext, watermarkKey, lastSeqNum);
                            }
                        }
                    );

                    int statusCode = response.getStatus().getCode();
                    if (isActive.get() && (statusCode < 200 || statusCode >= 300)) {
                        sink.error(new IllegalStateException(
                            "S2 SSE read failed with HTTP status " + statusCode + ". " + Read.errorHint(statusCode)
                        ));
                    }
                }
            } catch (Exception e) {
                if (isActive.get() && !sink.isCancelled()) {
                    logger.error("S2 realtime trigger triggerId={} failed: {}", this.id, e.getMessage());
                    sink.error(e);
                } else {
                    logger.debug("S2 realtime trigger triggerId={} stopped", this.id);
                }
            } finally {
                sink.complete();
                this.waitForTermination.countDown();
            }
        });
    }

    Long handleEvent(HttpSseEvent<String> event, FluxSink<Read.Record> sink) {
        if (!isActive.get() || sink.isCancelled()) {
            return null;
        }

        String data = event.data();

        if ("[DONE]".equals(data)) {
            sink.complete();
            return null;
        }

        if ("error".equals(event.name())) {
            sink.error(new IllegalStateException("S2 SSE stream reported an error: " + data));
            return null;
        }

        if ("batch".equals(event.name())) {
            try {
                S2Batch batch = JacksonMapper.ofJson().readValue(data, S2Batch.class);
                Long lastSeqNum = null;

                if (batch.getRecords() != null) {
                    for (Read.Record record : batch.getRecords()) {
                        if (sink.isCancelled()) {
                            break;
                        }
                        sink.next(record);
                        lastSeqNum = record.getSeqNum();
                    }
                }

                return lastSeqNum;
            } catch (Exception e) {
                sink.error(e);
            }
        }

        return null;
    }

    private void saveWatermark(RunContext runContext, String key, long seqNum) {
        try {
            writeLastSeen(runContext, key, seqNum);
        } catch (Exception e) {
            runContext.logger().warn(
                "S2 realtime trigger triggerId={} could not save its position at sequence number {}: {}",
                this.id,
                seqNum,
                e.getMessage()
            );
        }
    }

    HttpRequest createSseRequest(RunContext runContext, String streamName) throws Exception {
        return createSseRequest(runContext, streamName, -1L);
    }

    HttpRequest createSseRequest(RunContext runContext, String streamName, long lastSeen) throws Exception {
        String basin = runContext.render(this.getBasin())
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("S2 'basin' is required"));

        String endpoint = runContext.render(this.getBasinEndpoint())
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("S2 'basinEndpoint' is required"));

        endpoint = endpoint.replace("{basin}", basin);
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }

        Optional<Long> startSeqNum = runContext.render(this.seqNum).as(Long.class);
        Optional<Long> startTimestamp = runContext.render(this.timestamp).as(Long.class);
        Optional<Long> startTailOffset = runContext.render(this.tailOffset).as(Long.class);

        long selectors = Stream.of(startSeqNum, startTimestamp, startTailOffset)
            .filter(Optional::isPresent)
            .count();

        if (selectors > 1) {
            throw new IllegalArgumentException("Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set");
        }

        Map<String, Object> params = new LinkedHashMap<>();
        if (lastSeen >= 0) {
            params.put("seq_num", lastSeen + 1);
        } else {
            startSeqNum.ifPresent(value -> params.put("seq_num", value));
            startTimestamp.ifPresent(value -> params.put("timestamp", value));
            startTailOffset.ifPresent(value -> params.put("tail_offset", value));
        }

        String encodedStream = URLEncoder.encode(streamName, StandardCharsets.UTF_8).replace("+", "%20");

        String url = endpoint + "/streams/" + encodedStream + "/records";

        if (!params.isEmpty()) {
            url = url + "?" + params.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        }

        return createRequest(
            runContext,
            "GET",
            url,
            null,
            Map.of("Accept", List.of("text/event-stream"))
        );
    }

    private void closeClient() {
        HttpClient httpClient = this.client.getAndSet(null);
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void kill() {
        stop(true);
    }

    @Override
    public void stop() {
        stop(false);
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        boolean started = this.client.get() != null;
        closeClient();

        if (wait && started) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class S2Batch {
        private List<Read.Record> records;
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Sequence number",
            description = "Sequence number assigned by S2 to the record."
        )
        private final Long seqNum;

        @Schema(
            title = "Timestamp",
            description = "Timestamp of the record."
        )
        private final Long timestamp;

        @Schema(
            title = "Record body",
            description = "Body of the S2 record."
        )
        private final String body;

        @Schema(
            title = "Record headers",
            description = "Name value header pairs of the S2 record."
        )
        private final List<List<String>> headers;
    }
}