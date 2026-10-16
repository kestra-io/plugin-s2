package io.kestra.plugin.s2;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read records from an S2 stream",
    description = "Reads records from an S2 stream by sequence number, timestamp, or tail offset."
)
@Plugin(
    examples = {
        @Example(
            title = "Read latest records",
            full = true,
            code = """
                id: s2_read
                namespace: company.team

                tasks:
                  - id: read
                    type: io.kestra.plugin.s2.Read
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                    fetchType: FETCH
                    maxRecords: 100
                """
        )
    }
)
public class Read extends AbstractS2Task implements RunnableTask<Read.Output> {
    @NotNull
    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to read records from."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @Schema(
        title = "Start sequence number",
        description = "Start from the first record whose sequence number is greater than or equal to this value."
    )
    @PluginProperty(group = "main")
    private Property<Long> seqNum;

    @Schema(
        title = "Start timestamp",
        description = "Start from the first record whose timestamp is greater than or equal to this value."
    )
    @PluginProperty(group = "main")
    private Property<Long> timestamp;

    @Schema(
        title = "Start tail offset",
        description = "Start this many records before the tail of the stream."
    )
    @PluginProperty(group = "main")
    private Property<Long> tailOffset;

    @Schema(
        title = "Fetch type",
        description = "Determines whether records are returned directly, a single record is returned, or records are stored in Kestra internal storage."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Schema(
        title = "Maximum records",
        description = "Maximum number of records to read, between 1 and 1000."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<@Min(1) @Max(1000) Integer> maxRecords = Property.ofValue(100);

    String buildQueryString(RunContext runContext) throws Exception {
        Map<String, Object> params = new LinkedHashMap<>();

        Optional<Long> startSeqNum =
            renderOptional(runContext, this.seqNum, Long.class);

        Optional<Long> startTimestamp =
            renderOptional(runContext, this.timestamp, Long.class);

        Optional<Long> startTailOffset =
            renderOptional(runContext, this.tailOffset, Long.class);

        long selectors = Stream.of(
                startSeqNum,
                startTimestamp,
                startTailOffset
            )
            .filter(Optional::isPresent)
            .count();

        if (selectors > 1) {
            throw new IllegalArgumentException(
                "Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set"
            );
        }

        int limit = resolveLimit(runContext);

        if (startSeqNum.isPresent()) {
            params.put("seq_num", startSeqNum.get());
        }

        if (startTimestamp.isPresent()) {
            params.put("timestamp", startTimestamp.get());
        }

        if (startTailOffset.isPresent()) {
            params.put("tail_offset", startTailOffset.get());
        }

        if (selectors == 0) {
            params.put("tail_offset", limit);
        }

        params.put("count", limit);

        return params.entrySet()
            .stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining("&"));
    }

    private int resolveLimit(RunContext runContext) throws Exception {
        FetchType type = renderOptional(
            runContext,
            this.fetchType,
            FetchType.class
        ).orElse(FetchType.STORE);

        if (type == FetchType.FETCH_ONE) {
            return 1;
        }

        int requested = renderOptional(
            runContext,
            this.maxRecords,
            Integer.class
        ).orElse(100);

        if (requested < 1 || requested > 1000) {
            throw new IllegalArgumentException(
                "'maxRecords' must be between 1 and 1000"
            );
        }

        return requested;
    }

    private static <T> Optional<T> renderOptional(
        RunContext runContext,
        Property<T> property,
        Class<T> type
    ) throws Exception {
        if (property == null) {
            return Optional.empty();
        }

        return runContext.render(property).as(type);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'stream' is required")
            );

        var uri = buildRecordsUri(runContext, stream);

        var query = buildQueryString(runContext);

        var request = createRequest(
            runContext,
            "GET",
            uri + "?" + query,
            null,
            Map.of()
        );

        var response = execute(
            runContext,
            request,
            String.class
        );

        int statusCode = response.getStatus().getCode();

        if (statusCode != 200 && statusCode != 416) {
            throw new IllegalStateException(
                buildErrorMessage(
                    statusCode,
                    response.getBody()
                )
            );
        }

        var batch = parseResponse(response.getBody());

        List<Record> records = batch.getRecords() == null
            ? List.of()
            : batch.getRecords();

        Long tail = batch.getTail() == null
            ? null
            : batch.getTail().getSeqNum();

        FetchType type = renderOptional(
            runContext,
            this.fetchType,
            FetchType.class
        ).orElse(FetchType.STORE);

        return switch (type) {
            case FETCH -> Output.builder()
                .records(records)
                .size(records.size())
                .tail(tail)
                .build();

            case FETCH_ONE -> Output.builder()
                .record(records.isEmpty() ? null : records.getFirst())
                .size(records.isEmpty() ? 0 : 1)
                .tail(tail)
                .build();

            case STORE -> Output.builder()
                .uri(store(runContext, records))
                .size(records.size())
                .tail(tail)
                .build();

            default -> throw new IllegalStateException(
                "Unsupported fetch type: " + type
            );
        };
    }

    static ReadBatch parseResponse(String responseBody) throws Exception {
        if (responseBody == null || responseBody.isBlank()) {
            throw new IllegalStateException(
                "Empty response from S2 read, check the basin endpoint"
            );
        }

        var batch = JacksonMapper.ofJson()
            .readValue(responseBody, ReadBatch.class);

        if (batch == null) {
            throw new IllegalStateException(
                "Incomplete response from S2 read: response body must contain a JSON object"
            );
        }

        return batch;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Record {

        @Schema(
            title = "Sequence number",
            description = "Sequence number assigned by S2."
        )
        @JsonAlias("seq_num")
        private Long seqNum;

        @Schema(
            title = "Timestamp",
            description = "Timestamp of the record."
        )
        private Long timestamp;

        @Schema(
            title = "Record body",
            description = "Body of the S2 record."
        )
        private String body;

        @Schema(
            title = "Record headers",
            description = "Name value header pairs of the S2 record."
        )
        private List<List<String>> headers;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ReadBatch {

        private List<Record> records;

        private StreamPosition tail;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class StreamPosition {

        @JsonProperty("seq_num")
        private Long seqNum;

        private Long timestamp;
    }

    private URI store(
        RunContext runContext,
        List<Record> records
    ) throws Exception {
        File tempFile = runContext.workingDir()
            .createTempFile(".ion")
            .toFile();

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            for (Record record : records) {
                FileSerde.write(outputStream, record);
            }
        }

        return runContext.storage().putFile(tempFile);
    }

    static String buildErrorMessage(int statusCode, String body) {
        return "S2 read failed with HTTP status "
            + statusCode
            + ". "
            + errorHint(statusCode)
            + " Response: "
            + body;
    }

    static String errorHint(int statusCode) {
        return switch (statusCode) {
            case 400 ->
                "Bad request. Check the read parameters and stream configuration.";
            case 401 ->
                "Unauthorized. Check that 'accessToken' is valid and not expired.";
            case 403 ->
                "Permission denied. Check the access token permissions.";
            case 404 ->
                "Stream or basin not found. Check the 'stream' and 'basin' values.";
            case 408 ->
                "Request timed out. Retry the task if appropriate.";
            case 409 ->
                "Conflict. Check the S2 error code and message before retrying.";
            case 416 ->
                "The requested read position is not currently available. Check the starting position.";
            default ->
                "Unexpected response from S2.";
        };
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Records",
            description = "The records that were read. Only set when 'fetchType' is FETCH."
        )
        private final List<Record> records;

        @Schema(
            title = "Record",
            description = "The first record that was read. Only set when 'fetchType' is FETCH_ONE and a record exists."
        )
        private final Record record;

        @Schema(
            title = "Records URI",
            description = "URI of the ION file in Kestra internal storage containing the records. Only set when 'fetchType' is STORE."
        )
        private final URI uri;

        @Schema(
            title = "Size",
            description = "Number of records read."
        )
        private final Integer size;

        @Schema(
            title = "Tail",
            description = "Sequence number that will be assigned to the next record on the stream."
        )
        private final Long tail;
    }
}