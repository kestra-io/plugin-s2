package io.kestra.plugin.s2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Append records to an S2 stream",
    description = "Appends records atomically to an S2 stream."
)
@Plugin(
    examples = {
        @Example(
            title = "Append records to a stream",
            full = true,
            code = """
                id: append_records
                namespace: company.team

                tasks:
                  - id: append
                    type: io.kestra.plugin.s2.Append
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                    records:
                      - body: "order created"
                """
        )
    }
)
public class Append extends AbstractS2Task implements RunnableTask<Append.Output> {

    @NotNull
    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to append records to."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @NotNull
    @Schema(
        title = "Records",
        description = "Records to append to the S2 stream."
    )
    @PluginProperty(group = "main")
    private Data records;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("'stream' is required"));

        if (this.records == null) {
            throw new IllegalArgumentException("'records' is required");
        }

        Flux<Record> records = this.records.readAs(
            runContext,
            Record.class,
            map -> JacksonMapper.ofJson().convertValue(map, Record.class)
        );

        var recordList = records.take(1001).collectList().block();

        if (recordList == null || recordList.isEmpty()) {
            throw new IllegalArgumentException("'records' must contain at least one record");
        }

        if (recordList.size() > 1000) {
            throw new IllegalArgumentException("S2 append supports at most 1000 records per request");
        }

        AppendInput input = AppendInput.builder()
            .records(toS2Records(recordList))
            .build();

        URI uri = buildRecordsUri(runContext, stream);

        HttpRequest request = createRequest(
            runContext,
            "POST",
            uri.toString(),
            HttpRequest.JsonRequestBody.of(input),
            Map.of()
        );

        HttpResponse<String> response = execute(
            runContext,
            request,
            String.class
        );

        int statusCode = response.getStatus().getCode();

        if (statusCode != 200) {
            throw new IllegalStateException(
                buildErrorMessage(
                    statusCode,
                    response.getBody()
                )
            );
        }

        return toOutput(response.getBody());
    }

    static Output toOutput(String responseBody) throws Exception {
        if (responseBody == null || responseBody.isBlank()) {
            throw new IllegalStateException(
                "Empty response from S2 append, check the basin endpoint"
            );
        }

        AppendAck ack = JacksonMapper.ofJson()
            .readValue(responseBody, AppendAck.class);

        if (ack.getStart() == null || ack.getStart().getSeqNum() == null
            || ack.getEnd() == null || ack.getEnd().getSeqNum() == null
            || ack.getTail() == null || ack.getTail().getSeqNum() == null) {
            throw new IllegalStateException(
                "Incomplete response from S2 append: start, end, and tail sequence numbers are required"
            );
        }

        return Output.builder()
            .start(ack.getStart().getSeqNum())
            .end(ack.getEnd().getSeqNum())
            .tail(ack.getTail().getSeqNum())
            .build();
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Record {

        @Schema(
            title = "Record body",
            description = "Body of the S2 record."
        )
        private String body;

        @Schema(
            title = "Record headers",
            description = "Headers attached to the S2 record."
        )
        private List<List<String>> headers;

        @Schema(
            title = "Timestamp",
            description = "Optional record timestamp."
        )
        private Long timestamp;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class AppendInput {

        private List<S2Record> records;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class S2Record {

        private String body;
        private List<List<String>> headers;
        private Long timestamp;
    }

    private List<S2Record> toS2Records(List<Record> records) {
        return records.stream()
            .map(record -> S2Record.builder()
                .body(record.getBody())
                .headers(record.getHeaders())
                .timestamp(record.getTimestamp())
                .build()
            )
            .toList();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    private static class AppendAck {

        private StreamPosition start;
        private StreamPosition end;
        private StreamPosition tail;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    private static class StreamPosition {

        @JsonProperty("seq_num")
        private Long seqNum;

        private Long timestamp;
    }

    static String buildErrorMessage(int statusCode, String body) {
        var hint = switch (statusCode) {
            case 400 ->
                "Bad request. Check the stream name and record fields.";
            case 401 ->
                "Unauthorized. Check that 'accessToken' is valid and not expired.";
            case 403 ->
                "Permission denied or quota exhausted. Check the access token permissions and S2 quota.";
            case 404 ->
                "Stream or basin not found. Check the 'stream' and 'basin' values.";
            case 408 ->
                "Request timed out. This operation can be retried.";
            case 409 ->
                "Conflict. Check the S2 error code and message before retrying.";
            default ->
                "Unexpected response from S2.";
        };

        return "S2 append failed with HTTP status "
            + statusCode
            + ". "
            + hint
            + " Response: "
            + body;
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Start sequence number",
            description = "Sequence number assigned to the first appended record."
        )
        private final Long start;

        @Schema(
            title = "End sequence number",
            description = "Sequence number just after the last appended record."
        )
        private final Long end;

        @Schema(
            title = "Tail sequence number",
            description = "Sequence number that will be assigned to the next record on the stream."
        )
        private final Long tail;
    }
}