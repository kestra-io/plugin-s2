package io.kestra.plugin.s2;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List S2 streams",
    description = "Lists streams in an S2 basin with optional prefix filtering and pagination."
)
@Plugin(
    examples = {
        @Example(
            title = "List streams",
            full = true,
            code = """
                id: list_streams
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.s2.ListStreams
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    limit: 100
                """
        )
    }
)
public class ListStreams extends AbstractS2Task implements RunnableTask<ListStreams.Output> {

    @Schema(
        title = "Prefix",
        description = "Filters streams whose names begin with this prefix."
    )
    @PluginProperty(group = "main")
    private Property<String> prefix;

    @Schema(
        title = "Start after",
        description = "Returns streams whose names are lexicographically after this value."
    )
    @PluginProperty(group = "main")
    private Property<String> startAfter;

    @Schema(
        title = "Limit",
        description = "Maximum number of streams to return."
    )
    @PluginProperty(group = "main")
    @Min(0)
    @Max(1000)
    @Builder.Default
    private Property<Integer> limit = Property.ofValue(1000);

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI uri = buildStreamsUri(runContext);

        HttpRequest request = createRequest(
            runContext,
            "GET",
            uri.toString(),
            null,
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

        return JacksonMapper.ofJson()
            .readValue(response.getBody(), Output.class);
    }

    URI buildStreamsUri(RunContext runContext) throws Exception {
        return URI.create(
            basinBaseUrl(runContext) + "/streams?" + buildQueryString(runContext)
        );
    }

    String buildQueryString(RunContext runContext) throws Exception {
        Map<String, String> query = new LinkedHashMap<>();

        String prefix = renderOptional(runContext, this.prefix);

        if (prefix != null) {
            validateQueryValue(prefix, "prefix");
            query.put("prefix", prefix);
        }

        String startAfter = renderOptional(runContext, this.startAfter);

        if (startAfter != null) {
            validateQueryValue(startAfter, "startAfter");
            query.put("start_after", startAfter);
        }

        Integer limit = runContext.render(this.limit)
            .as(Integer.class)
            .orElse(1000);

        if (limit < 0 || limit > 1000) {
            throw new IllegalArgumentException(
                "'limit' must be between 0 and 1000"
            );
        }

        query.put("limit", String.valueOf(limit));

        return query.entrySet()
            .stream()
            .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                + "="
                + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
            .reduce(
                (left, right) -> left + "&" + right
            )
            .orElse("");
    }

    private String renderOptional(
        RunContext runContext,
        Property<String> property
    ) throws Exception {
        if (property == null) {
            return null;
        }

        return runContext.render(property)
            .as(String.class)
            .orElse(null);
    }

    private static void validateQueryValue(
        String value,
        String property
    ) {
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                "'" + property + "' must not contain NUL bytes"
            );
        }
    }

    static String buildErrorMessage(int statusCode, String body) {
        String hint = switch (statusCode) {
            case 400 ->
                "Bad request. Check the prefix, startAfter and limit values.";
            case 401 ->
                "Unauthorized. Check that 'accessToken' is valid and not expired.";
            case 403 ->
                "Permission denied. Check the access token permissions.";
            case 404 ->
                "Basin not found. Check the 'basin' value.";
            case 408 ->
                "Request timed out. This operation can be retried.";
            default ->
                "Unexpected response from S2.";
        };

        return "S2 list streams failed with HTTP status "
            + statusCode
            + ". "
            + hint
            + " Response: "
            + body;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Streams",
            description = "The streams returned by S2, ordered by name."
        )
        private List<StreamInfo> streams = new ArrayList<>();

        @Schema(
            title = "Has more",
            description = "Whether more streams are available after this page."
        )
        @JsonAlias("has_more")
        private boolean hasMore;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamInfo {

        @Schema(
            title = "Stream name",
            description = "Name of the stream."
        )
        private String name;

        @Schema(
            title = "Creation time",
            description = "Time the stream was created."
        )
        @JsonAlias("created_at")
        private String createdAt;

        @Schema(
            title = "Encryption cipher",
            description = "Cipher used to encrypt the stream, if any."
        )
        private String cipher;

        @Schema(
            title = "Deletion time",
            description = "Time the stream was marked for deletion, if any."
        )
        @JsonAlias("deleted_at")
        private String deletedAt;
    }
}