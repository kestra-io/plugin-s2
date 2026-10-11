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
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an S2 stream",
    description = "Creates a new stream in an S2 basin."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a stream",
            full = true,
            code = """
                id: create_stream
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.s2.CreateStream
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                """
        )
    }
)
public class CreateStream extends AbstractS2Task implements RunnableTask<CreateStream.Output> {

    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to create."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("'stream' is required"));

        validateStreamName(stream);

        URI uri = buildStreamsUri(runContext);

        CreateStreamInput input = CreateStreamInput.builder()
            .stream(stream)
            .build();

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

        if (statusCode != 201) {
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
        return URI.create(basinBaseUrl(runContext) + "/streams");
    }

    private static void validateStreamName(String stream) {
        if (stream.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                "'stream' must not contain NUL bytes"
            );
        }

        if (stream.getBytes(StandardCharsets.UTF_8).length > 512) {
            throw new IllegalArgumentException(
                "'stream' must not exceed 512 bytes"
            );
        }
    }

    static String buildErrorMessage(int statusCode, String body) {
        String hint = switch (statusCode) {
            case 400 ->
                "Bad request. Check the stream name.";
            case 401 ->
                "Unauthorized. Check that 'accessToken' is valid and not expired.";
            case 403 ->
                "Permission denied. Check the access token permissions.";
            case 404 ->
                "Basin not found. Check the 'basin' value.";
            case 408 ->
                "Request timed out. This operation can be retried.";
            case 409 ->
                "Conflict. Check the S2 error code and message before retrying.";
            default ->
                "Unexpected response from S2.";
        };

        return "S2 create stream failed with HTTP status "
            + statusCode
            + ". "
            + hint
            + " Response: "
            + body;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class CreateStreamInput {
        private String stream;
    }

    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Stream name",
            description = "Name of the created stream."
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