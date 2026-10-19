package io.kestra.plugin.s2;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete an S2 stream",
    description = "Deletes an S2 stream and its records."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a stream",
            full = true,
            code = """
                id: delete_stream
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.s2.DeleteStream
                    accessToken: "{{ secret('S2_ACCESS_TOKEN') }}"
                    basin: my-basin
                    stream: orders
                """
        )
    }
)
public class DeleteStream extends AbstractS2Task implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(
        title = "Stream",
        description = "Name of the S2 stream to delete."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var stream = runContext.render(this.stream)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(
                () -> new IllegalArgumentException("'stream' is required")
            );

        S2Utils.validateStreamName(stream);

        URI uri = buildStreamUri(runContext, stream);

        HttpRequest request = createRequest(
            runContext,
            "DELETE",
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

        if (statusCode != 202) {
            throw new IllegalStateException(
                buildErrorMessage(
                    statusCode,
                    response.getBody()
                )
            );
        }

        return null;
    }

    URI buildStreamUri(
        RunContext runContext,
        String stream
    ) throws Exception {
        var encodedStream = S2Utils.encodeStreamName(stream);

        return URI.create(
            basinBaseUrl(runContext) + "/streams/" + encodedStream
        );
    }

    static String buildErrorMessage(
        int statusCode,
        String body
    ) {
        var hint = switch (statusCode) {
            case 400 ->
                "Bad request. Check the stream name.";
            case 401 ->
                "Unauthorized. Check that 'accessToken' is valid and not expired.";
            case 403 ->
                "Permission denied or quota exhausted. Check the access token permissions and quota.";
            case 404 ->
                "Stream not found. Check the 'stream' value.";
            case 408 ->
                "Request timed out. This operation can be retried.";
            case 409 ->
                "Stream deletion is already pending or the resource is in conflict. Retry after the pending condition clears.";
            default ->
                "Unexpected response from S2.";
        };

        return "S2 delete stream failed with HTTP status "
            + statusCode
            + ". "
            + hint
            + " Response: "
            + body;
    }
}