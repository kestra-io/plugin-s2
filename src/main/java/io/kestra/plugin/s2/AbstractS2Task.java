package io.kestra.plugin.s2;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS2Task extends Task {

    @NotNull
    @Schema(
        title = "S2 access token",
        description = "Access token used to authenticate with S2"
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> accessToken;

    @NotNull
    @Schema(
        title = "S2 basin",
        description = "S2 basin containing the streams"
    )
    @PluginProperty(group = "connection")
    private Property<String> basin;

    @Schema(
        title = "Basin endpoint",
        description = "S2 basin API endpoint. The `{basin}` placeholder is replaced by the basin name. When the endpoint has no placeholder, the basin is sent in the `S2-Basin` header, which is how a custom endpoint such as s2-lite is addressed."
    )
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> basinEndpoint =
        Property.ofValue("https://{basin}.b.s2.dev/v1");

    protected HttpConfiguration getHttpConfiguration() {
        return HttpConfiguration.builder()
            .allowFailed(Property.ofValue(true))
            .build();
    }

    protected String basinBaseUrl(RunContext runContext) throws Exception {
       return S2Utils.basinBaseUrl(runContext, this.getBasin(), this.getBasinEndpoint());
    }

    protected URI buildRecordsUri(
        RunContext runContext,
        String stream
    ) throws Exception {
        var encodedStream = S2Utils.encodeStreamName(stream);

        return URI.create(
            basinBaseUrl(runContext) + "/streams/" + encodedStream + "/records"
        );
    }

    protected HttpClient createHttpClient(RunContext runContext) throws Exception {
        return HttpClient.builder()
            .runContext(runContext)
            .configuration(getHttpConfiguration())
            .build();
    }

    protected String getAccessToken(RunContext runContext) throws Exception {
        return S2Utils.getAccessToken(runContext, this.accessToken);
    }

    protected HttpRequest createRequest(
        RunContext runContext,
        String method,
        String url,
        HttpRequest.RequestBody body,
        Map<String, List<String>> extraHeaders
    ) throws Exception {
        Map<String, List<String>> headers = new HashMap<>(extraHeaders);

        headers.put(
            "Authorization",
            List.of("Bearer " + getAccessToken(runContext))
        );

        var basinEndpoint = runContext.render(this.getBasinEndpoint())
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'basinEndpoint' is required")
            );

        if (!basinEndpoint.contains("{basin}")) {
            var basin = runContext.render(this.getBasin())
                .as(String.class)
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .orElseThrow(() ->
                    new IllegalArgumentException("S2 'basin' is required")
                );

            headers.put(
                "S2-Basin",
                List.of(basin)
            );
        }

        return HttpRequest.of(
            URI.create(url),
            method,
            body,
            headers
        );
    }

    protected <T> HttpResponse<T> execute(
        RunContext runContext,
        HttpRequest request,
        Class<T> responseType
    ) throws Exception {
        try (HttpClient httpClient = createHttpClient(runContext)) {
            return httpClient.request(request, responseType);
        }
    }
}