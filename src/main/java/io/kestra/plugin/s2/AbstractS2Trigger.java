package io.kestra.plugin.s2;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractS2Trigger extends AbstractTrigger {

    @Schema(
        title = "S2 access token",
        description = "Access token used to authenticate with S2"
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> accessToken;

    @Schema(
        title = "S2 basin",
        description = "S2 basin containing the stream"
    )
    @PluginProperty(group = "connection")
    private Property<String> basin;

    @Schema(
        title = "Account endpoint",
        description = "S2 account API endpoint"
    )
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> accountEndpoint =
        Property.ofValue("https://a.s2.dev/v1");

    @Schema(
        title = "Basin endpoint",
        description = "S2 basin API endpoint. The `{basin}` placeholder is replaced by the basin name. When the endpoint has no placeholder, the basin is sent in the `S2-Basin` header, which is how a custom endpoint such as s2-lite is addressed."
    )
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> basinEndpoint =
        Property.ofValue("https://{basin}.b.s2.dev/v1");

    protected HttpClient createHttpClient(RunContext runContext) throws Exception {
        return HttpClient.builder()
            .runContext(runContext)
            .configuration(HttpConfiguration.builder().build())
            .build();
    }

    protected String getAccessToken(RunContext runContext) throws Exception {
        return runContext.render(this.accessToken)
            .as(String.class)
            .map(String::trim)
            .filter(token -> !token.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'accessToken' is required")
            );
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

        String basinEndpoint = runContext.render(this.getBasinEndpoint())
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'basinEndpoint' is required")
            );

        if (!basinEndpoint.contains("{basin}")) {
            String basin = runContext.render(this.getBasin())
                .as(String.class)
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .orElseThrow(() ->
                    new IllegalArgumentException("S2 'basin' is required")
                );

            headers.put("S2-Basin", List.of(basin));
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

    protected String buildWatermarkKey(
        RunContext runContext,
        String stream
    ) {
        String flowId = runContext.flowInfo().id();
        String triggerId = this.getId();

        return "s2_watermark_"
            + flowId.length() + "_" + flowId + "_"
            + triggerId.length() + "_" + triggerId + "_"
            + stream.length() + "_" + stream;
    }

    protected long readLastSeen(
        RunContext runContext,
        String key
    ) throws Exception {
        KVStore kv = runContext.namespaceKv(
            runContext.flowInfo().namespace()
        );

        Optional<KVValue> value = kv.getValue(key);

        if (value.isEmpty()) {
            return -1L;
        }

        return Long.parseLong(value.get().value().toString());
    }

    protected void writeLastSeen(
        RunContext runContext,
        String key,
        long sequence
    ) throws Exception {
        KVStore kv = runContext.namespaceKv(
            runContext.flowInfo().namespace()
        );

        kv.put(
            key,
            new KVValueAndMetadata(
                new KVMetadata(null, (Duration) null),
                sequence
            ),
            true
        );
    }
}