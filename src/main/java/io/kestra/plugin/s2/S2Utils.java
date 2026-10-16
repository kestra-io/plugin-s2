package io.kestra.plugin.s2;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class S2Utils {

    private S2Utils() {
    }

    public static String validateBasin(String basin) {
        if (basin == null || !basin.matches("[A-Za-z0-9-]+")) {
            throw new IllegalArgumentException(
                "Invalid S2 basin: only letters, digits, and hyphens are allowed"
            );
        }

        return basin;
    }

    public static void validateStreamName(String stream) {
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

    public static String encodeStreamName(String stream) {
        return URLEncoder.encode(stream, StandardCharsets.UTF_8)
            .replace("+", "%20");
    }

    public static String getAccessToken(
        RunContext runContext,
        Property<String> accessToken
    ) throws Exception {
        return runContext.render(accessToken)
            .as(String.class)
            .map(String::trim)
            .filter(token -> !token.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'accessToken' is required")
            );
    }

    public static String basinBaseUrl(
        RunContext runContext,
        Property<String> basinProperty,
        Property<String> endpointProperty
    ) throws Exception {
        var basin = runContext.render(basinProperty)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'basin' is required")
            );

        var endpoint = runContext.render(endpointProperty)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() ->
                new IllegalArgumentException("S2 'basinEndpoint' is required")
            );

        basin = validateBasin(basin);

        if (endpoint.contains("{basin}")) {
            endpoint = endpoint.replace("{basin}", basin);
        }

        var baseUri = URI.create(endpoint);

        String basePath = baseUri.getPath();

        if (basePath == null) {
            basePath = "";
        }

        if (basePath.endsWith("/")) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }

        return baseUri.getScheme() + "://" + baseUri.getAuthority() + basePath;
    }
}