package io.kestra.plugin.s2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

@KestraTest
class S2UtilsTest {

    @Inject
    private RunContextFactory runContextFactory;

    private RunContext createRunContext() {
        Flow flow = Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("s2-utils-test")
            .build();

        return runContextFactory.of(flow, Map.of());
    }

    @Test
    void shouldAcceptValidBasin() {
        assertEquals("my-basin-123", S2Utils.validateBasin("my-basin-123"));
        assertEquals("abc", S2Utils.validateBasin("abc"));
        assertEquals("123", S2Utils.validateBasin("123"));
    }

    @Test
    void shouldRejectInvalidBasin() {
        assertThrows(IllegalArgumentException.class,
            () -> S2Utils.validateBasin("evil.example/"));

        assertThrows(IllegalArgumentException.class,
            () -> S2Utils.validateBasin("evil.example"));

        assertThrows(IllegalArgumentException.class,
            () -> S2Utils.validateBasin("my basin"));

        assertThrows(IllegalArgumentException.class,
            () -> S2Utils.validateBasin("basin_name"));
    }

    @Test
    void shouldRejectNullBasin() {
        assertThrows(IllegalArgumentException.class,
            () -> S2Utils.validateBasin(null));
    }

    @Test
    void shouldAcceptValidStreamName() {
        assertDoesNotThrow(() -> S2Utils.validateStreamName("orders"));
        assertDoesNotThrow(() -> S2Utils.validateStreamName("my stream"));
    }

    @Test
    void shouldRejectStreamNameContainingNullByte() {
        assertThrows(
            IllegalArgumentException.class,
            () -> S2Utils.validateStreamName("orders\0invalid")
        );
    }

    @Test
    void shouldEnforceStreamNameByteLimit() {
        S2Utils.validateStreamName("a".repeat(512));

        assertThrows(
            IllegalArgumentException.class,
            () -> S2Utils.validateStreamName("a".repeat(513))
        );
    }


    @Test
    void shouldEncodeStreamName() {
        assertEquals("my%20stream", S2Utils.encodeStreamName("my stream"));
        assertEquals("orders%2F2026", S2Utils.encodeStreamName("orders/2026"));
        assertEquals("a%2Bb", S2Utils.encodeStreamName("a+b"));
    }

    @Test
    void shouldEncodeUnicodeStreamName() {
        assertEquals("%E6%B5%8B%E8%AF%95", S2Utils.encodeStreamName("测试"));
    }


    @Test
    void shouldRenderAndTrimAccessToken() throws Exception {
        RunContext runContext = createRunContext();

        assertEquals(
            "test-token",
            S2Utils.getAccessToken(
                runContext,
                Property.ofValue("  test-token  ")
            )
        );
    }

    @Test
    void shouldRejectBlankAccessToken() throws Exception {
        RunContext runContext = createRunContext();

        assertThrows(
            IllegalArgumentException.class,
            () -> S2Utils.getAccessToken(
                runContext,
                Property.ofValue("   ")
            )
        );
    }


    @Test
    void shouldBuildBasinBaseUrl() throws Exception {
        RunContext runContext = createRunContext();

        assertEquals(
            "http://localhost:8080/v1",
            S2Utils.basinBaseUrl(
                runContext,
                Property.ofValue("test-basin"),
                Property.ofValue("http://localhost:8080/v1")
            )
        );
    }

    @Test
    void shouldRemoveTrailingSlashFromBasinBaseUrl() throws Exception {
        RunContext runContext = createRunContext();

        assertEquals(
            "http://localhost:8080/v1",
            S2Utils.basinBaseUrl(
                runContext,
                Property.ofValue("test-basin"),
                Property.ofValue("http://localhost:8080/v1/")
            )
        );
    }

    @Test
    void shouldReplaceBasinPlaceholderInEndpoint() throws Exception {
        RunContext runContext = createRunContext();

        assertEquals(
            "http://localhost:8080/test-basin",
            S2Utils.basinBaseUrl(
                runContext,
                Property.ofValue("test-basin"),
                Property.ofValue("http://localhost:8080/{basin}")
            )
        );
    }
}