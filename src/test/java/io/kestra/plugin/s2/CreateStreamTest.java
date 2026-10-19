package io.kestra.plugin.s2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CreateStreamTest {

    @Inject
    private RunContextFactory runContextFactory;

    private RunContext createRunContext() {
        Flow flow = Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("test-flow")
            .build();

        return runContextFactory.of(flow, Map.of());
    }

    private Path createInitFile() throws Exception {
        var initFile = Files.createTempFile("s2-init-", ".json");

        Files.writeString(
            initFile,
            """
            {
              "basins": [
                {
                  "name": "test-basin",
                  "streams": []
                }
              ]
            }
            """
        );

        Files.setPosixFilePermissions(
            initFile,
            Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ
            )
        );

        return initFile;
    }

    @Test
    void shouldCreateStream() throws Exception {
        var initFile = createInitFile();

        try (GenericContainer<?> s2 = new GenericContainer<>(
            "ghcr.io/s2-streamstore/s2:0.43.1"
        )
            .withCommand("lite", "--port", "80", "--init-file", "/s2-init.json")
            .withFileSystemBind(
                initFile.toString(),
                "/s2-init.json",
                BindMode.READ_ONLY
            )
            .withExposedPorts(80)
            .waitingFor(
                Wait.forLogMessage(".*starting plain http server.*\\n", 1)
            )
        ) {
            s2.start();

            var endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            CreateStream task = CreateStream.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .build();

            RunContext runContext = createRunContext();

            CreateStream.Output output = task.run(runContext);

            assertThat(output.getName(), is("orders"));
            assertThat(output.getCreatedAt(), is(notNullValue()));
        }
    }

    @Test
    void shouldKeepExplicitBasinEndpoint() throws Exception {
        CreateStream task = CreateStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        RunContext runContext = createRunContext();

        URI uri = task.buildStreamsUri(runContext);

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams")
        );
    }

    @Test
    void shouldResolveBasinPlaceholderInEndpoint() throws Exception {
        CreateStream task = CreateStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("https://{basin}.b.s2.dev/v1")
            )
            .build();

        RunContext runContext = createRunContext();

        URI uri = task.buildStreamsUri(runContext);

        assertThat(
            uri.toString(),
            is("https://test-basin.b.s2.dev/v1/streams")
        );
    }

    @Test
    void shouldRequireStream() throws Exception {
        CreateStream task = CreateStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(
            exception.getMessage(),
            is("'stream' is required")
        );
    }

    @Test
    void shouldRejectNulInStreamName() throws Exception {
        CreateStream task = CreateStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .stream(Property.ofValue("orders\0test"))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(
            exception.getMessage(),
            containsString("NUL")
        );
    }

    @Test
    void shouldRejectStreamNameOver512Bytes() throws Exception {
        var stream = "a".repeat(513);

        CreateStream task = CreateStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .stream(Property.ofValue(stream))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(
            exception.getMessage(),
            containsString("512 bytes")
        );
    }

    @Test
    void shouldExplainBadRequest() {
        assertThat(
            CreateStream.buildErrorMessage(400, "bad_request"),
            containsString("stream name")
        );
    }

    @Test
    void shouldExplainForbidden() {
        assertThat(
            CreateStream.buildErrorMessage(403, "forbidden"),
            containsString("access token permissions")
        );
    }

    @Test
    void shouldExplainBasinNotFound() {
        assertThat(
            CreateStream.buildErrorMessage(404, "basin_not_found"),
            containsString("'basin'")
        );
    }

    @Test
    void shouldMarkTimeoutAsRetryableAndExplainConflict() {
        assertThat(
            CreateStream.buildErrorMessage(408, "timeout"),
            containsString("can be retried")
        );

        assertThat(
            CreateStream.buildErrorMessage(409, "conflict"),
            containsString("Check the S2 error code and message before retrying")
        );
    }

    @Test
    void shouldIncludeStatusAndResponseBody() {
        String message = CreateStream.buildErrorMessage(
            404,
            "basin_not_found"
        );

        assertThat(
            message,
            containsString("HTTP status 404")
        );

        assertThat(
            message,
            containsString("basin_not_found")
        );
    }

    @Test
    void shouldExplainUnauthorized() {
        assertThat(
            CreateStream.buildErrorMessage(401, "unauthorized"),
            containsString("accessToken")
        );
    }

    @Test
    void shouldFailWhenStreamAlreadyExists() throws Exception {
        var initFile = createInitFile();

        try (GenericContainer<?> s2 = new GenericContainer<>(
            "ghcr.io/s2-streamstore/s2:0.43.1"
        )
            .withCommand("lite", "--port", "80", "--init-file", "/s2-init.json")
            .withFileSystemBind(
                initFile.toString(),
                "/s2-init.json",
                BindMode.READ_ONLY
            )
            .withExposedPorts(80)
            .waitingFor(
                Wait.forLogMessage(".*starting plain http server.*\\n", 1)
            )
        ) {
            s2.start();

            var endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            CreateStream task = CreateStream.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .build();

            RunContext runContext = createRunContext();

            task.run(runContext);

            IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> task.run(runContext)
            );

            assertThat(
                exception.getMessage(),
                containsString("HTTP status 409")
            );
        }
    }

    @Test
    void shouldRejectEmptyResponseBody() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> CreateStream.toOutput("")
        );

        assertThat(exception.getMessage(), containsString("Empty response"));
    }

    @Test
    void shouldRejectNullResponseBody() {
        assertThrows(
            IllegalStateException.class,
            () -> CreateStream.toOutput(null)
        );
    }

    @Test
    void shouldRejectNullJsonResponse() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> CreateStream.toOutput("null")
        );

        assertThat(exception.getMessage(), containsString("Incomplete response"));
    }

    @Test
    void shouldParseValidResponse() throws Exception {
        CreateStream.Output output = CreateStream.toOutput(
            "{\"name\":\"orders\",\"created_at\":\"2026-10-15T10:00:00Z\"}"
        );

        assertThat(output.getName(), is("orders"));
        assertThat(output.getCreatedAt(), is("2026-10-15T10:00:00Z"));
    }
}