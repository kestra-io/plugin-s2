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
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DeleteStreamTest {

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
        Path initFile = Files.createTempFile("s2-init-", ".json");

        Files.writeString(
            initFile,
            """
            {
              "basins": [
                {
                  "name": "test-basin",
                  "streams": [
                    {
                      "name": "orders"
                    }
                  ]
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
    void shouldDeleteStream() throws Exception {
        Path initFile = createInitFile();

        try (GenericContainer<?> s2 = new GenericContainer<>(
            "ghcr.io/s2-streamstore/s2:0.43.1"
        )
            .withCommand(
                "lite",
                "--port",
                "80",
                "--init-file",
                "/s2-init.json"
            )
            .withFileSystemBind(
                initFile.toString(),
                "/s2-init.json",
                BindMode.READ_ONLY
            )
            .withExposedPorts(80)
            .waitingFor(
                Wait.forLogMessage(
                    ".*starting plain http server.*\\n",
                    1
                )
            )
        ) {
            s2.start();

            String endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            DeleteStream task = DeleteStream.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .build();

            task.run(createRunContext());
        }
    }

    @Test
    void shouldBuildStreamUri() throws Exception {
        DeleteStream task = DeleteStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        URI uri = task.buildStreamUri(
            createRunContext(),
            "orders"
        );

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams/orders")
        );
    }

    @Test
    void shouldResolveBasinPlaceholderInEndpoint() throws Exception {
        DeleteStream task = DeleteStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("https://{basin}.b.s2.dev/v1")
            )
            .build();

        URI uri = task.buildStreamUri(
            createRunContext(),
            "orders"
        );

        assertThat(
            uri.toString(),
            is("https://test-basin.b.s2.dev/v1/streams/orders")
        );
    }

    @Test
    void shouldRequireStream() throws Exception {
        DeleteStream task = DeleteStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            is("'stream' is required")
        );
    }

    @Test
    void shouldRejectNulInStreamName() throws Exception {
        DeleteStream task = DeleteStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .stream(Property.ofValue("orders\0test"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            containsString("NUL")
        );
    }

    @Test
    void shouldRejectStreamNameOver512Bytes() throws Exception {
        String stream = "a".repeat(513);

        DeleteStream task = DeleteStream.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .stream(Property.ofValue(stream))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            containsString("512 bytes")
        );
    }

    @Test
    void shouldAcceptStreamNameWith512Bytes() throws Exception {
        String stream = "a".repeat(512);

        DeleteStream task = DeleteStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .stream(Property.ofValue(stream))
            .build();

        URI uri = task.buildStreamUri(
            createRunContext(),
            stream
        );

        assertThat(
            uri.getPath(),
            containsString("/streams/")
        );
    }

    @Test
    void shouldExplainBadRequest() {
        assertThat(
            DeleteStream.buildErrorMessage(
                400,
                "bad_request"
            ),
            containsString("stream name")
        );
    }

    @Test
    void shouldExplainForbidden() {
        assertThat(
            DeleteStream.buildErrorMessage(
                403,
                "permission_denied"
            ),
            containsString("Permission denied")
        );
    }

    @Test
    void shouldExplainStreamNotFound() {
        assertThat(
            DeleteStream.buildErrorMessage(
                404,
                "stream_not_found"
            ),
            containsString("Stream not found")
        );
    }

    @Test
    void shouldMarkTimeoutAsRetryable() {
        assertThat(
            DeleteStream.buildErrorMessage(
                408,
                "request_timeout"
            ),
            containsString("can be retried")
        );
    }

    @Test
    void shouldExplainPendingDeletion() {
        assertThat(
            DeleteStream.buildErrorMessage(
                409,
                "stream_deletion_pending"
            ),
            containsString("deletion is already pending")
        );
    }

    @Test
    void shouldIncludeStatusAndResponseBody() {
        String message = DeleteStream.buildErrorMessage(
            404,
            "stream_not_found"
        );

        assertThat(
            message,
            containsString("HTTP status 404")
        );

        assertThat(
            message,
            containsString("stream_not_found")
        );
    }

    @Test
    void shouldEncodeStreamNameInPath() throws Exception {
        DeleteStream task = DeleteStream.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        URI uri = task.buildStreamUri(
            createRunContext(),
            "my stream/1"
        );

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams/my%20stream%2F1")
        );
    }

    @Test
    void shouldExplainUnauthorized() {
        assertThat(
            DeleteStream.buildErrorMessage(401, "unauthorized"),
            containsString("accessToken")
        );
    }
}