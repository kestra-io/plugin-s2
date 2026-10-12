package io.kestra.plugin.s2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AppendTest {

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
    void shouldAppendRecords() throws Exception {
        Path initFile = createInitFile();

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

            String endpoint = "http://localhost:" + s2.getMappedPort(80) + "/v1";

            Append task = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(List.of(
                    Map.of(
                        "body", "order created",
                        "headers", List.of(
                            List.of("source", "kestra"),
                            List.of("type", "plugin")
                        )
                    )
                )))
                .build();

            RunContext runContext = createRunContext();

            Append.Output output = task.run(runContext);

            assertThat(output.getStart(), is(0L));
            assertThat(output.getEnd(), is(1L));
            assertThat(output.getTail(), is(1L));
        }
    }

    @Test
    void shouldKeepExplicitBasinEndpoint() throws Exception {
        Append task = Append.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        RunContext runContext = createRunContext();

        URI uri = task.buildRecordsUri(runContext, "orders");

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams/orders/records")
        );
    }

    @Test
    void shouldResolveBasinPlaceholderInEndpoint() throws Exception {
        Append task = Append.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("https://{basin}.b.s2.dev/v1")
            )
            .build();

        RunContext runContext = createRunContext();

        URI uri = task.buildRecordsUri(runContext, "orders");

        assertThat(
            uri.toString(),
            is("https://test-basin.b.s2.dev/v1/streams/orders/records")
        );
    }

    @Test
    void shouldRequireStream() throws Exception {
        Append task = Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .records(Data.from(List.of(
                Map.of("body", "order created")
            )))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("'stream' is required"));
    }

    @Test
    void shouldRequireRecords() throws Exception {
        Append task = Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("'records' is required"));
    }

    @Test
    void shouldRequireAtLeastOneRecord() throws Exception {
        Append task = Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .records(Data.from(List.of()))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(
            exception.getMessage(),
            is("'records' must contain at least one record")
        );
    }

    @Test
    void shouldRejectMoreThan1000Records() throws Exception {
        List<Map<String, String>> records = IntStream.range(0, 1001)
            .mapToObj(i -> Map.of("body", "record-" + i))
            .toList();

        Append task = Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .records(Data.from(records))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(
            exception.getMessage(),
            is("S2 append supports at most 1000 records per request")
        );
    }

    @Test
    void shouldExplainUnauthorized() {
        assertThat(
            Append.buildErrorMessage(401, "bad"),
            containsString("accessToken")
        );
    }

    @Test
    void shouldExplainForbidden() {
        assertThat(
            Append.buildErrorMessage(403, "bad"),
            containsString("quota")
        );
    }

    @Test
    void shouldExplainStreamNotFound() {
        assertThat(
            Append.buildErrorMessage(404, "stream_not_found"),
            containsString("Check the 'stream'")
        );
    }

    @Test
    void shouldMarkTimeoutAsRetryable() {
        assertThat(
            Append.buildErrorMessage(408, "bad"),
            containsString("can be retried")
        );
    }

    @Test
    void shouldExplainConflictWithoutAssumingRetryable() {
        String message = Append.buildErrorMessage(
            409,
            "transaction_conflict"
        );

        assertThat(
            message,
            containsString("Conflict")
        );

        assertThat(
            message,
            containsString("Check the S2 error code and message before retrying")
        );
    }

    @Test
    void shouldIncludeStatusAndResponseBody() {
        String message = Append.buildErrorMessage(
            404,
            "stream_not_found"
        );

        assertThat(message, containsString("HTTP status 404"));
        assertThat(message, containsString("stream_not_found"));
    }

    @Test
    void shouldEncodeStreamNameInPath() throws Exception {
        Append task = Append.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .build();

        URI uri = task.buildRecordsUri(createRunContext(), "my stream/1");

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams/my%20stream%2F1/records")
        );
    }

    @Test
    void shouldAppendRecordsFromStorageFile() throws Exception {
        Path initFile = createInitFile();

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

            String endpoint = "http://localhost:" + s2.getMappedPort(80) + "/v1";

            Append seed = Append.builder()
                .id("append")
                .type(Append.class.getName())
                .build();

            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, seed, Map.of());

            File file = runContext.workingDir().createTempFile(".ion").toFile();

            try (OutputStream outputStream = new FileOutputStream(file)) {
                FileSerde.write(outputStream, Append.Record.builder().body("from file 1").build());
                FileSerde.write(outputStream, Append.Record.builder().body("from file 2").build());
            }

            URI uri = runContext.storage().putFile(file);

            Append task = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(uri.toString()))
                .build();

            Append.Output output = task.run(runContext);

            assertThat(output.getStart(), is(0L));
            assertThat(output.getEnd(), is(2L));
            assertThat(output.getTail(), is(2L));
        }
    }
}