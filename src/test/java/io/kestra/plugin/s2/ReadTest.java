package io.kestra.plugin.s2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ReadTest {

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
    void shouldReadRecords() throws Exception {
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

            String endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            Append append = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(List.of(
                    Map.of("body", "order created"),
                    Map.of("body", "order shipped"),
                    Map.of("body", "order delivered")
                )))
                .build();

            RunContext runContext = createRunContext();

            append.run(runContext);

            Read read = Read.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .maxRecords(Property.ofValue(3))
                .build();

            Read.Output output = read.run(runContext);

            assertThat(output.getSize(), is(3));
            assertThat(output.getRecords().size(), is(3));

            assertThat(
                output.getRecords().get(0).getBody(),
                is("order created")
            );

            assertThat(
                output.getRecords().get(1).getBody(),
                is("order shipped")
            );

            assertThat(
                output.getRecords().get(2).getBody(),
                is("order delivered")
            );
        }
    }

    @Test
    void shouldReadOneRecord() throws Exception {
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

            String endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            RunContext runContext = createRunContext();

            Append append = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(List.of(
                    Map.of("body", "order created"),
                    Map.of("body", "order shipped"),
                    Map.of("body", "order delivered")
                )))
                .build();

            append.run(runContext);

            Read read = Read.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .maxRecords(Property.ofValue(100))
                .build();

            Read.Output output = read.run(runContext);

            assertThat(output.getSize(), is(1));
            assertThat(
                output.getRecord().getBody(),
                is("order delivered")
            );
        }
    }

    @Test
    void shouldStoreRecords() throws Exception {
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

            String endpoint =
                "http://localhost:"
                    + s2.getMappedPort(80)
                    + "/v1";

            RunContext runContext = createRunContext();

            Append append = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(List.of(
                    Map.of("body", "order created"),
                    Map.of("body", "order shipped"),
                    Map.of("body", "order delivered")
                )))
                .build();

            append.run(runContext);

            Read read = Read.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .fetchType(Property.ofValue(FetchType.STORE))
                .maxRecords(Property.ofValue(3))
                .build();

            Read.Output output = read.run(runContext);

            assertThat(output.getUri(), is(notNullValue()));
            assertThat(output.getSize(), is(3));
        }
    }

    @Test
    void shouldReadLatestRecordsByDefault() throws Exception {
        Read task = Read.builder()
            .basin(Property.ofValue("test-basin"))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        String query = task.buildQueryString(runContext);

        assertThat(query, containsString("tail_offset=100"));
        assertThat(query, containsString("count=100"));
    }

    @Test
    void shouldReadFromSequenceNumber() throws Exception {
        Read task = Read.builder()
            .seqNum(Property.ofValue(50L))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        String query = task.buildQueryString(runContext);

        assertThat(query, containsString("seq_num=50"));
        assertThat(query, containsString("count=100"));
    }

    @Test
    void shouldReadFromTimestamp() throws Exception {
        Read task = Read.builder()
            .timestamp(Property.ofValue(123456789L))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        String query = task.buildQueryString(runContext);

        assertThat(query, containsString("timestamp=123456789"));
        assertThat(query, containsString("count=100"));
    }

    @Test
    void shouldReadFromTailOffset() throws Exception {
        Read task = Read.builder()
            .tailOffset(Property.ofValue(20L))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        String query = task.buildQueryString(runContext);

        assertThat(query, containsString("tail_offset=20"));
        assertThat(query, containsString("count=100"));
    }

    @Test
    void shouldRejectMultipleStartSelectors() throws Exception {
        Read task = Read.builder()
            .seqNum(Property.ofValue(10L))
            .timestamp(Property.ofValue(123456789L))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(runContext)
        );

        assertThat(
            exception.getMessage(),
            is("Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set")
        );
    }

    @Test
    void shouldLimitFetchOneToOneRecord() throws Exception {
        Read task = Read.builder()
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        String query = task.buildQueryString(runContext);

        assertThat(query, containsString("tail_offset=1"));
        assertThat(query, containsString("count=1"));
    }

    @Test
    void shouldRejectInvalidMaxRecords() throws Exception {
        Read task = Read.builder()
            .maxRecords(Property.ofValue(1001))
            .build();

        RunContext runContext = createRunContext();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(runContext)
        );

        assertThat(
            exception.getMessage(),
            is("'maxRecords' must be between 1 and 1000")
        );
    }

    @Test
    void shouldExplainUnauthorized() {
        assertThat(
            Read.buildErrorMessage(401, "bad"),
            containsString("accessToken")
        );
    }

    @Test
    void shouldExplainStreamNotFound() {
        assertThat(
            Read.buildErrorMessage(404, "stream_not_found"),
            containsString("Check the 'stream'")
        );
    }

    @Test
    void shouldExplainInvalidReadPosition() {
        assertThat(
            Read.buildErrorMessage(416, "tail"),
            containsString("starting position")
        );
    }

    @Test
    void shouldIncludeStatusAndResponseBody() {
        String message = Read.buildErrorMessage(
            404,
            "stream_not_found"
        );

        assertThat(message, containsString("HTTP status 404"));
        assertThat(message, containsString("stream_not_found"));
    }

    @Test
    void shouldExplainTimeout() {
        assertThat(
            Read.buildErrorMessage(408, "request_timeout"),
            containsString("Retry the task if appropriate")
        );
    }

    @Test
    void shouldExplainConflict() {
        assertThat(
            Read.buildErrorMessage(409, "transaction_conflict"),
            containsString("Check the S2 error code and message before retrying")
        );
    }

    @Test
    void shouldFailWhenStreamMissing() throws Exception {
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

            Read read = Read.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue("http://localhost:" + s2.getMappedPort(80) + "/v1"))
                .stream(Property.ofValue("missing"))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build();

            IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> read.run(createRunContext())
            );

            assertThat(exception.getMessage(), containsString("HTTP status 404"));
        }
    }
}