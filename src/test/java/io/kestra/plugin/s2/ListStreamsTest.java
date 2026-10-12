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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ListStreamsTest {

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
                      "name": "alpha"
                    },
                    {
                      "name": "beta"
                    },
                    {
                      "name": "orders"
                    },
                    {
                      "name": "orders_archive"
                    },
                    {
                      "name": "payments"
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
    void shouldListStreams() throws Exception {
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

            ListStreams task = ListStreams.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .build();

            ListStreams.Output output = task.run(createRunContext());

            assertThat(output.getStreams(), hasSize(5));
            assertThat(
                output.getStreams().get(0).getName(),
                is("alpha")
            );
            assertThat(
                output.getStreams().get(1).getName(),
                is("beta")
            );
            assertThat(
                output.getStreams().get(2).getName(),
                is("orders")
            );
            assertThat(
                output.getStreams().get(3).getName(),
                is("orders_archive")
            );
            assertThat(
                output.getStreams().get(4).getName(),
                is("payments")
            );
            assertThat(output.isHasMore(), is(false));
        }
    }

    @Test
    void shouldFilterStreamsByPrefix() throws Exception {
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

            ListStreams task = ListStreams.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .prefix(Property.ofValue("orders"))
                .build();

            ListStreams.Output output = task.run(createRunContext());

            assertThat(output.getStreams(), hasSize(2));
            assertThat(
                output.getStreams().get(0).getName(),
                is("orders")
            );
            assertThat(
                output.getStreams().get(1).getName(),
                is("orders_archive")
            );
            assertThat(output.isHasMore(), is(false));
        }
    }

    @Test
    void shouldUseStartAfter() throws Exception {
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

            ListStreams task = ListStreams.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .startAfter(Property.ofValue("beta"))
                .build();

            ListStreams.Output output = task.run(createRunContext());

            assertThat(output.getStreams(), hasSize(3));
            assertThat(
                output.getStreams().get(0).getName(),
                is("orders")
            );
            assertThat(
                output.getStreams().get(1).getName(),
                is("orders_archive")
            );
            assertThat(
                output.getStreams().get(2).getName(),
                is("payments")
            );
        }
    }

    @Test
    void shouldRespectLimit() throws Exception {
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

            ListStreams task = ListStreams.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .limit(Property.ofValue(2))
                .build();

            ListStreams.Output output = task.run(createRunContext());

            assertThat(output.getStreams(), hasSize(2));
            assertThat(
                output.getStreams().get(0).getName(),
                is("alpha")
            );
            assertThat(
                output.getStreams().get(1).getName(),
                is("beta")
            );
            assertThat(output.isHasMore(), is(true));
        }
    }

    @Test
    void shouldUseDefaultLimit() throws Exception {
        ListStreams task = ListStreams.builder()
            .build();

        RunContext runContext = createRunContext();

        assertThat(
            task.buildQueryString(runContext),
            is("limit=1000")
        );
    }

    @Test
    void shouldBuildQueryString() throws Exception {
        ListStreams task = ListStreams.builder()
            .prefix(Property.ofValue("orders"))
            .startAfter(Property.ofValue("beta"))
            .limit(Property.ofValue(100))
            .build();

        RunContext runContext = createRunContext();

        assertThat(
            task.buildQueryString(runContext),
            is("prefix=orders&start_after=beta&limit=100")
        );
    }

    @Test
    void shouldRejectInvalidLimit() throws Exception {
        ListStreams task = ListStreams.builder()
            .limit(Property.ofValue(1001))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            is("'limit' must be between 0 and 1000")
        );
    }

    @Test
    void shouldRejectNegativeLimit() throws Exception {
        ListStreams task = ListStreams.builder()
            .limit(Property.ofValue(-1))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            is("'limit' must be between 0 and 1000")
        );
    }

    @Test
    void shouldRejectNulInPrefix() throws Exception {
        ListStreams task = ListStreams.builder()
            .prefix(Property.ofValue("orders\0"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            containsString("NUL")
        );
    }

    @Test
    void shouldRejectNulInStartAfter() throws Exception {
        ListStreams task = ListStreams.builder()
            .startAfter(Property.ofValue("beta\0"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.buildQueryString(createRunContext())
        );

        assertThat(
            exception.getMessage(),
            containsString("NUL")
        );
    }

    @Test
    void shouldKeepExplicitBasinEndpoint() throws Exception {
        ListStreams task = ListStreams.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("http://localhost:1234/v1")
            )
            .build();

        URI uri = task.buildStreamsUri(createRunContext());

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams?limit=1000")
        );
    }

    @Test
    void shouldResolveBasinPlaceholderInEndpoint() throws Exception {
        ListStreams task = ListStreams.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(
                Property.ofValue("https://{basin}.b.s2.dev/v1")
            )
            .build();

        URI uri = task.buildStreamsUri(createRunContext());

        assertThat(
            uri.toString(),
            is("https://test-basin.b.s2.dev/v1/streams?limit=1000")
        );
    }

    @Test
    void shouldExplainBadRequest() {
        assertThat(
            ListStreams.buildErrorMessage(400, "bad_request"),
            containsString("prefix, startAfter and limit")
        );
    }

    @Test
    void shouldExplainForbidden() {
        assertThat(
            ListStreams.buildErrorMessage(403, "forbidden"),
            containsString("access token permissions")
        );
    }

    @Test
    void shouldExplainBasinNotFound() {
        assertThat(
            ListStreams.buildErrorMessage(404, "basin_not_found"),
            containsString("'basin'")
        );
    }

    @Test
    void shouldExplainTimeout() {
        assertThat(
            ListStreams.buildErrorMessage(408, "timeout"),
            containsString("can be retried")
        );
    }

    @Test
    void shouldEncodeQueryValues() throws Exception {
        ListStreams task = ListStreams.builder()
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .prefix(Property.ofValue("a b/c"))
            .build();

        URI uri = task.buildStreamsUri(createRunContext());

        assertThat(
            uri.toString(),
            is("http://localhost:1234/v1/streams?prefix=a+b%2Fc&limit=1000")
        );
    }

    @Test
    void shouldExplainUnauthorized() {
        assertThat(
            ListStreams.buildErrorMessage(401, "unauthorized"),
            containsString("accessToken")
        );
    }
}