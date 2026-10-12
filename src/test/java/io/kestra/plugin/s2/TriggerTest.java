package io.kestra.plugin.s2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.RunContextInitializer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private RunContextInitializer runContextInitializer;

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
    void shouldTriggerForNewRecords() throws Exception {
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
                "http://localhost:" + s2.getMappedPort(80) + "/v1";

            Flow flow = Flow.builder()
                .tenantId("main")
                .namespace("test")
                .id("test-flow-" + System.nanoTime())
                .build();

            Trigger trigger = Trigger.builder()
                .id("watch")
                .type(Trigger.class.getName())
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .maxRecords(Property.ofValue(100))
                .build();

            TriggerContext triggerContext = TriggerContext.builder()
                .namespace(flow.getNamespace())
                .flowId(flow.getId())
                .triggerId(trigger.getId())
                .date(ZonedDateTime.now())
                .tenantId(flow.getTenantId())
                .build();

            RunContext runContext = runContextInitializer.forScheduler(
                (DefaultRunContext) runContextFactory.of(flow, trigger),
                triggerContext,
                trigger
            );

            RunContext taskRunContext = runContextFactory.of(flow, Map.of());

            Append append = Append.builder()
                .accessToken(Property.ofValue("test-token"))
                .basin(Property.ofValue("test-basin"))
                .basinEndpoint(Property.ofValue(endpoint))
                .stream(Property.ofValue("orders"))
                .records(Data.from(List.of(
                    Map.of("body", "record 0"),
                    Map.of("body", "record 1"),
                    Map.of("body", "record 2")
                )))
                .build();

            append.run(taskRunContext);

            ConditionContext conditionContext = ConditionContext.builder()
                .runContext(runContext)
                .flow(flow)
                .build();

            Optional<Execution> result = trigger.evaluate(
                conditionContext,
                triggerContext
            );

            assertThat(result.isPresent(), is(true));

            Optional<Execution> secondResult = trigger.evaluate(
                conditionContext,
                triggerContext
            );

            assertThat(secondResult.isPresent(), is(false));
        }
    }

    @Test
    void shouldRequireStream() throws Exception {
        Flow flow = Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("test-flow-validation")
            .build();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .maxRecords(Property.ofValue(100))
            .build();

        RunContext runContext = runContextFactory.of(flow, Map.of());

        TriggerContext triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(exception.getMessage(), is("'stream' is required"));
    }

    @Test
    void shouldRejectInvalidMaxRecords() throws Exception {
        Flow flow = Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("test-flow-invalid-max")
            .build();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .maxRecords(Property.ofValue(0))
            .build();

        RunContext runContext = runContextFactory.of(flow, Map.of());

        TriggerContext triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(
            exception.getMessage(),
            is("'maxRecords' must be between 1 and 1000")
        );
    }

    @Test
    void shouldRejectTooManyMaxRecords() throws Exception {
        Flow flow = Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("test-flow-invalid-max")
            .build();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .maxRecords(Property.ofValue(1001))
            .build();

        RunContext runContext = runContextFactory.of(flow, Map.of());

        TriggerContext triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(
            exception.getMessage(),
            is("'maxRecords' must be between 1 and 1000")
        );
    }
}