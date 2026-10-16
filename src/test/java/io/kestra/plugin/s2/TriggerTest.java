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
import java.util.stream.Stream;

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
        var initFile = Files.createTempFile("s2-init-", ".json");

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

    private GenericContainer<?> container(Path initFile) {
        return new GenericContainer<>("ghcr.io/s2-streamstore/s2:0.43.1")
            .withCommand("lite", "--port", "80", "--init-file", "/s2-init.json")
            .withFileSystemBind(initFile.toString(), "/s2-init.json", BindMode.READ_ONLY)
            .withExposedPorts(80)
            .waitingFor(Wait.forLogMessage(".*starting plain http server.*\\n", 1));
    }

    private String endpoint(GenericContainer<?> s2) {
        return "http://localhost:" + s2.getMappedPort(80) + "/v1";
    }

    private Flow flow() {
        return Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id("test-flow-" + System.nanoTime())
            .build();
    }

    private Trigger trigger(String endpoint, Long seqNum) {
        return Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue(endpoint))
            .stream(Property.ofValue("orders"))
            .maxRecords(Property.ofValue(100))
            .seqNum(seqNum == null ? null : Property.ofValue(seqNum))
            .build();
    }

    private TriggerContext triggerContext(Flow flow, Trigger trigger) {
        return TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();
    }

    private ConditionContext conditionContext(Flow flow, Trigger trigger, TriggerContext triggerContext) {
        RunContext runContext = runContextInitializer.forScheduler(
            (DefaultRunContext) runContextFactory.of(flow, trigger),
            triggerContext,
            trigger
        );

        return ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();
    }

    private void append(Flow flow, String endpoint, String... bodies) throws Exception {
        List<Map<String, String>> records = Stream.of(bodies)
            .map(body -> Map.of("body", body))
            .toList();

        Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue(endpoint))
            .stream(Property.ofValue("orders"))
            .records(Data.from(records))
            .build()
            .run(runContextFactory.of(flow, Map.of()));
    }

    @Test
    void shouldSkipExistingRecordsOnFirstPoll() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow();

            append(flow, endpoint, "old 0", "old 1");

            Trigger trigger = trigger(endpoint, null);
            TriggerContext triggerContext = triggerContext(flow, trigger);
            ConditionContext conditionContext = conditionContext(flow, trigger, triggerContext);

            assertThat(trigger.evaluate(conditionContext, triggerContext).isPresent(), is(false));

            append(flow, endpoint, "new 2", "new 3", "new 4");

            var result = trigger.evaluate(conditionContext, triggerContext);

            assertThat(result.isPresent(), is(true));
            assertThat(((Number) result.get().getTrigger().getVariables().get("size")).intValue(), is(3));

            assertThat(trigger.evaluate(conditionContext, triggerContext).isPresent(), is(false));
        }
    }

    @Test
    void shouldNotSkipRecordsAppendedAfterFirstPollOnEmptyStream() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow();

            Trigger trigger = trigger(endpoint, null);
            TriggerContext triggerContext = triggerContext(flow, trigger);
            ConditionContext conditionContext = conditionContext(flow, trigger, triggerContext);

            assertThat(trigger.evaluate(conditionContext, triggerContext).isPresent(), is(false));

            append(flow, endpoint, "first", "second");

            var result = trigger.evaluate(conditionContext, triggerContext);

            assertThat(result.isPresent(), is(true));
            assertThat(((Number) result.get().getTrigger().getVariables().get("size")).intValue(), is(2));
        }
    }

    @Test
    void shouldReplayFromInitialSeqNum() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow();

            append(flow, endpoint, "record 0", "record 1", "record 2");

            Trigger trigger = trigger(endpoint, 0L);
            TriggerContext triggerContext = triggerContext(flow, trigger);
            ConditionContext conditionContext = conditionContext(flow, trigger, triggerContext);

            var result = trigger.evaluate(conditionContext, triggerContext);

            assertThat(result.isPresent(), is(true));
            assertThat(((Number) result.get().getTrigger().getVariables().get("size")).intValue(), is(3));

            assertThat(trigger.evaluate(conditionContext, triggerContext).isPresent(), is(false));
        }
    }

    @Test
    void shouldRejectMultipleInitialPositions() throws Exception {
        var flow = flow();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .seqNum(Property.ofValue(1L))
            .timestamp(Property.ofValue(2L))
            .build();

        TriggerContext triggerContext = triggerContext(flow, trigger);
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContextFactory.of(flow, Map.of()))
            .flow(flow)
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(
            exception.getMessage(),
            is("Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set")
        );
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