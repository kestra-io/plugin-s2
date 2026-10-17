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
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import io.kestra.core.http.HttpSseEvent;
import static org.hamcrest.Matchers.containsString;
import java.util.concurrent.atomic.AtomicReference;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class RealtimeTriggerTest {

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

    private Flow flow(String id) {
        return Flow.builder()
            .tenantId("main")
            .namespace("test")
            .id(id)
            .build();
    }

    private RealtimeTrigger trigger(String endpoint, String stream, Long seqNum) {
        return RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue(endpoint))
            .stream(Property.ofValue(stream))
            .seqNum(seqNum == null ? null : Property.ofValue(seqNum))
            .build();
    }

    private void append(Flow flow, String endpoint, String... bodies) throws Exception {
        List<Map<String, String>> records = Stream.of(bodies)
            .map(body -> Map.of("body", body))
            .toList();

        Append append = Append.builder()
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue(endpoint))
            .stream(Property.ofValue("orders"))
            .from(records)
            .build();

        append.run(runContextFactory.of(flow, Map.of()));
    }

    private TriggerContext triggerContext(Flow flow, RealtimeTrigger trigger) {
        return TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();
    }

    @Test
    void shouldEmitBacklogAndLiveRecordsThenStop() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());
            var runContext = runContextFactory.of(flow, Map.of());

            append(flow, endpoint, "record 0", "record 1");

            RealtimeTrigger trigger = trigger(endpoint, "orders", 0L);

            var received = new CopyOnWriteArrayList<Read.Record>();
            var errors = new CopyOnWriteArrayList<Throwable>();
            var backlog = new CountDownLatch(2);
            var live = new CountDownLatch(1);
            var completed = new CountDownLatch(1);

            Flux.from(trigger.publisher(runContext, trigger.createSseRequest(runContext, "orders")))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    record -> {
                        received.add(record);
                        if (received.size() <= 2) {
                            backlog.countDown();
                        } else {
                            live.countDown();
                        }
                    },
                    error -> {
                        errors.add(error);
                        completed.countDown();
                    },
                    completed::countDown
                );

            assertThat(backlog.await(30, TimeUnit.SECONDS), is(true));
            assertThat(received.get(0).getSeqNum(), is(0L));
            assertThat(received.get(1).getSeqNum(), is(1L));
            assertThat(received.get(0).getBody(), is(notNullValue()));

            append(flow, endpoint, "record 2");

            assertThat(live.await(30, TimeUnit.SECONDS), is(true));
            assertThat(received.get(2).getSeqNum(), is(2L));

            trigger.stop();

            assertThat(completed.await(10, TimeUnit.SECONDS), is(true));
            assertThat(errors, empty());
        }
    }

    @Test
    void killShouldBlockUntilTerminated() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());
            var runContext = runContextFactory.of(flow, Map.of());

            append(flow, endpoint, "record 0");

            RealtimeTrigger trigger = trigger(endpoint, "orders", 0L);

            var errors = new CopyOnWriteArrayList<Throwable>();
            var firstRecord = new CountDownLatch(1);
            var completed = new CountDownLatch(1);

            Flux.from(trigger.publisher(runContext, trigger.createSseRequest(runContext, "orders")))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    record -> firstRecord.countDown(),
                    error -> {
                        errors.add(error);
                        completed.countDown();
                    },
                    completed::countDown
                );

            assertThat(firstRecord.await(30, TimeUnit.SECONDS), is(true));

            trigger.kill();

            assertThat(completed.getCount(), is(0L));
            assertThat(errors, empty());
        }
    }

    @Test
    void shouldStartExecutionForRecord() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());

            append(flow, endpoint, "record 0");

            RealtimeTrigger trigger = trigger(endpoint, "orders", 0L);
            TriggerContext triggerContext = triggerContext(flow, trigger);

            var runContext = runContextInitializer.forScheduler(
                (DefaultRunContext) runContextFactory.of(flow, trigger),
                triggerContext,
                trigger
            );

            var conditionContext = ConditionContext.builder()
                .runContext(runContext)
                .flow(flow)
                .build();

            var execution = Flux.from(trigger.evaluate(conditionContext, triggerContext))
                .subscribeOn(Schedulers.boundedElastic())
                .blockFirst(Duration.ofSeconds(30));

            trigger.stop();

            assertThat(execution, is(notNullValue()));
            assertThat(execution.getFlowId(), is(flow.getId()));
            var variables = execution.getTrigger().getVariables();
            assertThat(((Number) variables.get("seqNum")).longValue(), is(0L));
            assertThat(variables.get("body"), is(notNullValue()));
        }
    }

    @Test
    void shouldFailWhenStreamMissing() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());
            var runContext = runContextFactory.of(flow, Map.of());

            RealtimeTrigger trigger = trigger(endpoint, "missing", 0L);

            var errors = new CopyOnWriteArrayList<Throwable>();
            var completed = new CountDownLatch(1);

            Flux.from(trigger.publisher(runContext, trigger.createSseRequest(runContext, "missing")))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    record -> {},
                    error -> {
                        errors.add(error);
                        completed.countDown();
                    },
                    completed::countDown
                );

            assertThat(completed.await(30, TimeUnit.SECONDS), is(true));
            assertThat(errors, hasSize(1));
            assertThat(errors.get(0).getMessage(), org.hamcrest.Matchers.containsString("404"));
            assertThat(errors.get(0).getMessage(), containsString("Stream or basin not found"));

            trigger.stop();
        }
    }

    @Test
    void shouldRequireStream() throws Exception {
        var flow = flow("test-flow-validation");

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .build();

        var runContext = runContextFactory.of(flow, Map.of());
        TriggerContext triggerContext = triggerContext(flow, trigger);

        var conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(exception.getMessage(), is("'stream' is required"));
    }

    @Test
    void shouldRejectMultipleStartSelectors() throws Exception {
        var flow = flow("test-flow-selectors");

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .accessToken(Property.ofValue("test-token"))
            .basin(Property.ofValue("test-basin"))
            .basinEndpoint(Property.ofValue("http://localhost:1234/v1"))
            .stream(Property.ofValue("orders"))
            .seqNum(Property.ofValue(1L))
            .timestamp(Property.ofValue(2L))
            .build();

        var runContext = runContextFactory.of(flow, Map.of());
        TriggerContext triggerContext = triggerContext(flow, trigger);

        var conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );

        assertThat(
            exception.getMessage(),
            is("Only one of 'seqNum', 'timestamp' or 'tailOffset' can be set")
        );
    }

    @Test
    void shouldStartAtTailWithoutStartPosition() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());
            var runContext = runContextFactory.of(flow, Map.of());

            append(flow, endpoint, "before subscribe");

            RealtimeTrigger trigger = trigger(endpoint, "orders", null);

            var received = new CopyOnWriteArrayList<Read.Record>();
            var errors = new CopyOnWriteArrayList<Throwable>();
            var firstRecord = new CountDownLatch(1);

            Flux.from(trigger.publisher(runContext, trigger.createSseRequest(runContext, "orders")))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    record -> {
                        received.add(record);
                        firstRecord.countDown();
                    },
                    errors::add
                );

            Thread.sleep(3000);
            append(flow, endpoint, "after subscribe");

            assertThat(firstRecord.await(30, TimeUnit.SECONDS), is(true));
            assertThat(errors, empty());
            assertThat(received.get(0).getSeqNum(), is(1L));

            trigger.stop();
        }
    }

    private RealtimeTrigger plainTrigger() {
        return RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .build();
    }

    private HttpSseEvent<String> event(String name, String data) {
        return HttpSseEvent.<String>builder().name(name).data(data).build();
    }

    @Test
    void shouldEmitRecordsFromBatchEventAndIgnorePing() {
        RealtimeTrigger trigger = plainTrigger();

        List<Read.Record> records = Flux.<Read.Record>create(sink -> {
                trigger.handleEvent(
                    event("batch", "{\"records\":[{\"seq_num\":4,\"timestamp\":1,\"body\":\"a\"},{\"seq_num\":5,\"timestamp\":2,\"body\":\"b\"}],\"tail\":{\"seq_num\":6,\"timestamp\":2}}"),
                    sink
                );
                trigger.handleEvent(event("ping", "{\"timestamp\":3}"), sink);
                sink.complete();
            })
            .collectList()
            .block(Duration.ofSeconds(5));

        assertThat(records, hasSize(2));
        assertThat(records.get(0).getSeqNum(), is(4L));
        assertThat(records.get(1).getBody(), is("b"));
    }

    @Test
    void shouldCompleteOnDoneEvent() {
        RealtimeTrigger trigger = plainTrigger();

        List<Read.Record> records = Flux.<Read.Record>create(sink -> trigger.handleEvent(event(null, "[DONE]"), sink))
            .collectList()
            .block(Duration.ofSeconds(5));

        assertThat(records, empty());
    }

    @Test
    void shouldFailOnErrorEvent() {
        RealtimeTrigger trigger = plainTrigger();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> Flux.<Read.Record>create(sink -> trigger.handleEvent(event("error", "boom"), sink))
                .collectList()
                .block(Duration.ofSeconds(5))
        );

        assertThat(exception.getMessage(), containsString("boom"));
    }

    @Test
    void shouldFailOnMalformedBatch() {
        RealtimeTrigger trigger = plainTrigger();

        assertThrows(
            Exception.class,
            () -> Flux.<Read.Record>create(sink -> trigger.handleEvent(event("batch", "not json"), sink))
                .collectList()
                .block(Duration.ofSeconds(5))
        );
    }

    @Test
    void shouldIgnoreEventsOnceStopped() {
        RealtimeTrigger trigger = plainTrigger();
        trigger.stop();

        List<Read.Record> records = Flux.<Read.Record>create(sink -> {
                trigger.handleEvent(
                    event("batch", "{\"records\":[{\"seq_num\":0,\"timestamp\":1,\"body\":\"a\"}]}"),
                    sink
                );
                sink.complete();
            })
            .collectList()
            .block(Duration.ofSeconds(5));

        assertThat(records, empty());
    }

    private ConditionContext schedulerConditionContext(Flow flow, RealtimeTrigger trigger, TriggerContext triggerContext) {
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

    @Test
    void shouldResumeAfterLastDeliveredRecord() throws Exception {
        try (GenericContainer<?> s2 = container(createInitFile())) {
            s2.start();

            var endpoint = endpoint(s2);
            var flow = flow("test-flow-" + System.nanoTime());

            append(flow, endpoint, "record 0", "record 1", "record 2");

            RealtimeTrigger first = trigger(endpoint, "orders", 0L);
            TriggerContext firstContext = triggerContext(flow, first);

            List<Execution> firstRun = Flux.from(first.evaluate(schedulerConditionContext(flow, first, firstContext), firstContext))
                .subscribeOn(Schedulers.boundedElastic())
                .take(3)
                .collectList()
                .block(Duration.ofSeconds(30));

            first.stop();

            assertThat(firstRun, hasSize(3));

            var checkContext = runContextFactory.of(flow, Map.of());
            String key = first.buildWatermarkKey(checkContext, "orders");

            long deadline = System.currentTimeMillis() + 10_000;
            while (first.readLastSeen(checkContext, key) != 2L && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }

            assertThat(first.readLastSeen(checkContext, key), is(2L));

            append(flow, endpoint, "record 3");

            RealtimeTrigger second = trigger(endpoint, "orders", 0L);
            TriggerContext secondContext = triggerContext(flow, second);

            Execution resumed = Flux.from(second.evaluate(schedulerConditionContext(flow, second, secondContext), secondContext))
                .subscribeOn(Schedulers.boundedElastic())
                .blockFirst(Duration.ofSeconds(30));

            second.stop();

            assertThat(resumed, is(notNullValue()));
            assertThat(((Number) resumed.getTrigger().getVariables().get("seqNum")).longValue(), is(3L));
        }
    }

    @Test
    void shouldReturnLastSeqNumOfDeliveredBatch() {
        RealtimeTrigger trigger = plainTrigger();
        AtomicReference<Long> batchResult = new AtomicReference<>();
        AtomicReference<Long> pingResult = new AtomicReference<>(-1L);

        Flux.<Read.Record>create(sink -> {
                batchResult.set(trigger.handleEvent(
                    event("batch", "{\"records\":[{\"seq_num\":7,\"timestamp\":1,\"body\":\"a\"},{\"seq_num\":8,\"timestamp\":2,\"body\":\"b\"}]}"),
                    sink
                ));
                pingResult.set(trigger.handleEvent(event("ping", "{\"timestamp\":3}"), sink));
                sink.complete();
            })
            .collectList()
            .block(Duration.ofSeconds(5));

        assertThat(batchResult.get(), is(8L));
        assertThat(pingResult.get(), is(nullValue()));
    }
}