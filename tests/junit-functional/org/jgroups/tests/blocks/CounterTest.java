package org.jgroups.tests.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.Options;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.AssertJUnit.*;

/**
 * {@link  AsyncCounter} and {@link  SyncCounter} test.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CounterTest {

    private static final String CLUSTER = "_counter_test_";

    protected JChannel a, b, c;
    protected CounterService service_a, service_b, service_c;

    @AfterClass(alwaysRun = true)
    public void afterMethod() {
        for (JChannel ch : Arrays.asList(c, b, a)) {
            RAFT raft = ch.getProtocolStack().findProtocol(RAFT.class);
            try {
                Utils.deleteLog(raft);
            } catch (Exception ignored) {
            }
            Util.close(ch);
        }
    }

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        List<String> members = Arrays.asList("a", "b", "c");

        a = createChannel(0, members).connect(CLUSTER);
        b = createChannel(1, members).connect(CLUSTER);
        c = createChannel(2, members).connect(CLUSTER);

        Util.waitUntilAllChannelsHaveSameView(1000, 500, a, b, c);

        service_a = new CounterService(a).allowDirtyReads(false);
        service_b = new CounterService(b).allowDirtyReads(false);
        service_c = new CounterService(c).allowDirtyReads(false);
    }

    public void testIncrement() {
        List<SyncCounter> counters = createCounters("increment");

        assertEquals(1, counters.get(0).incrementAndGet());
        assertValues(counters, 1);

        assertEquals(6, counters.get(1).addAndGet(5));
        assertValues(counters, 6);

        assertEquals(11, counters.get(2).addAndGet(5));
        assertValues(counters, 11);
    }

    public void testDecrement() {
        List<SyncCounter> counters = createCounters("decrement");

        assertEquals(-1, counters.get(0).decrementAndGet());
        assertValues(counters, -1);

        assertEquals(-8, counters.get(1).addAndGet(-7));
        assertValues(counters, -8);

        assertEquals(-9, counters.get(2).decrementAndGet());
        assertValues(counters, -9);
    }

    public void testSet() {
        List<SyncCounter> counters = createCounters("set");

        counters.get(0).set(10);
        assertValues(counters, 10);

        counters.get(1).set(15);
        assertValues(counters, 15);

        counters.get(2).set(-10);
        assertValues(counters, -10);
    }

    public void testCompareAndSet() {
        List<SyncCounter> counters = createCounters("casb");

        assertTrue(counters.get(0).compareAndSet(0, 2));
        assertValues(counters, 2);

        assertFalse(counters.get(1).compareAndSet(0, 3));
        assertValues(counters, 2);

        assertTrue(counters.get(2).compareAndSet(2, -2));
        assertValues(counters, -2);
    }

    public void testCompareAndSwap() {
        List<SyncCounter> counters = createCounters("casl");

        assertEquals(0, counters.get(0).compareAndSwap(0, 2));
        assertValues(counters, 2);

        assertEquals(2, counters.get(1).compareAndSwap(0, 3));
        assertValues(counters, 2);

        assertEquals(2, counters.get(2).compareAndSwap(2, -2));
        assertValues(counters, -2);
    }

    public void testIgnoreReturnValue() {
        List<SyncCounter> counters=createCounters("ignore");
        SyncCounter counter=counters.get(1);
        long ret=counter.incrementAndGet();
        assert ret == 1;

        counter=counter.withOptions(Options.create(true));
        ret=counter.incrementAndGet();
        assert ret == 0;

        ret=counter.addAndGet(10);
        assert ret == 0;
        ret=counter.get();
        assert ret == 12;

        ret=counter.getLocal();
        assert ret == 12;

        boolean rc=counter.compareAndSet(12, 15);
        assert !rc;

        ret=counter.get();
        assert ret == 15;

        counter.set(20);
        ret=counter.get();
        assert ret == 20;

        AsyncCounter ctr=counter.async();
        CompletionStage<Long> f=ctr.addAndGet(5);
        Long val=CompletableFutures.join(f);
        assert val == null;
        f=ctr.get();
        ret=CompletableFutures.join(f);
        assert ret == 25;
        f=ctr.getLocal();
        ret=CompletableFutures.join(f);
        assert ret == 25;
        f=ctr.incrementAndGet();
        val=CompletableFutures.join(f);
        assert val == null;
        ctr.set(30);
        assert CompletableFutures.join(ctr.get()) == 30;

        counter=counter.withOptions(Options.create(false));
        ret=counter.decrementAndGet();
        assert ret == 29;
    }

    public void testChainAddAndGet() {
        List<AsyncCounter> counters = createAsyncCounters("chain-add-and-get");

        CompletionStage<Long> stage = counters.get(0).addAndGet(5)
                .thenCompose(value -> counters.get(0).addAndGet(value));
        stage.thenAccept(value -> assertEquals(10L, (long) value)).toCompletableFuture().join();

        List<CompletionStage<Boolean>> checkValueStage = new ArrayList<>(counters.size());
        Function<Long, Boolean> isTen = value -> value == 10;
        for (AsyncCounter c : counters) {
            checkValueStage.add(c.get().thenApply(isTen));
        }
        for (CompletionStage<Boolean> c : checkValueStage) {
            assertTrue(c.toCompletableFuture().join());
        }
        assertAsyncValues(counters, 10);
    }

    public void testCombineCounters() {
        List<AsyncCounter> counters1 = createAsyncCounters("combine-1");
        List<AsyncCounter> counters2 = createAsyncCounters("combine-2");

        long delta1 = Util.random(100);
        long delta2 = Util.random(100);

        CompletionStage<Long> stage1 = counters1.get(0).addAndGet(delta1);
        CompletionStage<Long> stage2 = counters2.get(1).addAndGet(delta2);

        stage1.thenCombine(stage2, Math::max)
                .thenAccept(value -> assertEquals(Math.max(delta1, delta2), (long) value))
                .toCompletableFuture().join();

        assertAsyncValues(counters1, delta1);
        assertAsyncValues(counters2, delta2);
    }

    public void testCompareAndSwapChained() {
        List<AsyncCounter> counters = createAsyncCounters("cas-chained");

        final long initialValue = 100;
        final long finalValue = 10;

        counters.get(0).set(initialValue).toCompletableFuture().join();

        AtomicLong rv = new AtomicLong();
        boolean result = counters.get(0).compareAndSwap(1, finalValue)
                .thenCompose(rValue -> {
                    rv.set(rValue);
                    return counters.get(0).compareAndSwap(rValue, finalValue);
                })
                .thenApply(value -> value == initialValue)
                .toCompletableFuture()
                .join();

        assertTrue(result);
        assertEquals(initialValue, rv.longValue());
        assertAsyncValues(counters, finalValue);
    }

    public void testCompareAndSetChained() {
        List<AsyncCounter> counters = createAsyncCounters("casb-chained");
        final AsyncCounter counter = counters.get(0);

        final long initialValue = 100;
        final long finalValue = 10;

        counter.set(initialValue).toCompletableFuture().join();

        boolean result = counter.compareAndSet(1, finalValue)
                .thenCompose(success -> {
                    if (success) {
                        // should not reach here
                        return CompletableFutures.completedFalse();
                    }
                    return counter.get().thenCompose(value -> counter.compareAndSet(value, finalValue));
                })
                .toCompletableFuture()
                .join();

        assertTrue(result);
        assertAsyncValues(counters, finalValue);
    }

    public void testAsyncIncrementPerf() {
        List<AsyncCounter> counters = createAsyncCounters("async-perf-1");
        final AsyncCounter counter = counters.get(0);
        final long maxValue = 10_000;

        CompletableFuture<Long> stage = CompletableFutures.completedNull();
        Function<Long, CompletionStage<Long>> increment = __ -> counter.incrementAndGet();

        long start = System.currentTimeMillis();

        for (int i = 0; i < maxValue; ++i) {
            stage = stage.thenCompose(increment);
        }

        long loopEnd = System.currentTimeMillis() - start;

        stageEquals(maxValue, stage);

        long waitEnd = System.currentTimeMillis() - start;

        System.out.printf("async perf: val=%d, loop time=%d ms, total time=%d\n", counter.get().toCompletableFuture().join(), loopEnd, waitEnd);
        assertAsyncValues(counters, maxValue);
    }

    public void testSyncIncrementPerf() {
        List<SyncCounter> counters = createCounters("sync-perf-1");
        final SyncCounter counter = counters.get(0);
        final long maxValue = 10_000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < maxValue; ++i) {
            counter.incrementAndGet();
        }
        long time = System.currentTimeMillis() - start;

        System.out.printf("sync perf: val=%d, time=%d ms\n", counter.get(), time);
        assertValues(counters, maxValue);
    }

    public void testConcurrentCas() {
        List<AsyncCounter> counters = createAsyncCounters("ccas");
        final long maxValue = 10_000;

        List<CompletionStage<Long>> results = new ArrayList<>(counters.size());
        List<AtomicInteger> successes = new ArrayList<>(counters.size());

        long start = System.currentTimeMillis();

        for (AsyncCounter c : counters) {
            AtomicInteger s = new AtomicInteger();
            successes.add(s);
            results.add(compareAndSwap(c, 0, 1, maxValue, s));
        }

        long loopEnd = System.currentTimeMillis() - start;

        for (CompletionStage<Long> c : results) {
            stageEquals(maxValue, c);
        }

        long waitEnd = System.currentTimeMillis() - start;

        System.out.printf("cas async perf: val=%d, loop time=%d ms, total time=%d\n", counters.get(0).get().toCompletableFuture().join(), loopEnd, waitEnd);
        assertAsyncValues(counters, maxValue);

        long casCount = 0;
        for (int i = 0; i < successes.size(); ++i) {
            System.out.printf("cas results for node %d: %d CAS succeed%n", i, successes.get(i).intValue());
            casCount += successes.get(i).longValue();
        }
        assertEquals(maxValue, casCount);
    }

    public void testDelete() throws Exception {
        List<AsyncCounter> counters = createAsyncCounters("to-delete");
        for (AsyncCounter counter : counters) {
            assertEquals(0, counter.sync().get());
        }

        for (CounterService service : Arrays.asList(service_a, service_b, service_c)) {
            assertTrue(service.printCounters().contains("to-delete"));
        }

        // blocks until majority
        service_a.deleteCounter("to-delete");

        // wait at most for 10 seconds
        // delete may take a while to replicate
        boolean counterRemoved = false;
        String raftMemberWithCounter = null;
        for (int i = 0; i< 10 && !counterRemoved; ++i) {
            counterRemoved = true;
            raftMemberWithCounter = null;
            for (CounterService service : Arrays.asList(service_a, service_b, service_c)) {
                if (service.printCounters().contains("to-delete")) {
                    counterRemoved = false;
                    raftMemberWithCounter = service.raftId();
                }
            }
            if (!counterRemoved) {
                Thread.sleep(1000);
            }
        }
        assertTrue("Counter exists in " + raftMemberWithCounter, counterRemoved);
    }

    private static CompletionStage<Long> compareAndSwap(AsyncCounter counter, long expected, long update, long maxValue, AtomicInteger successes) {
        return counter.compareAndSwap(expected, update)
                .thenCompose(value -> {
                    // cas is successful if return value is equals to expected
                    if (value == expected) {
                        successes.incrementAndGet();
                    }
                    return value < maxValue ?
                            compareAndSwap(counter, value, value + 1, maxValue, successes) :
                            CompletableFuture.completedFuture(value);
                });
    }

    private static void stageEquals(long value, CompletionStage<Long> stage) {
        assertEquals(value, (long) stage.toCompletableFuture().join());
    }

    private static void assertValues(List<SyncCounter> counters, long expectedValue) {
        for (SyncCounter counter : counters) {
            assertEquals(expectedValue, counter.get());
        }
    }

    private static void assertAsyncValues(List<AsyncCounter> counters, long expectedValue) {
        for (AsyncCounter counter : counters) {
            stageEquals(expectedValue, counter.get());
        }
    }

    private List<SyncCounter> createCounters(String name) {
        return Stream.of(service_a, service_b, service_c)
                .map(counterService -> createCounter(name, counterService))
                .map(AsyncCounter::sync)
                .collect(Collectors.toList());
    }

    private List<AsyncCounter> createAsyncCounters(String name) {
        return Stream.of(service_a, service_b, service_c)
                .map(counterService -> createCounter(name, counterService))
                .collect(Collectors.toList());
    }

    private static AsyncCounter createCounter(String name, CounterService counterService) {
        return CompletableFutures.join(counterService.getOrCreateAsyncCounter(name, 0));
    }

    private static JChannel createChannel(int id, final List<String> members) throws Exception {
        String name = members.get(id);
        ELECTION election = new ELECTION();
        RAFT raft = new RAFT().members(members).raftId(members.get(id)).logClass(FileBasedLog.class.getCanonicalName()).logPrefix(name + "-" + CLUSTER);
        //noinspection resource
        return new JChannel(Util.getTestStack(election, raft, new REDIRECT())).name(name);
    }


}
