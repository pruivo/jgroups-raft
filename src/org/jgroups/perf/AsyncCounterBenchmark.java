package org.jgroups.perf;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.raft.Options;
import org.jgroups.util.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Basic {@link org.jgroups.blocks.atomic.AsyncCounter} benchmark
 */
public class AsyncCounterBenchmark implements CounterBenchmark {

    private List<CompletionStage<Void>> requests;
    private LongSupplier deltaSupplier;
    private int concurrency;
    private AsyncCounter counter;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final LongAdder updates = new LongAdder();
    private final AtomicHistogram histogram = HistogramUtil.createAtomic();

    @Override
    public void init(int concurrency, ThreadFactory threadFactory, LongSupplier deltaSupplier, Counter counter) {
        this.concurrency = concurrency;
        this.deltaSupplier = deltaSupplier;
        this.counter = counter.async().withOptions(Options.create(true));
        requests = new ArrayList<>(concurrency);
    }

    @Override
    public void start() {
        stop.set(false);
        final long currentTime = System.nanoTime();
        for (int i = 0; i < concurrency; ++i) {
            requests.add(updateCounter(counter, currentTime));
        }
    }

    @Override
    public void stop() {
        stop.set(true);
    }

    @Override
    public void join() throws InterruptedException {
        for (CompletionStage<Void> stage : requests) {
            stage.toCompletableFuture().join();
        }
    }

    @Override
    public long getTotalUpdates() {
        return updates.sum();
    }

    @Override
    public Histogram getResults(boolean printUpdaters, Function<AbstractHistogram, String> timePrinter) {
        return histogram;
    }

    @Override
    public void close() throws Exception {
        stop.set(true);
        requests.clear();
    }

    private void updateTime(long timeNanos) {
        updates.increment();
        histogram.recordValue(timeNanos);
    }

    private CompletionStage<Void> updateCounter(AsyncCounter counter, long start) {
        if (stop.get()) {
            // we don't check the return value
            return CompletableFutures.completedNull();
        }
        return counter.addAndGet(deltaSupplier.getAsLong()).thenCompose(__ -> {
            final long currentTime = System.nanoTime();
            updateTime(currentTime - start);
            return updateCounter(counter, currentTime);
        });
    }
}
