package org.graylog2.shared.internalevents;

import com.codahale.metrics.*;
import com.google.common.eventbus.AsyncEventBus;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class MetricAsyncEventBus extends AsyncEventBus {
    private final Timer processingTime;
    private final Counter registeredHandlers;
    private final Meter postedEvents;
    private final Counter queuedEvents;

    public MetricAsyncEventBus(String identifier, Executor executor, MetricRegistry metricRegistry) {
        super(identifier, executor);

        this.processingTime = metricRegistry.timer(name(this.getClass(), "processingTime"));
        this.registeredHandlers = metricRegistry.counter(name(this.getClass(), "registeredHandlers"));
        this.postedEvents = metricRegistry.meter(name(this.getClass(), "postedEvents"));
        this.queuedEvents = metricRegistry.counter(name(this.getClass(), "queuedEvents"));
    }

    @Inject
    public MetricAsyncEventBus(Executor executor, MetricRegistry metricRegistry) {
        this("default", executor, metricRegistry);
    }

    @Override
    public void register(Object object) {
        super.register(object);
        this.registeredHandlers.inc();
    }

    @Override
    public void unregister(Object object) {
        super.unregister(object);
        this.registeredHandlers.dec();
    }

    @Override
    public void post(Object event) {
        super.post(event);
        this.postedEvents.mark();
        this.queuedEvents.inc();
    }

    @Override
    protected void dispatchQueuedEvents() {
        super.dispatchQueuedEvents();
        this.queuedEvents.dec();
    }
}
