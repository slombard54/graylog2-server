package org.graylog2.shared.internalevents;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.graylog2.shared.BaseConfiguration;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.*;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
@Singleton
public class InternalEventHandlerImpl implements InternalEventHandler {
    private final EventBus eventBus;

    @Inject
    public InternalEventHandlerImpl(BaseConfiguration configuration, MetricRegistry metricRegistry) {
        BlockingQueue<Runnable> waitQueue = new ArrayBlockingQueue<Runnable>(configuration.getEventBusMaxQueueSize());

        Executor executor = new ThreadPoolExecutor(configuration.getEventBusInitialPoolSize(),
                configuration.getEventBusMaxPoolSize(),
                configuration.getEventBusPoolKeepalive(), TimeUnit.SECONDS,
                waitQueue);

        this.eventBus = new MetricAsyncEventBus(executor, metricRegistry);
    }

    @Override
    public void submit(Object event) {
        eventBus.post(event);
    }

    @Override
    public void register(Object handler) {
        eventBus.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        eventBus.unregister(handler);
    }
}
