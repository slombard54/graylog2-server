package org.graylog2.shared.internalevents;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.*;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
@Singleton
public class InternalEventHandlerImpl implements InternalEventHandler {
    private final AsyncEventBus eventBus;

    @Inject
    public InternalEventHandlerImpl() {
        BlockingQueue<Runnable> waitQueue = new ArrayBlockingQueue<Runnable>(1000);
        Executor executor = new ThreadPoolExecutor(20, 100, 60, TimeUnit.SECONDS, waitQueue);
        this.eventBus = new AsyncEventBus(executor);
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
