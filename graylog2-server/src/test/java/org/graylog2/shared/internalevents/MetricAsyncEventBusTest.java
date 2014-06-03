package org.graylog2.shared.internalevents;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.Subscribe;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.codahale.metrics.MetricRegistry.name;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class MetricAsyncEventBusTest {
    private MetricAsyncEventBus eventBus;
    private MetricRegistry metricRegistry;
    private Counter registeredHandlers;
    private Meter postedEvents;
    private Counter queuedEvents;

    private interface TestEvent {}

    private interface TestSubscriber {
        @Subscribe
        public void handleTestEvent(TestEvent event);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        Executor executor = Executors.newSingleThreadExecutor();

        this.metricRegistry = mock(MetricRegistry.class);
        this.registeredHandlers = mock(Counter.class);
        this.postedEvents = mock(Meter.class);
        this.queuedEvents = mock(Counter.class);
        when(metricRegistry.counter(name(MetricAsyncEventBus.class, "registeredHandlers"))).thenReturn(registeredHandlers);
        when(metricRegistry.meter(name(MetricAsyncEventBus.class, "postedEvents"))).thenReturn(postedEvents);
        when(metricRegistry.counter(name(MetricAsyncEventBus.class, "queuedEvents"))).thenReturn(queuedEvents);

        this.eventBus = new MetricAsyncEventBus(executor, metricRegistry);
    }

    @Test
    public void testRegister() throws Exception {
        TestSubscriber testSubscriber = mock(TestSubscriber.class);

        this.eventBus.register(testSubscriber);

        verify(registeredHandlers).inc();
    }

    @Test
    public void testUnregister() throws Exception {
        TestSubscriber testSubscriber = mock(TestSubscriber.class);

        this.eventBus.register(testSubscriber);
        this.eventBus.unregister(testSubscriber);

        verify(registeredHandlers).dec();
    }

    @Test
    public void testPost() throws Exception {
        TestSubscriber testSubscriber = mock(TestSubscriber.class);
        TestEvent testEvent = mock(TestEvent.class);

        this.eventBus.register(testSubscriber);
        this.eventBus.post(testEvent);

        verify(registeredHandlers).inc();
        verify(postedEvents).mark();
        verify(queuedEvents).inc();
    }
}
