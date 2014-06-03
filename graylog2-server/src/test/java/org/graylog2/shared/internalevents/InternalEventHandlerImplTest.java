package org.graylog2.shared.internalevents;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import org.graylog2.shared.BaseConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class InternalEventHandlerImplTest {
    private static final int expectedValue = 23;

    private interface TestEvent {
        public int getValue();
    }

    private class TestSubscriber {
        @Subscribe
        public void handleTestEvent(TestEvent event) {
            Assert.assertEquals(event.getValue(), expectedValue);
        }

        @Subscribe
        public void handleDeadEvent(DeadEvent event) {
            throw new RuntimeException("Caught dead event: " + event);
        }
    }

    private InternalEventHandler internalEventHandler;

    @BeforeMethod
    public void setUp() throws Exception {
        BaseConfiguration configuration = mock(BaseConfiguration.class);
        when(configuration.getEventBusInitialPoolSize()).thenReturn(1);
        when(configuration.getEventBusMaxPoolSize()).thenReturn(10);
        when(configuration.getEventBusPoolKeepalive()).thenReturn(60);
        when(configuration.getEventBusMaxQueueSize()).thenReturn(100);

        MetricRegistry metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.meter(anyString())).thenReturn(mock(Meter.class));
        when(metricRegistry.timer(anyString())).thenReturn(mock(Timer.class));
        when(metricRegistry.counter(anyString())).thenReturn(mock(Counter.class));

        this.internalEventHandler = new InternalEventHandlerImpl(configuration, metricRegistry);
        this.internalEventHandler.register(new TestSubscriber());
    }

    @Test
    public void testSubmit() throws Exception {
        TestEvent testEvent = mock(TestEvent.class);
        when(testEvent.getValue()).thenReturn(expectedValue);

        this.internalEventHandler.submit(testEvent);
        verify(testEvent).getValue();
    }
}
