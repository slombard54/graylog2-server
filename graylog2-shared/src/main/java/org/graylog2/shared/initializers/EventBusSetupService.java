package org.graylog2.shared.initializers;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;
import org.graylog2.shared.bindings.InstantiationService;
import org.graylog2.shared.internalevents.InternalEventHandler;
import org.graylog2.shared.internalevents.Subscriber;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class EventBusSetupService extends AbstractIdleService {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final InternalEventHandler internalEventHandler;
    private final InstantiationService instantiationService;

    @Inject
    public EventBusSetupService(InternalEventHandler internalEventHandler, InstantiationService instantiationService) {
        this.internalEventHandler = internalEventHandler;
        this.instantiationService = instantiationService;
    }

    @Override
    protected void startUp() throws Exception {
        Reflections reflections = new Reflections("org.graylog2");
        for (Class<?> subscriberClass : reflections.getTypesAnnotatedWith(Subscriber.class)) {
            LOG.debug("Registering event subscriber: {}", subscriberClass);
            internalEventHandler.register(instantiationService.getInstance(subscriberClass));
        }
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
