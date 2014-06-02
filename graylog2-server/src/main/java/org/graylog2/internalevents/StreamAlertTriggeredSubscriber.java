package org.graylog2.internalevents;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.graylog2.alarmcallbacks.AlarmCallbackConfiguration;
import org.graylog2.alarmcallbacks.AlarmCallbackConfigurationService;
import org.graylog2.alarmcallbacks.AlarmCallbackFactory;
import org.graylog2.alarmcallbacks.EmailAlarmCallback;
import org.graylog2.plugin.alarms.callbacks.AlarmCallback;
import org.graylog2.plugin.alarms.callbacks.AlarmCallbackConfigurationException;
import org.graylog2.plugin.alarms.callbacks.AlarmCallbackException;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.shared.internalevents.StreamAlertTriggeredEvent;
import org.graylog2.shared.internalevents.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
@Subscriber
public class StreamAlertTriggeredSubscriber {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final AlarmCallbackConfigurationService alarmCallbackConfigurationService;
    private final AlarmCallbackFactory alarmCallbackFactory;
    private final EmailAlarmCallback emailAlarmCallback;
    private final EventBus eventBus;

    @Inject
    public StreamAlertTriggeredSubscriber(AlarmCallbackConfigurationService alarmCallbackConfigurationService,
                                          AlarmCallbackFactory alarmCallbackFactory,
                                          EmailAlarmCallback emailAlarmCallback,
                                          EventBus eventBus) {
        this.alarmCallbackConfigurationService = alarmCallbackConfigurationService;
        this.alarmCallbackFactory = alarmCallbackFactory;
        this.emailAlarmCallback = emailAlarmCallback;
        this.eventBus = eventBus;
    }

    @Subscribe public void streamAlertTriggered(StreamAlertTriggeredEvent event) {
        Stream stream = event.getCheckResult().getTriggeredCondition().getStream();
        try {
            List<AlarmCallbackConfiguration> callConfigurations = alarmCallbackConfigurationService.getForStream(stream);
            if (callConfigurations.size() > 0)
                for (AlarmCallbackConfiguration configuration : callConfigurations) {
                    AlarmCallback alarmCallback = alarmCallbackFactory.create(configuration);
                    alarmCallback.call(stream, event.getCheckResult());
                }
            else
                emailAlarmCallback.call(stream, event.getCheckResult());

        } catch (AlarmCallbackException | ClassNotFoundException | AlarmCallbackConfigurationException e) {
            LOG.error("Exception while handling triggered stream alert: {}", e);
        }
    }
}
