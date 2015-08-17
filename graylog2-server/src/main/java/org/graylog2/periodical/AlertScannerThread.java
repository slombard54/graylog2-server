/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.periodical;

import org.graylog2.Configuration;
import org.graylog2.alarmcallbacks.AlarmCallbackConfiguration;
import org.graylog2.alarmcallbacks.AlarmCallbackConfigurationService;
import org.graylog2.alarmcallbacks.AlarmCallbackFactory;
import org.graylog2.alarmcallbacks.AlarmCallbackHistory;
import org.graylog2.alarmcallbacks.AlarmCallbackHistoryService;
import org.graylog2.alarmcallbacks.EmailAlarmCallback;
import org.graylog2.alerts.Alert;
import org.graylog2.alerts.AlertService;
import org.graylog2.initializers.IndexerSetupService;
import org.graylog2.plugin.alarms.AlertCondition;
import org.graylog2.plugin.alarms.callbacks.AlarmCallback;
import org.graylog2.plugin.periodical.Periodical;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.streams.StreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

public class AlertScannerThread extends Periodical {
    private static final Logger LOG = LoggerFactory.getLogger(AlertScannerThread.class);

    private final StreamService streamService;
    private final AlarmCallbackConfigurationService alarmCallbackConfigurationService;
    private final AlarmCallbackFactory alarmCallbackFactory;
    private final EmailAlarmCallback emailAlarmCallback;
    private final IndexerSetupService indexerSetupService;
    private final AlertService alertService;
    private final Configuration configuration;
    private final AlarmCallbackHistoryService alarmCallbackHistoryService;

    @Inject
    public AlertScannerThread(final AlertService alertService,
                              final StreamService streamService,
                              final AlarmCallbackConfigurationService alarmCallbackConfigurationService,
                              final AlarmCallbackFactory alarmCallbackFactory,
                              final EmailAlarmCallback emailAlarmCallback,
                              final IndexerSetupService indexerSetupService,
                              final Configuration configuration,
                              final AlarmCallbackHistoryService alarmCallbackHistoryService) {
        this.alertService = alertService;
        this.streamService = streamService;
        this.alarmCallbackConfigurationService = alarmCallbackConfigurationService;
        this.alarmCallbackFactory = alarmCallbackFactory;
        this.emailAlarmCallback = emailAlarmCallback;
        this.indexerSetupService = indexerSetupService;
        this.configuration = configuration;
        this.alarmCallbackHistoryService = alarmCallbackHistoryService;
    }

    @Override
    public void doRun() {
        if (!indexerSetupService.isRunning()) {
            LOG.error("Indexer is not running, not checking streams for alerts.");
            return;
        }

        LOG.debug("Running alert checks.");
        final List<Stream> alertedStreams = streamService.loadAllWithConfiguredAlertConditions();

        LOG.debug("There are {} streams with configured alert conditions.", alertedStreams.size());

        // Load all streams that have configured alert conditions.
        for (Stream stream : alertedStreams) {
            LOG.debug("Stream [{}] has [{}] configured alert conditions.", stream, streamService.getAlertConditions(stream).size());

            if(stream.isPaused()) {
                LOG.debug("Stream [{}] has been paused. Skipping alert check.", stream);
                continue;
            }

            // Check if a threshold is reached.
            for (AlertCondition alertCondition : streamService.getAlertConditions(stream)) {
                try {
                    final AlertCondition.CheckResult result = alertService.triggered(alertCondition);
                    if (result.isTriggered()) {
                        // Alert is triggered!
                        LOG.debug("Alert condition [{}] is triggered. Sending alerts.", alertCondition);

                        // Persist alert.
                        final Alert alert = alertService.factory(result);
                        alertService.save(alert);

                        final List<AlarmCallbackConfiguration> callConfigurations = alarmCallbackConfigurationService.getForStream(stream);

                        // Checking if alarm callbacks have been defined
                        if (callConfigurations.size() > 0)
                            for (AlarmCallbackConfiguration configuration : callConfigurations) {
                                AlarmCallbackHistory alarmCallbackHistory;
                                AlarmCallback alarmCallback = null;
                                try {
                                    alarmCallback = alarmCallbackFactory.create(configuration);
                                    alarmCallback.call(stream, result);
                                    alarmCallbackHistory = alarmCallbackHistoryService.success(configuration, alert, alertCondition);
                                } catch (Exception e) {
                                    if (alarmCallback != null) {
                                        LOG.warn("Alarm callback <" + alarmCallback.getName() + "> failed. Skipping.", e);
                                    } else {
                                        LOG.warn("Alarm callback with id " + configuration.getId() + " failed. Skipping.", e);
                                    }
                                    alarmCallbackHistory = alarmCallbackHistoryService.error(configuration, alert, alertCondition, e.getMessage());
                                }

                                try {
                                    alarmCallbackHistoryService.save(alarmCallbackHistory);
                                } catch (Exception e) {
                                    LOG.warn("Unable to save history of alarm callback run: ", e);
                                }
                            }
                        else {
                            /* Using e-mail alarm callback per default if there are no alarm callbacks configured explicitly.
                               This way we are supporting users who have upgraded from an old version where alarm callbacks
                               were non-existent. It also helps for users who forgot to set up alarm callbacks for newly
                               created alert conditions. */
                            emailAlarmCallback.call(stream, result);
                        }
                    } else {
                        // Alert not triggered.
                        LOG.debug("Alert condition [{}] is not triggered.", alertCondition);
                    }
                } catch(Exception e) {
                    LOG.error("Skipping alert check that threw an exception.", e);
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public boolean runsForever() {
        return false;
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public boolean stopOnGracefulShutdown() {
        return true;
    }

    @Override
    public boolean masterOnly() {
        return true;
    }

    @Override
    public boolean startOnThisNode() {
        return true;
    }

    @Override
    public int getInitialDelaySeconds() {
        return 10;
    }

    @Override
    public int getPeriodSeconds() {
        return configuration.getAlertCheckInterval();
    }
}
