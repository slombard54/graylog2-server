package org.graylog2.shared.internalevents;

import org.graylog2.plugin.alarms.AlertCondition;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class StreamAlertTriggeredEvent {
    private final AlertCondition.CheckResult checkResult;

    public StreamAlertTriggeredEvent(AlertCondition.CheckResult checkResult) {
        this.checkResult = checkResult;
    }

    public AlertCondition.CheckResult getCheckResult() {
        return checkResult;
    }
}
