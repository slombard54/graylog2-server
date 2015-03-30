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
package org.graylog2.restclient.models.api.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.graylog2.rest.models.system.buffers.responses.SingleRingUtilization;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class BufferSummaryResponse {
    public static final BufferSummaryResponse EMPTY = new BufferSummaryResponse();

    public long utilization;

    @JsonProperty("utilization_percent")
    public float utilizationPercent;

    public static BufferSummaryResponse fromSingleRingUtilization(SingleRingUtilization sru) {
        final BufferSummaryResponse bur = new BufferSummaryResponse();
        bur.utilization = sru.utilization();
        bur.utilizationPercent = sru.utilizationPercent();

        return bur;
    }
}
