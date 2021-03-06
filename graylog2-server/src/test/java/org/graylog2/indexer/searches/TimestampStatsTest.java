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
package org.graylog2.indexer.searches;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.assertj.jodatime.api.Assertions.assertThat;

public class TimestampStatsTest {
    @Test
    public void testCreate() throws Exception {
        DateTime min = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
        DateTime max = new DateTime(2015, 1, 3, 0, 0, DateTimeZone.UTC);
        DateTime avg = new DateTime(2015, 1, 2, 0, 0, DateTimeZone.UTC);
        TimestampStats timestampStats = TimestampStats.create(min, max, avg);

        assertThat(timestampStats.min()).isEqualTo(min);
        assertThat(timestampStats.max()).isEqualTo(max);
        assertThat(timestampStats.avg()).isEqualTo(avg);
    }

    @Test
    public void testEmptyInstance() throws Exception {
        assertThat(TimestampStats.EMPTY.min()).isEqualTo(new DateTime(0L, DateTimeZone.UTC));
        assertThat(TimestampStats.EMPTY.max()).isEqualTo(new DateTime(0L, DateTimeZone.UTC));
        assertThat(TimestampStats.EMPTY.avg()).isEqualTo(new DateTime(0L, DateTimeZone.UTC));
    }
}