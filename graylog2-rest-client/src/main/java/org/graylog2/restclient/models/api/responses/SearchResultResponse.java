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
import org.graylog2.rest.models.system.indexer.responses.IndexRangeSummary;
import org.joda.time.DateTime;

import java.util.List;

public class SearchResultResponse {
    public int time;
    public String query;
    public long total_results;
    public List<MessageSummaryResponse> messages;
    public List<String> fields;

    @JsonProperty("used_indices")
    public List<IndexRangeSummary> usedIndices;

    @JsonProperty("built_query")
    public String builtQuery;

    public String from;
    public String to;

    public DateTime getFromDataTime() {
        return from != null ? DateTime.parse(from) : null;
    }

    public DateTime getToDataTime() {
        return to != null ? DateTime.parse(to) : null;
    }
}
