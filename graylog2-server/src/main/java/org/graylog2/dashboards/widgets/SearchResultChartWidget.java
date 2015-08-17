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
package org.graylog2.dashboards.widgets;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import org.graylog2.indexer.results.HistogramResult;
import org.graylog2.indexer.searches.Searches;
import org.graylog2.indexer.searches.timeranges.TimeRange;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public class SearchResultChartWidget extends ChartWidget {

    private final String query;
    private final TimeRange timeRange;

    private final Searches searches;

    public SearchResultChartWidget(MetricRegistry metricRegistry, Searches searches, String id, String description, WidgetCacheTime cacheTime, Map<String, Object> config, String query, TimeRange timeRange, String creatorUserId) {
        super(metricRegistry, Type.SEARCH_RESULT_CHART, id, description, cacheTime, config, creatorUserId);
        this.searches = searches;

        this.query = getNonEmptyQuery(query);
        this.timeRange = timeRange;
    }

    // We need to ensure query is not empty, or the histogram calculation will fail
    private String getNonEmptyQuery(String query) {
        if (isNullOrEmpty(query)) {
            return "*";
        }
        return query;
    }

    public String getQuery() {
        return query;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    @Override
    public Map<String, Object> getPersistedConfig() {
        final ImmutableMap.Builder<String, Object> persistedConfig = ImmutableMap.<String, Object>builder()
                .putAll(super.getPersistedConfig())
                .put("query", query)
                .put("timerange", timeRange.getPersistedConfig());

        return persistedConfig.build();
    }

    @Override
    protected ComputationResult compute() {
        String filter = null;
        if (!isNullOrEmpty(streamId)) {
            filter = "streams:" + streamId;
        }

        HistogramResult histogram = searches.histogram(query, interval, filter, timeRange);
        return new ComputationResult(histogram.getResults(), histogram.took().millis(), histogram.getHistogramBoundaries());
    }
}
