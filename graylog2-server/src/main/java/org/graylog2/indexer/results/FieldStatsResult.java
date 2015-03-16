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
package org.graylog2.indexer.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

import java.util.List;

public class FieldStatsResult extends IndexQueryResult {

    private final long count;
    private final double sum;
    private final double sumOfSquares;
    private final double mean;
    private final double min;
    private final double max;
    private final double variance;
    private final double stdDeviation;
    private List<ResultMessage> searchHits;

    public FieldStatsResult(ExtendedStats f, String originalQuery, BytesReference builtQuery, TimeValue took) {
        super(originalQuery, builtQuery, took);

        this.count = f.getCount();
        this.sum = f.getSum();
        this.sumOfSquares = f.getSumOfSquares();
        this.mean = f.getAvg();
        this.min = f.getMin();
        this.max = f.getMax();
        this.variance = f.getVariance();
        this.stdDeviation = f.getStdDeviation();
    }

    public FieldStatsResult(ExtendedStats facet, SearchHits searchHits, String query, BytesReference source, TimeValue took) {
        this(facet, query, source, took);
        this.searchHits = buildResults(searchHits);
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getSumOfSquares() {
        return sumOfSquares;
    }

    public double getMean() {
        return mean;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getVariance() {
        return variance;
    }

    public double getStdDeviation() {
        return stdDeviation;
    }

    public List<ResultMessage> getSearchHits() {
        return searchHits;
    }
}
