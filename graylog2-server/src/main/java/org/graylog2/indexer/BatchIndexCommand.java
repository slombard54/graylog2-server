/**
 * Copyright 2014 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.indexer;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class BatchIndexCommand extends HystrixCommand<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchIndexCommand.class);

    private final Client client;
    private final List<Message> messages;
    private final LinkedBlockingQueue<BulkItemResponse[]> failureQueue;

    public BatchIndexCommand(Client client, List<Message> messages, LinkedBlockingQueue<BulkItemResponse[]> failureQueue) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("IndexerGroup"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withCircuitBreakerSleepWindowInMilliseconds(5000)
                    .withExecutionIsolationThreadInterruptOnTimeout(false)));

        this.client = client;
        this.messages = messages;
        this.failureQueue = failureQueue;
    }

    @Override
    protected Boolean run() throws Exception {
        if (messages.isEmpty()) {
            return true;
        }

        final BulkRequestBuilder request = client.prepareBulk();
        for (Message msg : messages) {
            request.add(buildIndexRequest(Deflector.DEFLECTOR_NAME, msg.toElasticSearchObject(), msg.getId())); // Main index.
        }

        request.setConsistencyLevel(WriteConsistencyLevel.ONE);
        request.setReplicationType(ReplicationType.ASYNC);

        final BulkResponse response = client.bulk(request.request()).actionGet();

        LOG.debug("Bulk indexed {} messages, took {} ms, failures: {}",
                new Object[] { response.getItems().length, response.getTookInMillis(), response.hasFailures() });

        if (response.hasFailures()) {
            propagateFailure(response.getItems());
        }

        return !response.hasFailures();
    }

    @Override
    protected Boolean getFallback() {
        // TODO
        LOG.error("FALLBACK");
        return false;
    }

    private IndexRequestBuilder buildIndexRequest(String index, Map<String, Object> source, String id) {
        final IndexRequestBuilder b = new IndexRequestBuilder(client);

        b.setId(id);
        b.setSource(source);
        b.setIndex(index);
        b.setContentType(XContentType.JSON);
        b.setOpType(IndexRequest.OpType.INDEX);
        b.setType(Indexer.TYPE);
        b.setConsistencyLevel(WriteConsistencyLevel.ONE);

        return b;
    }

    private void propagateFailure(BulkItemResponse[] items) {
        LOG.error("Failed to index [{}] messages. Please check the index error log in your web interface for the reason.", items.length);

        boolean r = failureQueue.offer(items);

        if(!r) {
            LOG.debug("Could not propagate failure to failure queue. Queue is full.");
        }
    }

}
