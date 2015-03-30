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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import javax.inject.Inject;
import com.mongodb.BasicDBObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.graylog2.Configuration;
import org.graylog2.database.CollectionName;
import org.graylog2.database.MongoConnection;
import org.graylog2.indexer.*;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.database.Persisted;
import org.graylog2.plugin.periodical.Periodical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class DeadLetterThread extends Periodical {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterThread.class);

    private final PersistedDeadLetterService persistedDeadLetterService;
    private final IndexFailureService indexFailureService;
    private final Messages messages;
    private final Configuration configuration;
    private final MongoConnection mongoConnection;
    private final MetricRegistry metricRegistry;

    @Inject
    public DeadLetterThread(PersistedDeadLetterService persistedDeadLetterService,
                            IndexFailureService indexFailureService,
                            Messages messages,
                            Configuration configuration,
                            MongoConnection mongoConnection,
                            MetricRegistry metricRegistry) {
        this.persistedDeadLetterService = persistedDeadLetterService;
        this.indexFailureService = indexFailureService;
        this.messages = messages;
        this.configuration = configuration;
        this.mongoConnection = mongoConnection;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void doRun() {
        verifyIndices();

        // Poll queue forever.
        while(true) {
            List<DeadLetter> items;
            try {
                items = messages.getDeadLetterQueue().take();
            } catch (InterruptedException ignored) { continue; /* daemon thread */ }

            for (DeadLetter item : items) {
                boolean written = false;

                // Try to write the failed message to MongoDB if enabled.
                if (configuration.isDeadLettersEnabled()) {
                    try {
                        Message message = item.getMessage();
                        PersistedDeadLetter persistedDeadLetter = persistedDeadLetterService.create(item.getId(), Tools.iso8601(), message.toElasticSearchObject());
                        persistedDeadLetterService.saveWithoutValidation(persistedDeadLetter);
                        written = true;
                    } catch(Exception e) {
                        LOG.error("Could not write message to dead letter queue.", e);
                    }
                }

                // Write failure to index_failures.
                try {
                    BulkItemResponse.Failure f = item.getFailure().getFailure();

                    Map<String, Object> doc = Maps.newHashMap();
                    doc.put("letter_id", item.getId());
                    doc.put("index", f.getIndex());
                    doc.put("type", f.getType());
                    doc.put("message", f.getMessage());
                    doc.put("timestamp", item.getTimestamp());
                    doc.put("written", written);

                    IndexFailure indexFailure = new IndexFailureImpl(doc);
                    indexFailureService.saveWithoutValidation(indexFailure);
                } catch(Exception e) {
                    LOG.error("Could not persist index failure.", e);
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    // TODO: Move this to the related persisted service classes
    private void verifyIndices() {
        mongoConnection.getDatabase().getCollection(getCollectionName(IndexFailureImpl.class)).createIndex(new BasicDBObject("timestamp", 1));
        mongoConnection.getDatabase().getCollection(getCollectionName(IndexFailureImpl.class)).createIndex(new BasicDBObject("letter_id", 1));

        mongoConnection.getDatabase().getCollection(getCollectionName(PersistedDeadLetterImpl.class)).createIndex(new BasicDBObject("timestamp", 1));
        mongoConnection.getDatabase().getCollection(getCollectionName(PersistedDeadLetterImpl.class)).createIndex(new BasicDBObject("letter_id", 1));
    }

    private String getCollectionName(Class<? extends Persisted> modelClass) {
        return modelClass.getAnnotation(CollectionName.class).value();
    }

    @Override
    public boolean runsForever() {
        return true;
    }

    @Override
    public boolean stopOnGracefulShutdown() {
        return false;
    }

    @Override
    public boolean masterOnly() {
        return false;
    }

    @Override
    public boolean startOnThisNode() {
        return true;
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public int getInitialDelaySeconds() {
        return 0;
    }

    @Override
    public int getPeriodSeconds() {
        return 0;
    }

    @Override
    public void initialize() {
        metricRegistry.register(MetricRegistry.name(DeadLetterThread.class, "queueSize"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return messages.getDeadLetterQueue().size();
            }
        });
    }

}
