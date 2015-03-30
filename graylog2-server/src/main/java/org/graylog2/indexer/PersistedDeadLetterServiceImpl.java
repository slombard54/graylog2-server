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
package org.graylog2.indexer;

import javax.inject.Inject;

import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.graylog2.database.MongoConnection;
import org.graylog2.database.PersistedServiceImpl;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class PersistedDeadLetterServiceImpl extends PersistedServiceImpl implements PersistedDeadLetterService {
    @Inject
    public PersistedDeadLetterServiceImpl(MongoConnection mongoConnection) {
        super(mongoConnection);
    }

    @Override
    public PersistedDeadLetter create(String letterId, DateTime timestamp, Map<String, Object> message) {
        return create(new ObjectId().toHexString(), letterId, timestamp, message);
    }

    @Override
    public PersistedDeadLetter create(String id, String letterId, DateTime timestamp, Map<String, Object> message) {
        Map<String, Object> doc = Maps.newHashMap();
        doc.put(PersistedDeadLetterImpl.LETTERID, letterId);
        doc.put(PersistedDeadLetterImpl.TIMESTAMP, timestamp);
        doc.put(PersistedDeadLetterImpl.MESSAGE, message);

        return new PersistedDeadLetterImpl(new ObjectId(id), doc);
    }

    @Override
    public long count() {
        return count(PersistedDeadLetterImpl.class, new BasicDBObject());
    }
}
