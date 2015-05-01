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
package org.graylog2.streams;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.graylog2.database.MongoConnection;
import org.graylog2.database.NotFoundException;
import org.graylog2.database.PersistedServiceImpl;
import org.graylog2.outputs.OutputRegistry;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.database.ValidationException;
import org.graylog2.plugin.streams.Output;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.rest.models.streams.outputs.requests.CreateOutputRequest;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OutputServiceImpl extends PersistedServiceImpl implements OutputService {
    private final StreamService streamService;
    private final OutputRegistry outputRegistry;

    @Inject
    public OutputServiceImpl(MongoConnection mongoConnection,
                             StreamService streamService,
                             OutputRegistry outputRegistry) {
        super(mongoConnection);
        this.streamService = streamService;
        this.outputRegistry = outputRegistry;
    }

    @Override
    public Output load(String streamOutputId) throws NotFoundException {
        DBObject o = get(OutputImpl.class, streamOutputId);

        if (o == null) {
            throw new NotFoundException("Output <" + streamOutputId + "> not found!");
        }

        return new OutputImpl((ObjectId) o.get("_id"), o.toMap());
    }

    @Override
    public Set<Output> loadAll() {
        return loadAll(new HashMap<String, Object>());
    }

    protected Set<Output> loadAll(Map<String, Object> additionalQueryOpts) {
        Set<Output> outputs = new HashSet<>();

        DBObject query = new BasicDBObject();

        // putAll() is not working with BasicDBObject.
        for (Map.Entry<String, Object> o : additionalQueryOpts.entrySet()) {
            query.put(o.getKey(), o.getValue());
        }

        List<DBObject> results = query(OutputImpl.class, query);
        for (DBObject o : results)
            outputs.add(new OutputImpl((ObjectId) o.get("_id"), o.toMap()));

        return outputs;
    }

    @Override
    public Output create(Output request) throws ValidationException {
        OutputImpl impl = getImplOrFail(request);
        final String id = save(impl);
        impl.setId(id);
        return request;
    }

    @Override
    public Output create(CreateOutputRequest request, String userId) throws ValidationException {
        return create(new OutputImpl(request.title(), request.type(), request.configuration(),
                Tools.iso8601().toDate(), userId, request.contentPack()));
    }

    @Override
    public void destroy(Output output) throws NotFoundException {
        final OutputImpl impl = getImplOrFail(output);
        streamService.removeOutputFromAllStreams(output);
        outputRegistry.removeOutput(output);
        super.destroy(impl);
    }

    @Override
    public Output update(String id, Map<String, Object> deltas) {
        return null;
    }

    @Override
    public long count() {
        return totalCount(OutputImpl.class);
    }

    @Override
    public Map<String, Long> countByType() {
        final DBCursor outputTypes = collection(OutputImpl.class).find(null, new BasicDBObject(OutputImpl.FIELD_TYPE, 1));

        final Map<String, Long> outputsCountByType = new HashMap<>(outputTypes.count());
        for (DBObject outputType : outputTypes) {
            final String type = (String) outputType.get(OutputImpl.FIELD_TYPE);
            if (type != null) {
                final Long oldValue = outputsCountByType.get(type);
                final Long newValue = (oldValue == null) ? 1 : oldValue + 1;
                outputsCountByType.put(type, newValue);
            }
        }

        return outputsCountByType;
    }

    OutputImpl getImplOrFail(Output output) {
        if (output instanceof OutputImpl) {
            final OutputImpl impl = (OutputImpl) output;
            return impl;
        } else {
            throw new IllegalArgumentException("Passed object must be of OutputImpl class, not " + output.getClass());
        }
    }
}
