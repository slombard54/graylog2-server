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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.bson.types.ObjectId;
import org.graylog2.plugin.streams.Output;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.plugin.streams.StreamRule;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(MockitoJUnitRunner.class)
public class StreamListFingerprintTest {
    @Mock
    Stream stream1;
    @Mock
    Stream stream2;
    @Mock
    StreamRule streamRule1;
    @Mock
    StreamRule streamRule2;
    @Mock
    StreamRule streamRule3;
    @Mock
    Output output1;
    @Mock
    Output output2;

    private final String expectedFingerprint = "944fc39a2e1db9d13ef7c7323a670ebd426e37c1";
    private final String expectedEmptyFingerprint = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

    @Before
    public void setUp() throws Exception {
        output1 = makeOutput(1, "output1");
        output2 = makeOutput(2, "output2");

        streamRule1 = makeStreamRule(1, "field1");
        streamRule2 = makeStreamRule(2, "field2");
        streamRule3 = makeStreamRule(3, "field3");

        stream1 = makeStream(1, "title1", new StreamRule[]{streamRule1, streamRule2}, new Output[]{output1, output2});
        stream2 = makeStream(2, "title2", new StreamRule[]{streamRule3}, new Output[]{output2, output1});
    }

    private static Stream makeStream(int id, String title, StreamRule[] rules, Output[] outputs) {
        final HashMap<String, Object> fields = Maps.newHashMap();
        fields.put(StreamImpl.FIELD_TITLE, title);
        return new StreamImpl(new ObjectId(String.format("%024d", id)), fields, Lists.newArrayList(rules), Sets.newHashSet(
                outputs));
    }

    private static StreamRule makeStreamRule(int id, String field) {
        final HashMap<String, Object> fields = Maps.newHashMap();
        fields.put(StreamRuleImpl.FIELD_FIELD, field);
        return new StreamRuleImpl(new ObjectId(String.format("%024d", id)), fields);
    }

    private static Output makeOutput(int id, String field) {
        final HashMap<String, Object> fields = Maps.newHashMap();
        fields.put(OutputImpl.FIELD_TITLE, field);
        fields.put(OutputImpl.FIELD_TYPE, "foo");
        fields.put(OutputImpl.FIELD_CREATED_AT, DateTime.parse("2015-01-01T00:00:00Z").toDate());
        fields.put(OutputImpl.FIELD_CREATOR_USER_ID, "user1");
        return new OutputImpl(new ObjectId(String.format("%024d", id)), fields);
    }

    @Test
    public void testGetFingerprint() throws Exception {
        final StreamListFingerprint fingerprint = new StreamListFingerprint(Lists.newArrayList(stream1, stream2));

        assertEquals(fingerprint.getFingerprint(), expectedFingerprint);
    }

    @Test
    public void testIdenticalStreams() throws Exception {
        final StreamListFingerprint fingerprint1 = new StreamListFingerprint(Lists.newArrayList(stream1));
        final StreamListFingerprint fingerprint2 = new StreamListFingerprint(Lists.newArrayList(stream1));
        final StreamListFingerprint fingerprint3 = new StreamListFingerprint(Lists.newArrayList(stream2));

        assertEquals(fingerprint1.getFingerprint(), fingerprint2.getFingerprint());
        assertNotEquals(fingerprint1.getFingerprint(), fingerprint3.getFingerprint());
    }

    @Test
    public void testWithEmptyStreamList() throws Exception {
        final StreamListFingerprint fingerprint = new StreamListFingerprint(Lists.<Stream>newArrayList());

        assertEquals(fingerprint.getFingerprint(), expectedEmptyFingerprint);
    }
}
