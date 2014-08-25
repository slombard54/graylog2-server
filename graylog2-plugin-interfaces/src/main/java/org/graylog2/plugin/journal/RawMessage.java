/**
 * The MIT License
 * Copyright (c) 2012 TORCH GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.graylog2.plugin.journal;

import com.eaio.uuid.UUID;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.ByteOrder;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A raw message is the unparsed data Graylog2 was handed by an input.
 * <p>
 * Typically this is a copy of the exact bytes received over the network, after all de-chunking, removal of transport
 * headers, etc has been performed, but before any parsing, decoding, checking of the actual payload has been performed.
 * </p>
 * <p>
 * Each raw message has a unique id, a timestamp it was received at (this might be different from the timestamp contained
 * in the payload, if that has any), a tag of what payload type this is supposed to be (e.g. syslog, GELF, RadioMessage etc.),
 * as well as an opaque meta data structure.<br>
 * The format of the meta data is not stable yet, but will likely be a JSON string.
 * </p>
 */
public class RawMessage implements Serializable {

    public static int CURRENT_VERSION = 1;

    private final int version;

    private final UUID id;

    private final DateTime timestamp;

    private final String sourceInputId;

    private final String metaData;

    private final String payloadType;

    private final ChannelBuffer payload;

    public RawMessage(String payloadType,
                      String sourceInputId,
                      ChannelBuffer payload){
        this(payloadType, sourceInputId, null, payload);
    }

    public RawMessage(String payloadType,
                      String sourceInputId,
                      @Nullable String metaData,
                      ChannelBuffer payload) {
        this(new UUID(), DateTime.now(), payloadType, sourceInputId, metaData, payload);
    }

    public RawMessage(UUID id,
                      DateTime timestamp,
                      String payloadType,
                      String sourceInputId,
                      @Nullable String metaData,
                      ChannelBuffer payload) {
        checkNotNull(payload);
        checkArgument(payload.readableBytes() > 0);
        checkNotNull(payloadType);
        checkArgument(!payloadType.isEmpty());

        this.version = CURRENT_VERSION;

        this.id = id;
        this.timestamp = timestamp;
        this.sourceInputId = sourceInputId;
        this.metaData = metaData == null ? "" : metaData;
        this.payloadType = payloadType;
        this.payload = payload;
    }

    public ChannelBuffer encode() {
        final byte[] sourceInputIdBytes = sourceInputId.getBytes(UTF_8);
        final byte[] metaDataBytes = metaData.getBytes(UTF_8);
        final byte[] payloadTypeBytes = payloadType.getBytes(UTF_8);

        final ChannelBuffer buffer = ChannelBuffers.buffer(
                ByteOrder.BIG_ENDIAN,
                1 + /* version */
                    16 + /* UUID is 2 longs */
                    8 + /* timestamp is 1 long, in millis from 1970 */
                    2 + /* source input id length */
                    sourceInputIdBytes.length + /* source input id string. TODO could this be a proper UUID instead? would save many bytes*/
                    2 + /* payload type length, this should not get larger than 65535 bytes... */
                    payloadTypeBytes.length + /* utf-8 encoded name of the payload type */
                    4 + /* size of metadata, one int */
                    metaDataBytes.length + /* number of bytes of UTF-8 encoded metadata, or 0 */
                    4 + /* size of payload, one int */
                    payload.readableBytes() /* raw length of payload data */
        );

        buffer.writeByte(version);

        buffer.writeLong(id.getTime());
        buffer.writeLong(id.getClockSeqAndNode());

        buffer.writeLong(timestamp.getMillis());

        buffer.writeShort(payloadTypeBytes.length);
        buffer.writeBytes(payloadTypeBytes);

        buffer.writeShort(sourceInputIdBytes.length);
        buffer.writeBytes(sourceInputIdBytes);

        buffer.writeInt(metaDataBytes.length);
        buffer.writeBytes(metaDataBytes);

        buffer.writeInt(payload.readableBytes());
        buffer.writeBytes(payload);

        return buffer;
    }

    public static RawMessage decode(ChannelBuffer buffer) {
        try {
            final byte version = buffer.readByte();
            if (version > CURRENT_VERSION) {
                throw new IllegalArgumentException("Cannot decode raw message with version " + version +
                                                           " this decoder only supports up to version " + CURRENT_VERSION);
            }

            final long time = buffer.readLong();
            final long clockSeqAndNode = buffer.readLong();

            final long millis = buffer.readLong();

            final int payloadTypeLength = buffer.readUnsignedShort();
            final ChannelBuffer payloadTypeBuffer = buffer.readSlice(payloadTypeLength);

            final int sourceInputLength = buffer.readUnsignedShort();
            final ChannelBuffer sourceInputBuffer = buffer.readSlice(sourceInputLength);

            final int metaDataLength = buffer.readInt();
            final ChannelBuffer metaDataBuffer = buffer.readSlice(metaDataLength);

            final int payloadLength = buffer.readInt();
            final ChannelBuffer payloadBuffer = buffer.readSlice(payloadLength);

            return new RawMessage(
                    new UUID(time, clockSeqAndNode),
                    new DateTime(millis),
                    payloadTypeBuffer.toString(UTF_8),
                    sourceInputBuffer.toString(UTF_8),
                    metaDataBuffer.toString(UTF_8),
                    payloadBuffer);

        } catch (IndexOutOfBoundsException e) {
            throw new IllegalStateException("Cannot decode truncated raw message.", e);
        }
    }

    @Override
    public String toString() {
        return "RawMessage{" +
                "version=" + version +
                ", id=" + id +
                ", timestamp=" + timestamp +
                ", sourceInputId='" + sourceInputId + '\'' +
                ", metaData='" + metaData + '\'' +
                ", payloadType='" + payloadType + '\'' +
                ", payload=" + payload +
                '}';
    }

    public UUID getId() {
        return id;
    }

    public byte[] getIdBytes() {
        final long time = id.getTime();
        final long clockSeqAndNode = id.getClockSeqAndNode();
        final ChannelBuffer buffer = ChannelBuffers.buffer(16);
        buffer.writeLong(time);
        buffer.writeLong(clockSeqAndNode);
        return buffer.array();
    }
}
