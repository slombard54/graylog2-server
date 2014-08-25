/*
 * Copyright 2014 TORCH GmbH
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
 */
package org.graylog2.shared.journal;

import kafka.message.CompressionCodec;
import kafka.message.Message;

import java.nio.ByteBuffer;

/**
 * The only reason this exists is to avoid the name clash with graylog2's Message class
 */
public final class KafkaMessage extends Message {
    public KafkaMessage(ByteBuffer buffer) {
        super(buffer);
    }

    public KafkaMessage(byte[] bytes,
                        byte[] key,
                        CompressionCodec codec,
                        int payloadOffset,
                        int payloadSize) {
        super(bytes, key, codec, payloadOffset, payloadSize);
    }

    public KafkaMessage(byte[] bytes, byte[] key, CompressionCodec codec) {
        super(bytes, key, codec);
    }

    public KafkaMessage(byte[] bytes, CompressionCodec codec) {
        super(bytes, codec);
    }

    public KafkaMessage(byte[] bytes, byte[] key) {
        super(bytes, key);
    }

    public KafkaMessage(byte[] bytes) {
        super(bytes);
    }
}
