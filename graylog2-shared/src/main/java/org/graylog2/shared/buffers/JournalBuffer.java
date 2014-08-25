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
package org.graylog2.shared.buffers;

import com.google.inject.Inject;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.shared.buffers.processors.JournalProcessor;
import org.graylog2.shared.journal.KafkaJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class JournalBuffer extends Buffer {

    private static final Logger log = LoggerFactory.getLogger(JournalBuffer.class);

    private final RingBuffer<RawMessageHolder> ringBuffer;

    private static final EventTranslatorOneArg<RawMessageHolder, RawMessage> translator = new EventTranslatorOneArg<RawMessageHolder, RawMessage>() {
        @Override
        public void translateTo(RawMessageHolder event, long sequence, RawMessage arg0) {
            event.rawMessage = arg0;
        }
    };

    @Inject
    public JournalBuffer(KafkaJournal kafkaJournal) {
        final Disruptor disruptor = new Disruptor(new RawMessageHolder.RawMessageHolderFactory(),
                                                  1024,
                                                  Executors.newFixedThreadPool(4));

        disruptor.handleEventsWith(new JournalProcessor(kafkaJournal));
        ringBuffer = disruptor.start();
    }

    @Override
    public void insertFailFast(Message message,
                               MessageInput sourceInput) throws BufferOutOfCapacityException, ProcessingDisabledException {
        throw new IllegalStateException("NO");
    }

    @Override
    public void insertCached(Message message, MessageInput sourceInput) {
        throw new IllegalStateException("NO");
    }

    @Override
    public void insertRaw(final RawMessage rawMessage) {
        log.info("raw message {}", rawMessage);

        getRingBuffer().publishEvent(translator, rawMessage);
    }

    public RingBuffer<RawMessageHolder> getRingBuffer() {
        return ringBuffer;
    }

    public static class RawMessageHolder {
        public RawMessage rawMessage;

        private static class RawMessageHolderFactory implements EventFactory {
            @Override
            public RawMessageHolder newInstance() {
                return new RawMessageHolder();
            }
        }
    }

}
