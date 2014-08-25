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
