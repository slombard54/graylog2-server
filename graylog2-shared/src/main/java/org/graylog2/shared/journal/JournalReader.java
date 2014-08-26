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
package org.graylog2.shared.journal;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import org.graylog2.inputs.gelf.gelf.GELFParser;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.periodical.Periodical;
import org.graylog2.shared.buffers.ProcessBuffer;
import org.graylog2.shared.inputs.InputRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

// TODO this is not behaving well on shutdown, due to a missing stop method in Periodical
public class JournalReader extends Periodical {
    private static final Logger LOG = LoggerFactory.getLogger(JournalReader.class);
    private final KafkaJournal journal;
    private final ProcessBuffer processBuffer;
    private final MetricRegistry metricRegistry;
    private final InputRegistry inputRegistry;
    private GELFParser gelfParser;

    @Inject
    public JournalReader(KafkaJournal journal,
                         ProcessBuffer processBuffer,
                         MetricRegistry metricRegistry,
                         InputRegistry inputRegistry) {
        this.journal = journal;
        this.processBuffer = processBuffer;
        this.metricRegistry = metricRegistry;
        this.inputRegistry = inputRegistry;
        gelfParser = new GELFParser(this.metricRegistry);
    }

    @Override
    public void doRun() {
        sleepUninterruptibly(20, TimeUnit.SECONDS);
        while (true) {
            try {
                final RawMessage raw = journal.read();
                if (raw == null) {
                    LOG.warn("Received invalid null message, this should never happen.");
                    continue;
                }
                LOG.info("Read message {} from journal, position {}", raw.getId(), raw.getSequenceNumber());
                final String payloadDecoderType = raw.getPayloadType();
                if (!"gelf".equals(payloadDecoderType)) {
                    LOG.error("invalid payload type {}", payloadDecoderType);
                }
                final String sourceInputId = raw.getSourceInputId();
                final MessageInput messageInput = inputRegistry.getRunningInput(sourceInputId);
                if (messageInput == null) {
                    LOG.error("Could not load message input {}. Skipping message", sourceInputId);
                    continue;
                }
                final Message parsed = gelfParser.parse(
                        raw.getId(),
                        new String(raw.getPayload(), UTF_8),
                        messageInput);
                try {
                    processBuffer.insertFailFast(parsed, messageInput);
                } catch (BufferOutOfCapacityException e) {
                    e.printStackTrace();
                } catch (ProcessingDisabledException e) {
                    e.printStackTrace();
                }
            } catch (Exception ignore) {
            }
        }
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
    protected Logger getLogger() {
        return LOG;
    }

}
