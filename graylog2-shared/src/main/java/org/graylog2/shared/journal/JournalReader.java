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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
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

// TODO this is not behaving well on shutdown, due to a missing stop method in Periodical
public class JournalReader extends Periodical {
    private static final Logger log = LoggerFactory.getLogger(JournalReader.class);
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
                    log.warn("Received invalid null message, this should never happen.");
                    continue;
                }
                log.info("Read message {} from journal, position {}", raw.getId(), raw.getSequenceNumber());
                final String payloadDecoderType = raw.getPayloadType();
                if (!"gelf".equals(payloadDecoderType)) {
                    log.error("invalid payload type {}", payloadDecoderType);
                }
                final String sourceInputId = raw.getSourceInputId();
                final MessageInput messageInput = inputRegistry.getRunningInput(sourceInputId);
                if (messageInput == null) {
                    log.error("Could not load message input {}. Skipping message", sourceInputId);
                    continue;
                }
                final Message parsed = gelfParser.parse(
                        raw.getId(),
                        raw.getPayload().toString(Charsets.UTF_8),
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
        return log;
    }

}
