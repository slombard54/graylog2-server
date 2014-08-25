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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.log.CleanerConfig;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.message.NoCompressionCodec$;
import kafka.utils.KafkaScheduler;
import kafka.utils.SystemTime$;
import org.graylog2.plugin.journal.RawMessage;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Map$;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Singleton
public class KafkaJournal {
    private static final Logger log = LoggerFactory.getLogger(KafkaJournal.class);

    private final LogManager logManager;
    private final Log kafkaLog;
    private final Semaphore messagesInJournal = new Semaphore(0);

    public static final NoCompressionCodec$ NO_COMPRESSION_CODEC = NoCompressionCodec$.MODULE$;

    private long readOffset = 0L; // TODO read from persisted store

    @Inject
    public KafkaJournal(@Named("spoolDirectory") String spoolDir) {

        // TODO all of these configuration values need tweaking
        // these are the default values as per kafka 0.8.1.1
        final LogConfig defaultConfig =
                new LogConfig(
                        1024 * 1024,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Integer.MAX_VALUE,
                        1024 * 1024,
                        4096,
                        60 * 1000,
                        24 * 60 * 60 * 1000L,
                        0.5,
                        false
                );
        // these are the default values as per kafka 0.8.1.1, except we don't turn on the cleaner
        final CleanerConfig cleanerConfig =
                new CleanerConfig(
                        1,
                        4 * 1024 * 1024L,
                        0.9d,
                        1024 * 1024,
                        32 * 1024 * 1024,
                        5 * 1024 * 1024L,
                        TimeUnit.SECONDS.toMillis(15),
                        false,
                        "MD5");
        logManager = new LogManager(
                new File[]{new File(spoolDir)},
                Map$.MODULE$.<String, LogConfig>empty(),
                defaultConfig,
                cleanerConfig,
                TimeUnit.SECONDS.toMillis(60),
                TimeUnit.SECONDS.toMillis(60),
                TimeUnit.SECONDS.toMillis(60),
                new KafkaScheduler(2, "relay", false),
                SystemTime$.MODULE$);

        final TopicAndPartition topicAndPartition = new TopicAndPartition("rawmessages", 0);
        final Option<Log> messageLog = logManager.getLog(topicAndPartition);
        if (messageLog.isEmpty()) {
            kafkaLog = logManager.createLog(topicAndPartition, logManager.defaultConfig());
        } else {
            kafkaLog = messageLog.get();
        }
        log.info("initialized kafka based journal at {}", spoolDir);

        final Thread thread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    final RawMessage message = read();
                    if (message != null) {
                        log.info("Read message {}", message);
                    }
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
    }


    public void write(RawMessage rawMessage) {

        final ChannelBuffer buffer = rawMessage.encode();
        final byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(0, bytes);
        }
        final KafkaMessage kafkaMessage = new KafkaMessage(bytes, rawMessage.getIdBytes(), NO_COMPRESSION_CODEC);

        final ByteBufferMessageSet messageSet =
                new ByteBufferMessageSet(JavaConversions.asScalaBuffer(
                                                 Collections.<kafka.message.Message>singletonList(kafkaMessage)));
        final Log.LogAppendInfo appendInfo = kafkaLog.append(messageSet, true);
        messagesInJournal.release();
        log.info("journalled message: {} bytes, log position {}", bytes.length, appendInfo.firstOffset());
    }

    public RawMessage read() {
        MessageSet messageSet = null;
        while (messageSet == null || messageSet.isEmpty()) {
            try {
                messageSet = kafkaLog.read(readOffset, 10 * 1024, Option.<Object>apply(readOffset + 1));
                if (messageSet.isEmpty()) {
                    log.info("No more messages to read, blocking.");
                    messagesInJournal.acquireUninterruptibly();
                } else {
                    // mark that we've taken a message
                    log.info("More messages arrived, reading.");
                    messagesInJournal.drainPermits();
                }
            } catch (OffsetOutOfRangeException e) {
                // there are no more messages to read from the log, we need to wait until new ones are available;
                messagesInJournal.acquireUninterruptibly();
                log.info("woken up");
            }
        }

        final Iterator<MessageAndOffset> iterator = messageSet.iterator();
        if (!(iterator.hasNext())) return null;

        final MessageAndOffset messageAndOffset = iterator.next();
        readOffset = messageAndOffset.nextOffset();
        final ByteBuffer payload = messageAndOffset.message().payload();
        log.info("Read message sequence number {}", messageAndOffset.offset());
        return RawMessage.decode(ChannelBuffers.wrappedBuffer(payload));

        // TODO now lookup the payload type parser and convert rawMessage to Message
        // then insert into

    }


}
