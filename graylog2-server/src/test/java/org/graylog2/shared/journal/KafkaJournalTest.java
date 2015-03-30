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
package org.graylog2.shared.journal;

import com.codahale.metrics.MetricRegistry;
import com.github.joschi.jadconfig.util.Size;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import kafka.log.LogSegment;
import kafka.utils.FileLock;
import org.graylog2.plugin.InstantMillisProvider;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.filefilter.FileFilterUtils.and;
import static org.apache.commons.io.filefilter.FileFilterUtils.directoryFileFilter;
import static org.apache.commons.io.filefilter.FileFilterUtils.fileFileFilter;
import static org.apache.commons.io.filefilter.FileFilterUtils.nameFileFilter;
import static org.apache.commons.io.filefilter.FileFilterUtils.suffixFileFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KafkaJournalTest {
    private static final int BULK_SIZE = 200;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ScheduledThreadPoolExecutor scheduler;
    private File journalDirectory;

    @Before
    public void setUp() throws IOException {
        scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.prestartCoreThread();
        journalDirectory = temporaryFolder.newFolder();
    }

    @After
    public void tearDown() {
        scheduler.shutdown();
    }

    @Test
    public void writeAndRead() throws IOException {
        final Journal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.megabytes(100l),
                Duration.standardHours(1),
                Size.megabytes(5l),
                Duration.standardHours(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());

        final byte[] idBytes = "id".getBytes(UTF_8);
        final byte[] messageBytes = "message".getBytes(UTF_8);

        final long position = journal.write(idBytes, messageBytes);
        final List<Journal.JournalReadEntry> messages = journal.read(1);

        final Journal.JournalReadEntry firstMessage = Iterators.getOnlyElement(messages.iterator());

        assertEquals(new String(firstMessage.getPayload(), UTF_8), "message");
    }

    @Test
    public void readAtLeastOne() throws Exception {
        final Journal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.megabytes(100l),
                Duration.standardHours(1),
                Size.megabytes(5l),
                Duration.standardHours(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());

        final byte[] idBytes = "id".getBytes(UTF_8);
        final byte[] messageBytes = "message1".getBytes(UTF_8);

        final long position = journal.write(idBytes, messageBytes);

        // Trying to read 0 should always read at least 1 entry.
        final List<Journal.JournalReadEntry> messages = journal.read(0);

        final Journal.JournalReadEntry firstMessage = Iterators.getOnlyElement(messages.iterator());

        assertEquals(new String(firstMessage.getPayload(), UTF_8), "message1");
    }

    private void createBulkChunks(KafkaJournal journal, int bulkCount) {
        // perform multiple writes to make multiple segments
        for (int currentBulk = 0; currentBulk < bulkCount; currentBulk++) {
            final List<Journal.Entry> entries = Lists.newArrayList();
            // write enough bytes in one go to be over the 1024 byte segment limit, which causes a segment roll
            for (int i = 0; i < BULK_SIZE; i++) {
                final byte[] idBytes = ("id" + i).getBytes(UTF_8);
                final byte[] messageBytes = ("message " + i).getBytes(UTF_8);

                entries.add(journal.createEntry(idBytes, messageBytes));

            }
            journal.write(entries);
        }
    }

    private int countSegmentsInDir(File messageJournalFile) {
        // let it throw
        return messageJournalFile.list(and(fileFileFilter(), suffixFileFilter(".log"))).length;
    }

    @Test
    public void segmentRotation() throws Exception {
        final KafkaJournal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.kilobytes(1l),
                Duration.standardHours(1),
                Size.kilobytes(10l),
                Duration.standardDays(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());

        createBulkChunks(journal, 3);

        final File[] files = journalDirectory.listFiles();
        assertNotNull(files);
        assertTrue("there should be files in the journal directory", files.length > 0);

        final File[] messageJournalDir = journalDirectory.listFiles((FileFilter) and(directoryFileFilter(),
                nameFileFilter("messagejournal-0")));
        assertTrue(messageJournalDir.length == 1);
        final File[] logFiles = messageJournalDir[0].listFiles((FileFilter) and(fileFileFilter(),
                suffixFileFilter(".log")));
        assertEquals("should have two journal segments", 3, logFiles.length);
    }

    @Test
    public void segmentSizeCleanup() throws Exception {
        final KafkaJournal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.kilobytes(1l),
                Duration.standardHours(1),
                Size.kilobytes(10l),
                Duration.standardDays(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());
        final File messageJournalDir = new File(journalDirectory, "messagejournal-0");
        assertTrue(messageJournalDir.exists());

        // create enough chunks so that we exceed the maximum journal size we configured
        createBulkChunks(journal, 3);

        // make sure everything is on disk
        journal.flushDirtyLogs();

        assertEquals(countSegmentsInDir(messageJournalDir), 3);

        final int cleanedLogs = journal.cleanupLogs();
        assertEquals(cleanedLogs, 1);

        final int numberOfSegments = countSegmentsInDir(messageJournalDir);
        assertEquals(numberOfSegments, 2);
    }

    @Test
    public void segmentAgeCleanup() throws Exception {
        final InstantMillisProvider clock = new InstantMillisProvider(DateTime.now());

        DateTimeUtils.setCurrentMillisProvider(clock);
        try {

            final KafkaJournal journal = new KafkaJournal(journalDirectory,
                    scheduler,
                    Size.kilobytes(1l),
                    Duration.standardHours(1),
                    Size.kilobytes(10l),
                    Duration.standardMinutes(1),
                    1_000_000,
                    Duration.standardMinutes(1),
                    new MetricRegistry());
            final File messageJournalDir = new File(journalDirectory, "messagejournal-0");
            assertTrue(messageJournalDir.exists());

            // we need to fix up the last modified times of the actual files.
            long lastModifiedTs[] = new long[2];

            // create two chunks, 30 seconds apart
            createBulkChunks(journal, 1);
            journal.flushDirtyLogs();
            lastModifiedTs[0] = clock.getMillis();

            clock.tick(Period.seconds(30));

            createBulkChunks(journal, 1);
            journal.flushDirtyLogs();
            lastModifiedTs[1] = clock.getMillis();

            int i = 0;
            for (final LogSegment segment : journal.getSegments()) {
                assertTrue(i < 2);
                segment.lastModified_$eq(lastModifiedTs[i]);
                i++;
            }

            int cleanedLogs = journal.cleanupLogs();
            assertEquals("no segments should've been cleaned", cleanedLogs, 0);
            assertEquals("two segments segment should remain", countSegmentsInDir(messageJournalDir), 2);

            // move clock beyond the retention period and clean again
            clock.tick(Period.seconds(120));

            cleanedLogs = journal.cleanupLogs();
            assertEquals("two segments should've been cleaned (only one will actually be removed...)", cleanedLogs, 2);
            assertEquals("one segment should remain", countSegmentsInDir(messageJournalDir), 1);

        } finally {
            DateTimeUtils.setCurrentMillisSystem();
        }
    }

    @Test
    public void segmentCommittedCleanup() throws Exception {
        final KafkaJournal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.kilobytes(1l),
                Duration.standardHours(1),
                Size.petabytes(1l), // never clean by size in this test
                Duration.standardDays(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());
        final File messageJournalDir = new File(journalDirectory, "messagejournal-0");
        assertTrue(messageJournalDir.exists());

        createBulkChunks(journal, 3);

        // make sure everything is on disk
        journal.flushDirtyLogs();

        assertEquals(countSegmentsInDir(messageJournalDir), 3);

        // we haven't committed any offsets, this should not touch anything.
        final int cleanedLogs = journal.cleanupLogs();
        assertEquals(cleanedLogs, 0);

        final int numberOfSegments = countSegmentsInDir(messageJournalDir);
        assertEquals(numberOfSegments, 3);

        // mark first half of first segment committed, should not clean anything
        journal.markJournalOffsetCommitted(BULK_SIZE / 2);
        assertEquals("should not touch segments", journal.cleanupLogs(), 0);
        assertEquals(countSegmentsInDir(messageJournalDir), 3);

        journal.markJournalOffsetCommitted(BULK_SIZE + 1);
        assertEquals("first segment should've been purged", journal.cleanupLogs(), 1);
        assertEquals(countSegmentsInDir(messageJournalDir), 2);

        journal.markJournalOffsetCommitted(BULK_SIZE * 4);
        assertEquals("only purge one segment, not the active one", journal.cleanupLogs(), 1);
        assertEquals(countSegmentsInDir(messageJournalDir), 1);
    }

    @Test(expected = RuntimeException.class)
    public void lockedJournalDir() throws Exception {
        // Grab the lock before starting the KafkaJournal.
        final File file = new File(journalDirectory, ".lock");
        file.createNewFile();
        final FileLock fileLock = new FileLock(file);
        fileLock.tryLock();

        final Journal journal = new KafkaJournal(journalDirectory,
                scheduler,
                Size.megabytes(100l),
                Duration.standardHours(1),
                Size.megabytes(5l),
                Duration.standardHours(1),
                1_000_000,
                Duration.standardMinutes(1),
                new MetricRegistry());
    }

    public static void deleteDirectory(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
