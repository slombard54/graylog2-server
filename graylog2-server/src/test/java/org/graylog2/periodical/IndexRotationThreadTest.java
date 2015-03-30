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
package org.graylog2.periodical;

import org.graylog2.indexer.Deflector;
import org.graylog2.indexer.NoTargetIndexException;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.initializers.IndexerSetupService;
import org.graylog2.notifications.NotificationService;
import org.graylog2.plugin.indexer.rotation.RotationStrategy;
import org.graylog2.system.activities.SystemMessageActivityWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.annotation.Nullable;
import javax.inject.Provider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IndexRotationThreadTest {
    @Mock
    private Deflector deflector;

    @Test
    public void testFailedRotation() {
        final Provider<RotationStrategy> provider = new Provider<RotationStrategy>() {
            @Override
            public RotationStrategy get() {
                return new RotationStrategy() {
                    @Nullable
                    @Override
                    public Result shouldRotate(String index) {
                        return null;
                    }
                };
            }
        };

        final IndexRotationThread rotationThread = new IndexRotationThread(
                mock(NotificationService.class),
                mock(Indices.class),
                deflector,
                mock(SystemMessageActivityWriter.class),
                mock(IndexerSetupService.class),
                provider
        );

        rotationThread.checkForRotation();

        verify(deflector, never()).cycle();
    }

    @Test
    public void testPerformRotation() throws NoTargetIndexException {
        final Provider<RotationStrategy> provider = new Provider<RotationStrategy>() {
            @Override
            public RotationStrategy get() {
                return new RotationStrategy() {
                    @Nullable
                    @Override
                    public Result shouldRotate(String index) {
                        return new Result() {
                            @Override
                            public String getDescription() {
                                return "performed";
                            }

                            @Override
                            public boolean shouldRotate() {
                                return true;
                            }
                        };
                    }
                };
            }
        };

        final IndexRotationThread rotationThread = new IndexRotationThread(
                mock(NotificationService.class),
                mock(Indices.class),
                deflector,
                mock(SystemMessageActivityWriter.class),
                mock(IndexerSetupService.class),
                provider
        );

        when(deflector.getNewestTargetName()).thenReturn("some_index");

        rotationThread.checkForRotation();

        verify(deflector, times(1)).cycle();
        verify(deflector, times(1)).getNewestTargetName();
    }

    @Test
    public void testDontPerformRotation() throws NoTargetIndexException {
        final Provider<RotationStrategy> provider = new Provider<RotationStrategy>() {
            @Override
            public RotationStrategy get() {
                return new RotationStrategy() {
                    @Nullable
                    @Override
                    public Result shouldRotate(String index) {
                        return new Result() {
                            @Override
                            public String getDescription() {
                                return "performed";
                            }

                            @Override
                            public boolean shouldRotate() {
                                return false;
                            }
                        };
                    }
                };
            }
        };

        final IndexRotationThread rotationThread = new IndexRotationThread(
                mock(NotificationService.class),
                mock(Indices.class),
                deflector,
                mock(SystemMessageActivityWriter.class),
                mock(IndexerSetupService.class),
                provider
        );

        when(deflector.getNewestTargetName()).thenReturn("some_index");

        rotationThread.checkForRotation();

        verify(deflector, never()).cycle();
        verify(deflector, times(1)).getNewestTargetName();
    }
}
