/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.estore;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
class TestErrorCollector {

    @Mock
    private Logger logger;
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ScheduledFuture<?> future;
    @Mock
    private Persistor<TestEventToStore> persistor;
    private final CradleEntitiesFactory entitiesFactory = spy(new CradleEntitiesFactory(1_024^2, 1_024^2, 30_000L));
    private final StoredTestEventId rootEvent = new StoredTestEventId(
            new BookId("test-book"),
            "test-scope",
            Instant.now(),
            "test-id"
    );
    private ErrorCollector errorCollector;
    @Captor
    private ArgumentCaptor<Runnable> taskCaptor;

    @BeforeEach
    void beforeEach() {
        doReturn(future).when(executor).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        errorCollector = CradleErrorCollector.create(executor, entitiesFactory, 1L, TimeUnit.MINUTES);
        errorCollector.init(persistor, rootEvent);
        verify(executor).scheduleAtFixedRate(taskCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.MINUTES));
        verifyNoMoreInteractions(executor);
        clearInvocations(executor);
    }

    @AfterEach
    void afterEach() {
        verifyNoMoreInteractions(logger);
        verifyNoMoreInteractions(executor);
        verifyNoMoreInteractions(future);
        verifyNoMoreInteractions(entitiesFactory);
        verifyNoMoreInteractions(persistor);
    }

    @ParameterizedTest
    @CsvSource({
            "60000,MICROSECONDS",
            "60,SECONDS",
            "1,MINUTES",
    })
    void testDrainTaskParameters(long period, TimeUnit timeUnit) throws Exception {
        try(ErrorCollector collector = CradleErrorCollector.create(executor, entitiesFactory, period, timeUnit)) {
            collector.init(persistor, rootEvent);
            verify(executor).scheduleAtFixedRate(taskCaptor.capture(), eq(period), eq(period), eq(timeUnit));
        }
        verify(future).cancel(eq(true));
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    @Test
    void testCollect() throws Exception {
        errorCollector.collect("A");
        for (int i = 0; i < 2; i++) {
            errorCollector.collect("B");
        }
        verifyNoMoreInteractions(persistor);

        taskCaptor.getValue().run();

        ArgumentCaptor<TestEventToStore> eventBatchCaptor = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor).persist(eventBatchCaptor.capture(), any());
        verify(entitiesFactory).testEventBuilder();

        assertTrue(eventBatchCaptor.getValue().isSingle());
        TestEventSingleToStore event = eventBatchCaptor.getValue().asSingle();

        assertEquals("estore internal problem(s): 3", event.getName());
        assertEquals("InternalError", event.getType());
        assertFalse(event.isSuccess());

        String body = new String(event.getContent());
        assertTrue(body.matches(".*\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+Z\",\"quantity\":1}.*"), () -> "body: " + body);
        assertTrue(body.matches(".*\"B\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+Z\",\"lastDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+Z\",\"quantity\":2}.*"), () -> "body: " + body);

        taskCaptor.getValue().run();
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    @Test
    void testLogAndCollect() throws Exception {
        RuntimeException exception = new RuntimeException("test-message");
        errorCollector.collect(logger, "A", exception);
        verify(logger).error(eq("A"), same(exception));

        verifyNoMoreInteractions(logger);
        verifyNoMoreInteractions(persistor);

        taskCaptor.getValue().run();

        ArgumentCaptor<TestEventToStore> eventBatchCaptor = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor).persist(eventBatchCaptor.capture(), any());
        verify(entitiesFactory).testEventBuilder();

        assertTrue(eventBatchCaptor.getValue().isSingle());
        TestEventSingleToStore event = eventBatchCaptor.getValue().asSingle();

        assertEquals("estore internal problem(s): 1", event.getName());
        assertEquals("InternalError", event.getType());
        assertFalse(event.isSuccess());

        String body = new String(event.getContent());
        assertTrue(body.matches("\\[\\{\"errors\":\\{\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+Z\",\"quantity\":1}}}]"), () -> "body: " + body);

        taskCaptor.getValue().run();
    }

    @Test
    void testClose() throws Exception {
        errorCollector.collect("A");
        verifyNoMoreInteractions(persistor);

        errorCollector.close();

        verify(future).cancel(eq(true));

        ArgumentCaptor<TestEventToStore> eventBatchCaptor = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor).persist(eventBatchCaptor.capture(), any());
        verify(entitiesFactory).testEventBuilder();

        assertTrue(eventBatchCaptor.getValue().isSingle());
        TestEventSingleToStore event = eventBatchCaptor.getValue().asSingle();

        assertEquals("estore internal problem(s): 1", event.getName());
        assertEquals("InternalError", event.getType());
        assertFalse(event.isSuccess());

        String body = new String(event.getContent());
        assertTrue(body.matches("\\[\\{\"errors\":\\{\"A\":\\{\"firstDate\":\"\\d+-\\d+-\\d+T\\d+:\\d+:\\d+.\\d+Z\",\"quantity\":1}}}]"), () -> "body: " + body);
    }
}