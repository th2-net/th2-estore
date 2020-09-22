/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.th2.infra.grpc.Event;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceVertxImplBase;
import com.exactpro.th2.eventstore.grpc.Response;
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest;
import com.exactpro.th2.eventstore.grpc.StoreEventRequest;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.store.common.utils.AsyncHelper;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import io.reactivex.Single;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.StringValue.of;


public class ReportEventStoreService extends EventStoreServiceVertxImplBase {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private static final String STORAGE_LOCK_NAME = "CassandraStorageLock";
    private final CradleManager cradleManager;
    private final Vertx vertx;

    public ReportEventStoreService(CradleManager cradleManager, Vertx vertx) {
        this.cradleManager = cradleManager;
        this.vertx = vertx;
    }

    @Override
    public void storeEvent(StoreEventRequest request, Promise<Response> response) {
        AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, storeEvent(request)
            .doOnSuccess(id -> response.complete(Response.newBuilder().setId(of(id)).build()))
            .doOnError(e -> {
                response.complete(Response.newBuilder().setError(of(e.toString())).build());
                logger.error("store event error: {}", e.getMessage(), e);
            })
            .ignoreElement()
        ).subscribe();
    }

    @Override
    public void storeEventBatch(StoreEventBatchRequest request, Promise<Response> response) {
        AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, storeEventBatch(request)
            .doOnSuccess(id -> response.complete(Response.newBuilder().setId(of(id)).build()))
            .doOnError(e -> {
                response.complete(Response.newBuilder().setError(of(e.toString())).build());
                logger.error("store event error: {}", e.getMessage(), e);
            })
            .ignoreElement()
        ).subscribe();
    }

    private Single<String> storeEvent(StoreEventRequest request) {
        return Single.just(request.getEvent())
            .flatMap(protoEvent -> vertx.rxExecuteBlocking(
                AsyncHelper.createHandler(() -> {

                    try {
                        StoredTestEventSingle cradleEventSingle = StoredTestEvent.newStoredTestEventSingle(ProtoUtil.toCradleEvent(protoEvent));

                        cradleManager.getStorage().storeTestEvent(cradleEventSingle);
                        logger.debug("Stored single event id '{}' parent id '{}'",
                            cradleEventSingle.getId(), cradleEventSingle.getParentId());

                        storeAttachedMessages(null, protoEvent);

                        return String.valueOf(cradleEventSingle.getId());
                    } catch (RuntimeException e) {
                        logger.error("Evet storing '{}' failed", protoEvent, e);
                        throw e;
                    }
                })
                ).toSingle()
            );
    }

    private Single<String> storeEventBatch(StoreEventBatchRequest request) {
        return Single.just(request.getEventBatch())
            .flatMap(protoBatch -> vertx.rxExecuteBlocking(
                AsyncHelper.createHandler(() -> {
                    try {
                        StoredTestEventBatch cradleBatch = ProtoUtil.toCradleBatch(protoBatch);
                        cradleManager.getStorage().storeTestEvent(cradleBatch);
                        logger.debug("Stored batch id '{}' parent id '{}' size '{}'",
                            cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount());

                        for (Event protoEvent : protoBatch.getEventsList()) {
                            storeAttachedMessages(cradleBatch.getId(), protoEvent);
                        }
                        return String.valueOf(cradleBatch.getId());
                    } catch (RuntimeException e) {
                        logger.error("Evet batch storing '{}' failed", protoBatch, e);
                        throw e;
                    }
                })
                ).toSingle()
            );
    }

    private void storeAttachedMessages(StoredTestEventId batchID, Event protoEvent) throws IOException {
        List<MessageID> attachedMessageIds = protoEvent.getAttachedMessageIdsList();
        if (!attachedMessageIds.isEmpty()) {
            List<StoredMessageId> messagesIds = attachedMessageIds.stream()
                .map(ProtoUtil::toStoredMessageId)
                .collect(Collectors.toList());

            cradleManager.getStorage().storeTestEventMessagesLink(
                ProtoUtil.toCradleEventID(protoEvent.getId()),
                batchID,
                messagesIds);
            logger.debug("Stored attached messages '{}' to event id '{}'", messagesIds, protoEvent.getId().getId());
        }
    }
}
