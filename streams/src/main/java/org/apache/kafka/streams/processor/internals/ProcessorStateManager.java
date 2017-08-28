/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class ProcessorStateManager implements StateManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorStateManager.class);

    public static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
    static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    private final File baseDir;
    private final TaskId taskId;
    private final String logPrefix;
    private final boolean isStandby;
    private final ChangelogReader changelogReader;
    private final Map<String, StateStore> stores;
    private final Map<String, StateStore> globalStores;
    private final Map<TopicPartition, Long> offsetLimits;
    private final Map<TopicPartition, Long> restoredOffsets;
    private final Map<TopicPartition, Long> checkpointedOffsets;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private final Map<String, String> storeToChangelogTopic;
    private final List<TopicPartition> changelogPartitions = new ArrayList<>();

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same topic.
    private final Map<String, TopicPartition> partitionForTopic;
    private OffsetCheckpoint checkpoint;

    /**
     * @throws LockException if the state directory cannot be locked because another thread holds the lock
     *                       (this might be recoverable by retrying)
     * @throws IOException if any severe error happens while creating or locking the state directory
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final boolean isStandby,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic,
                                 final ChangelogReader changelogReader,
                                 final boolean eosEnabled) throws IOException {
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        logPrefix = String.format("task [%s]", taskId);

        partitionForTopic = new HashMap<>();
        for (final TopicPartition source : sources) {
            partitionForTopic.put(source.topic(), source);
        }
        stores = new LinkedHashMap<>();
        globalStores = new HashMap<>();
        offsetLimits = new HashMap<>();
        restoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        restoreCallbacks = isStandby ? new HashMap<String, StateRestoreCallback>() : null;
        this.storeToChangelogTopic = storeToChangelogTopic;

        // get a handle on the parent/base directory of the task directory
        // note that the parent directory could have been accidentally deleted here,
        // so catch that exception if that is the case
        try {
            baseDir = stateDirectory.directoryForTask(taskId);
        } catch (final ProcessorStateException e) {
            throw new LockException(String.format("%s Failed to get the directory for task %s. Exception %s",
                logPrefix, taskId, e));
        }

        // load the checkpoint information
        checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointedOffsets = new HashMap<>(checkpoint.read());

        if (eosEnabled) {
            // delete the checkpoint file after finish loading its stored offsets
            checkpoint.delete();
            checkpoint = null;
        }

        log.debug("{} Created state store manager for task {} with the acquired state dir lock", logPrefix, taskId);
    }


    public static String storeChangelogTopic(final String applicationId, final String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    @Override
    public File baseDir() {
        return baseDir;
    }

    /**
     * @throws IllegalArgumentException if the store name has already been registered or if it is not a valid name
     * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
     *
     * // TODO: parameter loggingEnabled can be removed now
     *
     * @throws StreamsException if the store's change log does not contain the partition
     */
    @Override
    public void register(final StateStore store,
                         final boolean loggingEnabled,
                         final StateRestoreCallback stateRestoreCallback) {
        log.debug("{} Registering state store {} to its state manager", logPrefix, store.name());

        if (store.name().equals(CHECKPOINT_FILE_NAME)) {
            throw new IllegalArgumentException(String.format("%s Illegal store name: %s", logPrefix, CHECKPOINT_FILE_NAME));
        }

        if (stores.containsKey(store.name())) {
            throw new IllegalArgumentException(String.format("%s Store %s has already been registered.", logPrefix, store.name()));
        }

        // check that the underlying change log topic exist or not
        final String topic = storeToChangelogTopic.get(store.name());
        if (topic == null) {
            stores.put(store.name(), store);
            return;
        }

        final TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));
        changelogReader.validatePartitionExists(storePartition, store.name());

        if (isStandby) {
            if (store.persistent()) {
                log.trace("{} Preparing standby replica of persistent state store {} with changelog topic {}", logPrefix, store.name(), topic);

                restoreCallbacks.put(topic, stateRestoreCallback);
            }
        } else {
            log.trace("{} Restoring state store {} from changelog topic {}", logPrefix, store.name(), topic);
            final StateRestorer restorer = new StateRestorer(storePartition,
                                                             new CompositeRestoreListener(stateRestoreCallback),
                                                             checkpointedOffsets.get(storePartition),
                                                             offsetLimit(storePartition),
                                                             store.persistent(),
                                                             store.name());

            changelogReader.register(restorer);
        }
        changelogPartitions.add(storePartition);

        stores.put(store.name(), store);
    }

    @Override
    public Map<TopicPartition, Long> checkpointed() {
        final Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (final Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            final String topicName = entry.getKey();
            final int partition = getPartition(topicName);
            final TopicPartition storePartition = new TopicPartition(topicName, partition);

            if (checkpointedOffsets.containsKey(storePartition)) {
                partitionsAndOffsets.put(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                partitionsAndOffsets.put(storePartition, -1L);
            }
        }
        return partitionsAndOffsets;
    }

    List<ConsumerRecord<byte[], byte[]>> updateStandbyStates(final TopicPartition storePartition,
                                                             final List<ConsumerRecord<byte[], byte[]>> records) {
        final long limit = offsetLimit(storePartition);
        List<ConsumerRecord<byte[], byte[]>> remainingRecords = null;
        final List<KeyValue<byte[], byte[]>> restoreRecords = new ArrayList<>();

        // restore states from changelog records
        final BatchingStateRestoreCallback restoreCallback = getBatchingRestoreCallback(restoreCallbacks.get(storePartition.topic()));

        long lastOffset = -1L;
        int count = 0;
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            if (record.offset() < limit) {
                restoreRecords.add(KeyValue.pair(record.key(), record.value()));
                lastOffset = record.offset();
            } else {
                if (remainingRecords == null) {
                    remainingRecords = new ArrayList<>(records.size() - count);
                }

                remainingRecords.add(record);
            }
            count++;
        }

        if (!restoreRecords.isEmpty()) {
            try {
                restoreCallback.restoreAll(restoreRecords);
            } catch (final Exception e) {
                throw new ProcessorStateException(String.format("%s exception caught while trying to restore state from %s", logPrefix, storePartition), e);
            }
        }

        // record the restored offset for its change log partition
        restoredOffsets.put(storePartition, lastOffset + 1);

        return remainingRecords;
    }

    void putOffsetLimit(final TopicPartition partition, final long limit) {
        log.trace("{} Updating store offset limit for partition {} to {}", logPrefix, partition, limit);
        offsetLimits.put(partition, limit);
    }

    private long offsetLimit(final TopicPartition partition) {
        final Long limit = offsetLimits.get(partition);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    @Override
    public StateStore getStore(final String name) {
        return stores.get(name);
    }

    @Override
    public void flush() {
        if (!stores.isEmpty()) {
            log.debug("{} Flushing all stores registered in the state manager", logPrefix);
            for (final StateStore store : stores.values()) {
                try {
                    log.trace("{} Flushing store={}", logPrefix, store.name());
                    store.flush();
                } catch (final Exception e) {
                    throw new ProcessorStateException(String.format("%s Failed to flush state store %s", logPrefix, store.name()), e);
                }
            }
        }
    }

    /**
     * {@link StateStore#close() Close} all stores (even in case of failure).
     * Re-throw the first
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    @Override
    public void close(final Map<TopicPartition, Long> ackedOffsets) throws ProcessorStateException {
        RuntimeException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!stores.isEmpty()) {
            log.debug("{} Closing its state manager and all the registered state stores", logPrefix);
            for (final Map.Entry<String, StateStore> entry : stores.entrySet()) {
                log.debug("{} Closing storage engine {}", logPrefix, entry.getKey());
                try {
                    entry.getValue().close();
                } catch (final Exception e) {
                    if (firstException == null) {
                        firstException = new ProcessorStateException(String.format("%s Failed to close state store %s", logPrefix, entry.getKey()), e);
                    }
                    log.error("{} Failed to close state store {}: ", logPrefix, entry.getKey(), e);
                }
            }

            if (ackedOffsets != null) {
                checkpoint(ackedOffsets);
            }

        }

        if (firstException != null) {
            throw firstException;
        }
    }

    // write the checkpoint
    @Override
    public void checkpoint(final Map<TopicPartition, Long> ackedOffsets) {
        log.trace("{} Writing checkpoint: {}", logPrefix, ackedOffsets);
        checkpointedOffsets.putAll(changelogReader.restoredOffsets());
        for (final Map.Entry<String, StateStore> entry : stores.entrySet()) {
            final String storeName = entry.getKey();
            // only checkpoint the offset to the offsets file if
            // it is persistent AND changelog enabled
            if (entry.getValue().persistent() && storeToChangelogTopic.containsKey(storeName)) {
                final String changelogTopic = storeToChangelogTopic.get(storeName);
                final TopicPartition topicPartition = new TopicPartition(changelogTopic, getPartition(storeName));
                if (ackedOffsets.containsKey(topicPartition)) {
                    // store the last offset + 1 (the log position after restoration)
                    checkpointedOffsets.put(topicPartition, ackedOffsets.get(topicPartition) + 1);
                } else if (restoredOffsets.containsKey(topicPartition)) {
                    checkpointedOffsets.put(topicPartition, restoredOffsets.get(topicPartition));
                }
            }
        }
        // write the checkpoint file before closing, to indicate clean shutdown
        try {
            if (checkpoint == null) {
                checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
            }
            checkpoint.write(checkpointedOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write checkpoint file to {}:", new File(baseDir, CHECKPOINT_FILE_NAME), e);
        }
    }

    private int getPartition(final String topic) {
        final TopicPartition partition = partitionForTopic.get(topic);
        return partition == null ? taskId.partition : partition.partition();
    }

    void registerGlobalStateStores(final List<StateStore> stateStores) {
        log.debug("{} Register global stores {}", logPrefix, stateStores);
        for (final StateStore stateStore : stateStores) {
            globalStores.put(stateStore.name(), stateStore);
        }
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return globalStores.get(name);
    }

    private BatchingStateRestoreCallback getBatchingRestoreCallback(StateRestoreCallback callback) {
        if (callback instanceof BatchingStateRestoreCallback) {
            return (BatchingStateRestoreCallback) callback;
        }

        return new WrappedBatchingStateRestoreCallback(callback);
    }

    Collection<TopicPartition> changelogPartitions() {
        return changelogPartitions;
    }
}
