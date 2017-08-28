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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask {

    private static final Logger log = LoggerFactory.getLogger(StandbyTask.class);
    private Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();

    /**
     * Create {@link StandbyTask} with its assigned partitions
     *
     * @param id             the ID of this task
     * @param applicationId  the ID of the stream processing application
     * @param partitions     the collection of assigned {@link TopicPartition}
     * @param topology       the instance of {@link ProcessorTopology}
     * @param consumer       the instance of {@link Consumer}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param metrics        the {@link StreamsMetrics} created by the thread
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final String applicationId,
                final Collection<TopicPartition> partitions,
                final ProcessorTopology topology,
                final Consumer<byte[], byte[]> consumer,
                final ChangelogReader changelogReader,
                final StreamsConfig config,
                final StreamsMetrics metrics,
                final StateDirectory stateDirectory) {
        super(id, applicationId, partitions, topology, consumer, changelogReader, true, stateDirectory, config);

        // initialize the topology with its own context
        processorContext = new StandbyContextImpl(id, applicationId, config, stateMgr, metrics);
    }

    /**
     * <pre>
     * - update offset limits
     * </pre>
     */
    @Override
    public void resume() {
        log.debug("{} Resuming", logPrefix);
        updateOffsetLimits();
    }

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * - update offset limits
     * </pre>
     */
    @Override
    public void commit() {
        log.trace("{} Committing", logPrefix);
        flushAndCheckpointState();
        // reinitialize offset limits
        updateOffsetLimits();
    }

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * </pre>
     */
    @Override
    public void suspend() {
        log.debug("{} Suspending", logPrefix);
        flushAndCheckpointState();
    }

    private void flushAndCheckpointState() {
        stateMgr.flush();
        stateMgr.checkpoint(Collections.<TopicPartition, Long>emptyMap());
    }

    /**
     * <pre>
     * - {@link #commit()}
     * - close state
     * <pre>
     * @param clean ignored by {@code StandbyTask} as it can always try to close cleanly
     *              (ie, commit, flush, and write checkpoint file)
     */
    @Override
    public void close(final boolean clean) {
        if (!taskInitialized) {
            return;
        }
        log.debug("{} Closing", logPrefix);
        boolean committedSuccessfully = false;
        try {
            commit();
            committedSuccessfully = true;
        } finally {
            closeStateManager(committedSuccessfully);
        }
    }

    @Override
    public void closeSuspended(final boolean clean, final RuntimeException e) {
        close(clean);
    }

    @Override
    public boolean maybePunctuateStreamTime() {
        throw new UnsupportedOperationException("maybePunctuateStreamTime not supported by StandbyTask");
    }

    @Override
    public boolean maybePunctuateSystemTime() {
        throw new UnsupportedOperationException("maybePunctuateSystemTime not supported by StandbyTask");
    }

    @Override
    public boolean commitNeeded() {
        return false;
    }

    /**
     * Updates a state store using records from one change log partition
     *
     * @return a list of records not consumed
     */
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition,
                                                       final List<ConsumerRecord<byte[], byte[]>> records) {
        log.trace("{} Updating standby replicas of its state store for partition [{}]", logPrefix, partition);
        return stateMgr.updateStandbyStates(partition, records);
    }

    @Override
    public int addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        throw new UnsupportedOperationException("add records not supported by StandbyTasks");
    }

    public Map<TopicPartition, Long> checkpointedOffsets() {
        return checkpointedOffsets;
    }

    @Override
    public boolean process() {
        throw new UnsupportedOperationException("process not supported by StandbyTasks");
    }

    public boolean initialize() {
        initializeStateStores();
        checkpointedOffsets = Collections.unmodifiableMap(stateMgr.checkpointed());
        processorContext.initialized();
        taskInitialized = true;
        return true;
    }

}