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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ProduceRequest extends AbstractRequest {
    private static final String TRANSACTIONAL_ID_KEY_NAME = "transactional_id";
    private static final String ACKS_KEY_NAME = "acks";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_DATA_KEY_NAME = "topic_data";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_DATA_KEY_NAME = "data";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final byte magic;
        private final short acks;
        private final int timeout;
        private final Map<TopicPartition, MemoryRecords> partitionRecords;
        private final String transactionalId;

        public Builder(byte magic,
                       short acks,
                       int timeout,
                       Map<TopicPartition, MemoryRecords> partitionRecords,
                       String transactionalId) {
            super(ApiKeys.PRODUCE, (short) (magic == RecordBatch.MAGIC_VALUE_V2 ? 3 : 2));
            this.magic = magic;
            this.acks = acks;
            this.timeout = timeout;
            this.partitionRecords = partitionRecords;
            this.transactionalId = transactionalId;
        }

        public Builder(byte magic,
                       short acks,
                       int timeout,
                       Map<TopicPartition, MemoryRecords> partitionRecords) {
            this(magic, acks, timeout, partitionRecords, null);
        }

        @Override
        public ProduceRequest build(short version) {
            if (version < 2)
                throw new UnsupportedVersionException("ProduceRequest versions older than 2 are not supported.");

            return new ProduceRequest(version, acks, timeout, partitionRecords, transactionalId);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ProduceRequest")
                    .append(", magic=").append(magic)
                    .append(", acks=").append(acks)
                    .append(", timeout=").append(timeout)
                    .append(", partitionRecords=(").append(partitionRecords)
                    .append("), transactionalId='").append(transactionalId != null ? transactionalId : "")
                    .append("'");
            return bld.toString();
        }
    }

    private final short acks;
    private final int timeout;
    private final String transactionalId;

    private final Map<TopicPartition, Integer> partitionSizes;

    // This is set to null by `clearPartitionRecords` to prevent unnecessary memory retention when a produce request is
    // put in the purgatory (due to client throttling, it can take a while before the response is sent).
    // Care should be taken in methods that use this field.
    private volatile Map<TopicPartition, MemoryRecords> partitionRecords;
    private boolean transactional = false;
    private boolean idempotent = false;

    private ProduceRequest(short version, short acks, int timeout, Map<TopicPartition, MemoryRecords> partitionRecords, String transactionalId) {
        super(version);
        this.acks = acks;
        this.timeout = timeout;

        this.transactionalId = transactionalId;
        this.partitionRecords = partitionRecords;
        this.partitionSizes = createPartitionSizes(partitionRecords);

        for (MemoryRecords records : partitionRecords.values())
            validateRecords(version, records);
    }

    private static Map<TopicPartition, Integer> createPartitionSizes(Map<TopicPartition, MemoryRecords> partitionRecords) {
        Map<TopicPartition, Integer> result = new HashMap<>(partitionRecords.size());
        for (Map.Entry<TopicPartition, MemoryRecords> entry : partitionRecords.entrySet())
            result.put(entry.getKey(), entry.getValue().sizeInBytes());
        return result;
    }

    public ProduceRequest(Struct struct, short version) {
        super(version);
        partitionRecords = new HashMap<>();
        for (Object topicDataObj : struct.getArray(TOPIC_DATA_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicData.getArray(PARTITION_DATA_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                MemoryRecords records = (MemoryRecords) partitionResponse.getRecords(RECORD_SET_KEY_NAME);
                validateRecords(version, records);
                partitionRecords.put(new TopicPartition(topic, partition), records);
            }
        }
        partitionSizes = createPartitionSizes(partitionRecords);
        acks = struct.getShort(ACKS_KEY_NAME);
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
        transactionalId = struct.hasField(TRANSACTIONAL_ID_KEY_NAME) ? struct.getString(TRANSACTIONAL_ID_KEY_NAME) : null;
    }

    private void validateRecords(short version, MemoryRecords records) {
        if (version >= 3) {
            Iterator<MutableRecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext())
                throw new InvalidRecordException("Produce requests with version " + version + " must have at least " +
                        "one record batch");

            MutableRecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2)
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                        "contain record batches with magic version 2");

            if (iterator.hasNext())
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                        "contain exactly one record batch");
            idempotent = entry.hasProducerId();
            transactional = entry.isTransactional();
        }

        // Note that we do not do similar validation for older versions to ensure compatibility with
        // clients which send the wrong magic version in the wrong version of the produce request. The broker
        // did not do this validation before, so we maintain that behavior here.
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        // Store it in a local variable to protect against concurrent updates
        Map<TopicPartition, MemoryRecords> partitionRecords = partitionRecordsOrFail();
        short version = version();
        Struct struct = new Struct(ApiKeys.PRODUCE.requestSchema(version));
        Map<String, Map<Integer, MemoryRecords>> recordsByTopic = CollectionUtils.groupDataByTopic(partitionRecords);
        struct.set(ACKS_KEY_NAME, acks);
        struct.set(TIMEOUT_KEY_NAME, timeout);

        if (struct.hasField(TRANSACTIONAL_ID_KEY_NAME))
            struct.set(TRANSACTIONAL_ID_KEY_NAME, transactionalId);

        List<Struct> topicDatas = new ArrayList<>(recordsByTopic.size());
        for (Map.Entry<String, Map<Integer, MemoryRecords>> topicEntry : recordsByTopic.entrySet()) {
            Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, MemoryRecords> partitionEntry : topicEntry.getValue().entrySet()) {
                MemoryRecords records = partitionEntry.getValue();
                Struct part = topicData.instance(PARTITION_DATA_KEY_NAME)
                        .set(PARTITION_KEY_NAME, partitionEntry.getKey())
                        .set(RECORD_SET_KEY_NAME, records);
                partitionArray.add(part);
            }
            topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(TOPIC_DATA_KEY_NAME, topicDatas.toArray());
        return struct;
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks)
                .append(",timeout=").append(timeout);

        if (verbose)
            bld.append(",partitionSizes=").append(Utils.mkString(partitionSizes, "[", "]", "=", ","));
        else
            bld.append(",numPartitions=").append(partitionSizes.size());

        bld.append("}");
        return bld.toString();
    }

    @Override
    public ProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0)
            return null;

        Errors error = Errors.forException(e);
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        ProduceResponse.PartitionResponse partitionResponse = new ProduceResponse.PartitionResponse(error);

        for (TopicPartition tp : partitions())
            responseMap.put(tp, partitionResponse);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
                return new ProduceResponse(responseMap, throttleTimeMs);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.PRODUCE.latestVersion()));
        }
    }

    private Collection<TopicPartition> partitions() {
        return partitionSizes.keySet();
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public boolean isIdempotent() {
        return idempotent;
    }

    /**
     * Returns the partition records or throws IllegalStateException if clearPartitionRecords() has been invoked.
     */
    public Map<TopicPartition, MemoryRecords> partitionRecordsOrFail() {
        // Store it in a local variable to protect against concurrent updates
        Map<TopicPartition, MemoryRecords> partitionRecords = this.partitionRecords;
        if (partitionRecords == null)
            throw new IllegalStateException("The partition records are no longer available because " +
                    "clearPartitionRecords() has been invoked.");
        return partitionRecords;
    }

    public void clearPartitionRecords() {
        partitionRecords = null;
    }

    public static ProduceRequest parse(ByteBuffer buffer, short version) {
        return new ProduceRequest(ApiKeys.PRODUCE.parseRequest(version, buffer), version);
    }

    public static byte requiredMagicForVersion(short produceRequestVersion) {
        switch (produceRequestVersion) {
            case 0:
            case 1:
                return RecordBatch.MAGIC_VALUE_V0;

            case 2:
                return RecordBatch.MAGIC_VALUE_V1;

            case 3:
            case 4:
                return RecordBatch.MAGIC_VALUE_V2;

            default:
                // raise an exception if the version has not been explicitly added to this method.
                // this ensures that we cannot accidentally use the wrong magic value if we forget
                // to update this method on a bump to the produce request version.
                throw new IllegalArgumentException("Magic value to use for produce request version " +
                        produceRequestVersion + " is not known");
        }
    }
    
}
