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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    // TODO: these two fields can be package-private after KStreamBuilder is removed
    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    public static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String FOREACH_NAME = "KTABLE-FOREACH-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final KeyValueMapper<K, V, String> defaultKeyValueMapper;

    private final String queryableStoreName;
    private final boolean isQueryable;

    private boolean sendOldValues = false;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = null;
        this.valSerde = null;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    @Override
    public String queryableStoreName() {
        if (!isQueryable) {
            return null;
        }
        return this.queryableStoreName;
    }

    String internalStoreName() {
        return this.queryableStoreName;
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final StateStoreSupplier<KeyValueStore> storeSupplier,
                                  final boolean isFilterNot) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = builder.newName(FILTER_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, isFilterNot, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
        }
        return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, this.valSerde, sourceNodes, internalStoreName, internalStoreName != null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return filter(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return filterNot(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, true);
    }

    private <V1> KTable<K, V1> doMapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                           final Serde<V1> valueSerde,
                                           final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(mapper);
        String name = builder.newName(MAPVALUES_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapper, internalStoreName);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, name);
            return new KTableImpl<>(builder, name, processorSupplier, this.keySerde, valueSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        return mapValues(mapper, null, (String) null);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                        final Serde<V1> valueSerde,
                                        final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, valueSerde, queryableStoreName);
        }
        return doMapValues(mapper, valueSerde, storeSupplier);
    }

    @Override
    public  <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                         final Serde<V1> valueSerde,
                                         final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doMapValues(mapper, valueSerde, storeSupplier);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print() {
        print(null, null, this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print(final String label) {
        print(null, null, label);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde) {
        print(keySerde, valSerde, this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void print(final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final String label) {
        Objects.requireNonNull(label, "label can't be null");
        final String name = builder.newName(PRINTING_NAME);
        builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(null, defaultKeyValueMapper, label), keySerde, valSerde), this.name);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath) {
        writeAsText(filePath, this.name, null, null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath,
                            final String label) {
        writeAsText(filePath, label, null, null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        writeAsText(filePath, this.name, keySerde, valSerde);
    }

    /**
     * @throws TopologyException if file is not found
     */
    @SuppressWarnings("deprecation")
    @Override
    public void writeAsText(final String filePath,
                            final String label,
                            final Serde<K> keySerde,
                            final Serde<V> valSerde) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(label, "label can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyException("filePath can't be an empty string");
        }
        String name = builder.newName(PRINTING_NAME);
        try {
            PrintWriter printWriter = new PrintWriter(filePath, StandardCharsets.UTF_8.name());
            builder.internalTopologyBuilder.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(printWriter, defaultKeyValueMapper, label), keySerde, valSerde), this.name);
        } catch (final FileNotFoundException | UnsupportedEncodingException e) {
            throw new TopologyException(String.format("Unable to write stream to file at [%s] %s", filePath, e.getMessage()));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = builder.newName(FOREACH_NAME);
        KStreamPeek<K, Change<V>> processorSupplier = new KStreamPeek<>(new ForeachAction<K, Change<V>>() {
            @Override
            public void apply(K key, Change<V> value) {
                action.apply(key, value.newValue);
            }
        }, false);
        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : builder.newStoreName(KTableImpl.TOSTREAM_NAME);

        to(keySerde, valSerde, partitioner, topic);

        return builder.table(null, new FailOnInvalidTimestamp(), keySerde, valSerde, topic, internalStoreName);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        to(keySerde, valSerde, partitioner, topic);

        return builder.table(null, new FailOnInvalidTimestamp(), keySerde, valSerde, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(keySerde, valSerde, partitioner, topic, (String) null);
    }
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final String queryableStoreName) {
        return through(keySerde, valSerde, null, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(keySerde, valSerde, null, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic) {
        return through(keySerde, valSerde, null, topic, (String) null);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        return through(null, null, partitioner, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, partitioner, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(null, null, partitioner, topic, (String) null);
    }

    @Override
    public KTable<K, V> through(final String topic,
                                final String queryableStoreName) {
        return through(null, null, null, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, null, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final String topic) {
        return through(null, null, null, topic, (String) null);
    }

    @Override
    public void to(final String topic) {
        to(null, null, null, topic);
    }

    @Override
    public void to(final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        to(null, null, partitioner, topic);
    }

    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final String topic) {
        this.toStream().to(keySerde, valSerde, null, topic);
    }

    @Override
    public void to(final Serde<K> keySerde,
                   final Serde<V> valSerde,
                   final StreamPartitioner<? super K, ? super V> partitioner,
                   final String topic) {
        this.toStream().to(keySerde, valSerde, partitioner, topic);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = builder.newName(TOSTREAM_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<K, Change<V>, V>(new ValueMapper<Change<V>, V>() {
            @Override
            public V apply(Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, false, false, null, null);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final Serde<R> joinSerde,
                                     final String queryableStoreName) {
        return doJoin(other, joiner, false, false, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, false, false, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, true, true, null, null);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Serde<R> joinSerde,
                                          final String queryableStoreName) {
        return doJoin(other, joiner, true, true, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, true, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, true, false, null, null);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final Serde<R> joinSerde,
                                         final String queryableStoreName) {
        return doJoin(other, joiner, true, false, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joiner, true, false, storeSupplier);
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final Serde<R> joinSerde,
                                        final String queryableStoreName) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final StateStoreSupplier storeSupplier = queryableStoreName == null ? null : keyValueStore(this.keySerde, joinSerde, queryableStoreName);

        return doJoin(other, joiner, leftOuter, rightOuter, storeSupplier);
    }

    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String internalQueryableName = storeSupplier == null ? null : storeSupplier.name();
        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final String joinThisName = builder.newName(JOINTHIS_NAME);
        final String joinOtherName = builder.newName(JOINOTHER_NAME);
        final String joinMergeName = builder.newName(MERGE_NAME);

        final KTableKTableAbstractJoin<K, R, V, V1> joinThis;
        final KTableKTableAbstractJoin<K, R, V1, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        }

        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(builder, joinThisName, joinThis, sourceNodes, this.internalStoreName(), false),
                new KTableImpl<K, V1, R>(builder, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                        ((KTableImpl<K, ?, ?>) other).internalStoreName(), false),
                internalQueryableName
        );

        builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, this.name);
        builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());

        if (internalQueryableName != null) {
            builder.internalTopologyBuilder.addStateStore(storeSupplier, joinMergeName);
        }

        return new KTableImpl<>(builder, joinMergeName, joinMerge, allSourceNodes, internalQueryableName, internalQueryableName != null);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serde<K1> keySerde,
                                                  final Serde<V1> valueSerde) {
        Objects.requireNonNull(selector, "selector can't be null");
        String selectName = builder.newName(SELECT_NAME);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        builder.internalTopologyBuilder.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();

        return new KGroupedTableImpl<>(builder, selectName, this.name, keySerde, valueSerde);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return this.groupBy(selector, null, null);
    }

    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

}
