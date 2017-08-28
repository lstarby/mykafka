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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.MockValueJoiner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.Topology.AutoOffsetReset;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class InternalStreamsBuilderTest {

    private static final String APP_ID = "app-id";

    private final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());

    private KStreamTestDriver driver = null;

    @Before
    public void setUp() {
        builder.internalTopologyBuilder.setApplicationId(APP_ID);
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testNewName() {
        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-0000000000", newBuilder.newName("X-"));
        assertEquals("Y-0000000001", newBuilder.newName("Y-"));
        assertEquals("Z-0000000002", newBuilder.newName("Z-"));
    }

    @Test
    public void testNewStoreName() {
        assertEquals("X-STATE-STORE-0000000000", builder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", builder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", builder.newStoreName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-STATE-STORE-0000000000", newBuilder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", newBuilder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", newBuilder.newStoreName("Z-"));
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream() throws Exception {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final KStream<String, String> source1 = builder.stream(null, null, null, null, topic1);
        final KStream<String, String> source2 = builder.stream(null, null, null, null, topic2);
        final KStream<String, String> source3 = builder.stream(null, null, null, null, topic3);
        final KStream<String, String> processedSource1 =
                source1.mapValues(new ValueMapper<String, String>() {
                    @Override
                    public String apply(final String value) {
                        return value;
                    }
                }).filter(new Predicate<String, String>() {
                    @Override
                    public boolean test(final String key, final String value) {
                        return true;
                    }
                });
        final KStream<String, String> processedSource2 = source2.filter(new Predicate<String, String>() {
            @Override
            public boolean test(final String key, final String value) {
                return true;
            }
        });

        final KStream<String, String> merged = builder.merge(processedSource1, processedSource2, source3);
        merged.groupByKey().count("my-table");
        final Map<String, List<String>> actual = builder.internalTopologyBuilder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test
    public void shouldStillMaterializeSourceKTableIfStateNameNotSpecified() throws Exception {
        KTable table1 = builder.table(null, null, null, null, "topic1", "table1");
        KTable table2 = builder.table(null, null, null, null, "topic2", (String) null);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build(null);

        assertEquals(2, topology.stateStores().size());
        assertEquals("table1", topology.stateStores().get(0).name());

        final String internalStoreName = topology.stateStores().get(1).name();
        assertTrue(internalStoreName.contains(KTableImpl.STATE_STORE_NAME));
        assertEquals(2, topology.storeToChangelogTopic().size());
        assertEquals("topic1", topology.storeToChangelogTopic().get("table1"));
        assertEquals("topic2", topology.storeToChangelogTopic().get(internalStoreName));
        assertEquals(table1.queryableStoreName(), "table1");
        assertNull(table2.queryableStoreName());
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() throws Exception {
        builder.globalTable(null, null, null, "table", "globalTable");

        final ProcessorTopology topology = builder.internalTopologyBuilder.buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();

        assertEquals(1, stateStores.size());
        assertEquals("globalTable", stateStores.get(0).name());
    }

    private void doBuildGlobalTopologyWithAllGlobalTables() throws Exception {
        final ProcessorTopology topology = builder.internalTopologyBuilder.buildGlobalStateTopology();

        final List<StateStore> stateStores = topology.globalStateStores();
        final Set<String> sourceTopics = topology.sourceTopics();

        assertEquals(Utils.mkSet("table", "table2"), sourceTopics);
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() throws Exception {
        builder.globalTable(null, null, null, "table", "globalTable");
        builder.globalTable(null, null, null, "table2", "globalTable2");

        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTablesWithInternalStoreName() throws Exception {
        builder.globalTable(null, null, null, "table", null);
        builder.globalTable(null, null, null, "table2", null);

        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() throws Exception {
        final String one = "globalTable";
        final String two = "globalTable2";
        final GlobalKTable<String, String> globalTable = builder.globalTable(null, null, null, "table", one);
        final GlobalKTable<String, String> globalTable2 = builder.globalTable(null, null, null, "table2", two);

        builder.table(null, null, null, null, "not-global", "not-global");

        final KeyValueMapper<String, String, String> kvMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };

        final KStream<String, String> stream = builder.stream(null, null, null, null, "t1");
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        final KStream<String, String> stream2 = builder.stream(null, null, null, null, "t2");
        stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER);

        final Map<Integer, Set<String>> nodeGroups = builder.internalTopologyBuilder.nodeGroups();
        for (Integer groupId : nodeGroups.keySet()) {
            final ProcessorTopology topology = builder.internalTopologyBuilder.build(groupId);
            final List<StateStore> stateStores = topology.globalStateStores();
            final Set<String> names = new HashSet<>();
            for (StateStore stateStore : stateStores) {
                names.add(stateStore.name());
            }

            assertEquals(2, stateStores.size());
            assertTrue(names.contains(one));
            assertTrue(names.contains(two));
        }
    }

    @Test
    public void shouldMapStateStoresToCorrectSourceTopics() throws Exception {
        final KStream<String, String> playEvents = builder.stream(null, null, null, null, "events");

        final KTable<String, String> table = builder.table(null, null, null, null, "table-topic", "table-store");
        assertEquals(Collections.singletonList("table-topic"), builder.internalTopologyBuilder.stateStoreNameToSourceTopics().get("table-store"));

        final KStream<String, String> mapped = playEvents.map(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper());
        mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count("count");
        assertEquals(Collections.singletonList("table-topic"), builder.internalTopologyBuilder.stateStoreNameToSourceTopics().get("table-store"));
        assertEquals(Collections.singletonList(APP_ID + "-KSTREAM-MAP-0000000003-repartition"), builder.internalTopologyBuilder.stateStoreNameToSourceTopics().get("count"));
    }

    @Test
    public void shouldAddTopicToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        
        builder.stream(AutoOffsetReset.EARLIEST, null, null, null, topicName);

        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTopicToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";

        builder.stream(AutoOffsetReset.LATEST, null, null, null, topicName);

        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final String storeName = "test-store";

        builder.table(AutoOffsetReset.EARLIEST, null, null, null, topicName, storeName);

        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final String storeName = "test-store";

        builder.table(AutoOffsetReset.LATEST, null, null, null, topicName, storeName);

        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddTableToOffsetResetLists() {
        final String topicName = "topic-1";
        final String storeName = "test-store";
        final Serde<String> stringSerde = Serdes.String();

        builder.table(null, null, stringSerde, stringSerde, topicName, storeName);

        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddRegexTopicsToOffsetResetLists() {
        final Pattern topicPattern = Pattern.compile("topic-\\d");
        final String topic = "topic-5";

        builder.stream(null, null, null, null, topicPattern);

        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topic).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topic).matches());

    }

    @Test
    public void shouldAddRegexTopicToEarliestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-500000";

        builder.stream(AutoOffsetReset.EARLIEST, null, null, null,  topicPattern);

        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void shouldAddRegexTopicToLatestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-1000000";

        builder.stream(AutoOffsetReset.LATEST, null, null, null, topicPattern);

        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void shouldHaveNullTimestampExtractorWhenNoneSupplied() throws Exception {
        builder.stream(null, null, null, null, "topic");
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void shouldUseProvidedTimestampExtractor() throws Exception {
        builder.stream(null, new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void ktableShouldHaveNullTimestampExtractorWhenNoneSupplied() throws Exception {
        builder.table(null, null, null, null, "topic", "store");
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void ktableShouldUseProvidedTimestampExtractor() throws Exception {
        builder.table(null, new MockTimestampExtractor(), null, null, "topic", "store");
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    // TODO: this static functions are added because some non-TopologyBuilder unit tests need to access the internal topology builder,
    //       which is usually a bad sign of design patterns between TopologyBuilder and StreamThread. We need to consider getting rid of them later
    public static InternalTopologyBuilder internalTopologyBuilder(final InternalStreamsBuilder internalStreamsBuilder) {
        return internalStreamsBuilder.internalTopologyBuilder;
    }
}