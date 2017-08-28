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

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KGroupedStreamImplTest {

    private static final String TOPIC = "topic";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), TOPIC);
        groupedStream = stream.groupByKey(Serdes.String(), Serdes.String());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerOnReduce() throws Exception {
        groupedStream.reduce(null, "store");
    }

    @Test
    public void shouldAllowNullStoreNameOnReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, (StateStoreSupplier<KeyValueStore>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnCount() throws Exception {
        groupedStream.count((StateStoreSupplier<KeyValueStore>) null);
    }


    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedCount() throws Exception {
        groupedStream.count(TimeWindows.of(10), (StateStoreSupplier<WindowStore>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerWithWindowedReduce() throws Exception {
        groupedStream.reduce(null, TimeWindows.of(10), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsWithWindowedReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, (Windows) null, "store");
    }

    @Test
    public void shouldAllowNullStoreNameWithWindowedReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() throws Exception {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, Serdes.String(), "store");
    }

    @Test
    public void shouldAllowNullStoreNameOnAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Serdes.String(), null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, TimeWindows.of(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, Serdes.String(), "store");
    }

    @Test
    public void shouldAllowNullStoreNameOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), (StateStoreSupplier<WindowStore>) null);
    }

    private void doAggregateSessionWindows(final Map<Windowed<String>, Integer> results) throws Exception {
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "1");
        driver.setTime(15);
        driver.process(TOPIC, "2", "2");
        driver.setTime(30);
        driver.process(TOPIC, "1", "1");
        driver.setTime(70);
        driver.process(TOPIC, "1", "1");
        driver.setTime(90);
        driver.process(TOPIC, "1", "1");
        driver.setTime(100);
        driver.process(TOPIC, "1", "1");
        driver.flushState();
        assertEquals(Integer.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Integer.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Integer.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldAggregateSessionWindows() throws Exception {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        KTable table = groupedStream.aggregate(new Initializer<Integer>() {
            @Override
            public Integer apply() {
                return 0;
            }
        }, new Aggregator<String, String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final String value, final Integer aggregate) {
                return aggregate + 1;
            }
        }, new Merger<String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final Integer aggOne, final Integer aggTwo) {
                return aggOne + aggTwo;
            }
        }, SessionWindows.with(30), Serdes.Integer(), "session-store");
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldAggregateSessionWindowsWithInternalStoreName() throws Exception {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        KTable table = groupedStream.aggregate(new Initializer<Integer>() {
            @Override
            public Integer apply() {
                return 0;
            }
        }, new Aggregator<String, String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final String value, final Integer aggregate) {
                return aggregate + 1;
            }
        }, new Merger<String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final Integer aggOne, final Integer aggTwo) {
                return aggOne + aggTwo;
            }
        }, SessionWindows.with(30), Serdes.Integer());
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
        assertNull(table.queryableStoreName());
    }

    private void doCountSessionWindows(final Map<Windowed<String>, Long> results) throws Exception {
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "1");
        driver.setTime(15);
        driver.process(TOPIC, "2", "2");
        driver.setTime(30);
        driver.process(TOPIC, "1", "1");
        driver.setTime(70);
        driver.process(TOPIC, "1", "1");
        driver.setTime(90);
        driver.process(TOPIC, "1", "1");
        driver.setTime(100);
        driver.process(TOPIC, "1", "1");
        driver.flushState();
        assertEquals(Long.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Long.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Long.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldCountSessionWindows() throws Exception {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        KTable table = groupedStream.count(SessionWindows.with(30), "session-store");
        table.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
            @Override
            public void apply(final Windowed<String> key, final Long value) {
                results.put(key, value);
            }
        });
        doCountSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldCountSessionWindowsWithInternalStoreName() throws Exception {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        KTable table = groupedStream.count(SessionWindows.with(30));
        table.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
            @Override
            public void apply(final Windowed<String> key, final Long value) {
                results.put(key, value);
            }
        });
        doCountSessionWindows(results);
        assertNull(table.queryableStoreName());
    }

    private void doReduceSessionWindows(final Map<Windowed<String>, String> results) throws Exception {
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "A");
        driver.setTime(15);
        driver.process(TOPIC, "2", "Z");
        driver.setTime(30);
        driver.process(TOPIC, "1", "B");
        driver.setTime(70);
        driver.process(TOPIC, "1", "A");
        driver.setTime(90);
        driver.process(TOPIC, "1", "B");
        driver.setTime(100);
        driver.process(TOPIC, "1", "C");
        driver.flushState();
        assertEquals("A:B", results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals("Z", results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals("A:B:C", results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldReduceSessionWindows() throws Exception {
        final Map<Windowed<String>, String> results = new HashMap<>();
        KTable table = groupedStream.reduce(
                new Reducer<String>() {
                    @Override
                    public String apply(final String value1, final String value2) {
                        return value1 + ":" + value2;
                    }
                }, SessionWindows.with(30),
                "session-store");
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldReduceSessionWindowsWithInternalStoreName() throws Exception {
        final Map<Windowed<String>, String> results = new HashMap<>();
        KTable table = groupedStream.reduce(
                new Reducer<String>() {
                    @Override
                    public String apply(final String value1, final String value2) {
                        return value1 + ":" + value2;
                    }
                }, SessionWindows.with(30));
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertNull(table.queryableStoreName());
    }


    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() throws Exception {
        groupedStream.reduce(null, SessionWindows.with(10), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, (SessionWindows) null, "store");
    }

    @Test
    public void shouldAcceptNullStoreNameWhenReducingSessionWindows() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), (StateStoreSupplier<SessionStore>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), "storeName");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), "storeName");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                null,
                SessionWindows.with(10),
                Serdes.String(),
                "storeName");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, null, Serdes.String(), "storeName");
    }

    @Test
    public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierNameWhenAggregatingSessionWindows() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), (StateStoreSupplier<SessionStore>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenCountingSessionWindows() throws Exception {
        groupedStream.count((SessionWindows) null, "store");
    }

    @Test
    public void shouldAcceptNullStoreNameWhenCountingSessionWindows() throws Exception {
        groupedStream.count(SessionWindows.with(90), (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenCountingSessionWindows() throws Exception {
        groupedStream.count(SessionWindows.with(90), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenCountingSessionWindows() throws Exception {
        groupedStream.count(SessionWindows.with(90), (StateStoreSupplier<SessionStore>) null);
    }

    private void doCountWindowed(final List<KeyValue<Windowed<String>, Long>> results) throws Exception {
        driver.setUp(builder, TestUtils.tempDirectory(), 0);
        driver.setTime(0);
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "2", "B");
        driver.process(TOPIC, "3", "C");
        driver.setTime(500);
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "2", "B");
        driver.process(TOPIC, "2", "B");
        assertThat(results, equalTo(Arrays.asList(
                KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("3", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 2L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 1L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 2L)
        )));
    }

    @Test
    public void shouldCountWindowed() throws Exception {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.count(
                TimeWindows.of(500L),
                "aggregate-by-key-windowed")
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(final Windowed<String> key, final Long value) {
                        results.add(KeyValue.pair(key, value));
                    }
                });

        doCountWindowed(results);
    }

    @Test
    public void shouldCountWindowedWithInternalStoreName() throws Exception {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.count(
                TimeWindows.of(500L))
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(final Windowed<String> key, final Long value) {
                        results.add(KeyValue.pair(key, value));
                    }
                });

        doCountWindowed(results);
    }
}