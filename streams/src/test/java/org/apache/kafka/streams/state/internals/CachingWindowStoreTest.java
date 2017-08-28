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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


public class CachingWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10000L;
    private MockProcessorContext context;
    private RocksDBSegmentedBytesStore underlying;
    private CachingWindowStore<String, String> cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<String>, String> cacheListener;
    private ThreadCache cache;
    private String topic;
    private WindowKeySchema keySchema;

    @Before
    public void setUp() throws Exception {
        keySchema = new WindowKeySchema();
        final int retention = 30000;
        final int numSegments = 3;
        underlying = new RocksDBSegmentedBytesStore("test", retention, numSegments, keySchema);
        final RocksDBWindowStore<Bytes, byte[]> windowStore = new RocksDBWindowStore<>(underlying, Serdes.Bytes(), Serdes.ByteArray(), false, WINDOW_SIZE);
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>();
        cachingStore = new CachingWindowStore<>(windowStore,
                                                Serdes.String(),
                                                Serdes.String(),
                                                WINDOW_SIZE,
                                                Segments.segmentInterval(retention, numSegments));
        cachingStore.setFlushListener(cacheListener);
        cache = new ThreadCache("testCache", MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        topic = "topic";
        context = new MockProcessorContext(TestUtils.tempDirectory(), null, null, (RecordCollector) null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic));
        cachingStore.init(context, cachingStore);
    }

    @After
    public void closeStore() {
        context.close();
        cachingStore.close();
    }

    @Test
    public void shouldPutFetchFromCache() throws Exception {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        final WindowStoreIterator<byte[]> a = cachingStore.fetch(bytesKey("a"), 10, 10);
        final WindowStoreIterator<byte[]> b = cachingStore.fetch(bytesKey("b"), 10, 10);
        verifyKeyValue(a.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(b.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
        assertEquals(2, cache.size());
    }

    private void verifyKeyValue(final KeyValue<Long, byte[]> next, final long expectedKey, final String expectedValue) {
        assertThat(next.key, equalTo(expectedKey));
        assertThat(next.value, equalTo(bytesValue(expectedValue)));
    }

    private static byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private static Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

    @Test
    public void shouldPutFetchRangeFromCache() throws Exception {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetch(bytesKey("a"), bytesKey("b"), 10, 10);
        verifyWindowedKey(iterator.next(), new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a");
        verifyWindowedKey(iterator.next(), new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "b");
        assertFalse(iterator.hasNext());
        assertEquals(2, cache.size());
    }

    private void verifyWindowedKey(final KeyValue<Windowed<Bytes>, byte[]> actual,
                                   final Windowed<Bytes> expectedKey,
                                   final String expectedValue) {
        assertThat(actual.key.window(), equalTo(expectedKey.window()));
        assertThat(actual.key.key(), equalTo(expectedKey.key()));
        assertThat(actual.value, equalTo(bytesValue(expectedValue)));
    }


    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws Exception {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        final KeyValueIterator<Bytes, byte[]> iter = underlying.fetch(Bytes.wrap("0".getBytes()), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
        assertArrayEquals("0".getBytes(), next.value);
        assertFalse(iter.hasNext());
        assertEquals(added - 1, cache.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() throws Exception {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        assertEquals("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() throws Exception {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("a", cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws Exception {
        int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheListener.forwarded.size());
    }

    @Test
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks() throws Exception {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateAcrossWindows() throws Exception {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStore() throws Exception {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowStoreUtils.toBinaryKey(key, DEFAULT_TIMESTAMP, 0, WindowStoreUtils.getInnerStateSerde("app-id")), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        final WindowStoreIterator<byte[]> fetch = cachingStore.fetch(bytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStoreKeyRange() throws Exception {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowStoreUtils.toBinaryKey(key, DEFAULT_TIMESTAMP, 0, WindowStoreUtils.getInnerStateSerde("app-id")), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
            cachingStore.fetch(key, bytesKey("2"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        verifyWindowedKey(fetchRange.next(), new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a");
        verifyWindowedKey(fetchRange.next(), new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)), "b");
        assertFalse(fetchRange.hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() throws Exception {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        assertEquals(1, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), 0, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), bytesKey("b"), 0, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.put(bytesKey("a"), bytesValue("a"));
    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() throws Exception {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), 60000);

        final List<KeyValue<Long, byte[]>> expected = Utils.mkList(KeyValue.pair(0L, bytesValue("0001")), KeyValue.pair(1L, bytesValue("0003")), KeyValue.pair(60000L, bytesValue("0005")));
        final List<KeyValue<Long, byte[]>> actual = toList(cachingStore.fetch(bytesKey("a"), 0, Long.MAX_VALUE));
        verifyKeyValueList(expected, actual);
    }

    private <K> void verifyKeyValueList(final List<KeyValue<K, byte[]>> expected, final List<KeyValue<K, byte[]>> actual) {
        assertThat(actual.size(), equalTo(expected.size()));
        for (int i = 0; i < actual.size(); i++) {
            final KeyValue<K, byte[]> expectedKv = expected.get(i);
            final KeyValue<K, byte[]> actualKv = actual.get(i);
            assertThat(actualKv.key, equalTo(expectedKv.key));
            assertThat(actualKv.value, equalTo(expectedKv.value));
        }
    }

    @Test
    public void shouldFetchAndIterateOverKeyRange() throws Exception {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), 60000);

        verifyKeyValueList(Utils.mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("a", "0005", 60000L)),
                           toList(cachingStore.fetch(bytesKey("a"), bytesKey("a"), 0, Long.MAX_VALUE)));

        verifyKeyValueList(Utils.mkList(windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1)),
                           toList(cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), 0, Long.MAX_VALUE)));

        verifyKeyValueList(Utils.mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1), windowedPair("a", "0005", 60000L)),
                           toList(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), 0, Long.MAX_VALUE)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() throws Exception {
        cachingStore.put(null, bytesValue("anyValue"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() throws Exception {
        cachingStore.put(bytesKey("a"), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullKey() throws Exception {
        cachingStore.fetch(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() throws Exception {
        cachingStore.fetch(null, bytesKey("anyTo"), 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() throws Exception {
        cachingStore.fetch(bytesKey("anyFrom"), null, 1L, 2L);
    }

    private static KeyValue<Windowed<Bytes>, byte[]> windowedPair(String key, String value, long timestamp) {
        return KeyValue.pair(new Windowed<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)), bytesValue(value));
    }

    private int addItemsToCache() throws IOException {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < MAX_CACHE_SIZE_BYTES) {
            final String kv = String.valueOf(i++);
            cachingStore.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic) +
                8 + // timestamp
                4; // sequenceNumber
        }
        return i;
    }

}
