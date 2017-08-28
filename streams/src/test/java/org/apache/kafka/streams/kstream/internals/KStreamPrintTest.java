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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KStreamPrintTest {

    private final Serde<Integer> intSerd = Serdes.Integer();
    private final Serde<String> stringSerd = Serdes.String();
    private PrintWriter printWriter;
    private ByteArrayOutputStream byteOutStream;

    private KeyValueMapper<Integer, String, String> mapper;
    private KStreamPrint kStreamPrint;
    private Processor printProcessor;

    @Before
    public void setUp() {
        byteOutStream = new ByteArrayOutputStream();
        printWriter = new PrintWriter(new OutputStreamWriter(byteOutStream, StandardCharsets.UTF_8));

        mapper = new KeyValueMapper<Integer, String, String>() {
            @Override
            public String apply(final Integer key, final String value) {
                return String.format("%d, %s", key, value);
            }
        };

        kStreamPrint = new KStreamPrint<>(new PrintForeachAction<>(printWriter, mapper, "test-stream"), intSerd, stringSerd);

        printProcessor = kStreamPrint.get();
        ProcessorContext processorContext = EasyMock.createNiceMock(ProcessorContext.class);
        EasyMock.replay(processorContext);

        printProcessor.init(processorContext);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPrintStreamWithProvidedKeyValueMapper() {

        final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
                new KeyValue<>(0, "zero"),
                new KeyValue<>(1, "one"),
                new KeyValue<>(2, "two"),
                new KeyValue<>(3, "three"));

        final String[] expectedResult = {"[test-stream]: 0, zero", "[test-stream]: 1, one", "[test-stream]: 2, two", "[test-stream]: 3, three"};

        doTest(inputRecords, expectedResult);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPrintKeyValueStringBytesArray() {

        // we don't have a topic name because we don't need it for the test at this level
        final List<KeyValue<byte[], byte[]>> inputRecords = Arrays.asList(
                new KeyValue<>(intSerd.serializer().serialize(null, 0), stringSerd.serializer().serialize(null, "zero")),
                new KeyValue<>(intSerd.serializer().serialize(null, 1), stringSerd.serializer().serialize(null, "one")),
                new KeyValue<>(intSerd.serializer().serialize(null, 2), stringSerd.serializer().serialize(null, "two")),
                new KeyValue<>(intSerd.serializer().serialize(null, 3), stringSerd.serializer().serialize(null, "three")));

        final String[] expectedResult = {"[test-stream]: 0, zero", "[test-stream]: 1, one", "[test-stream]: 2, two", "[test-stream]: 3, three"};

        doTest(inputRecords, expectedResult);
    }

    private void assertFlushData(final String[] expectedResult, final ByteArrayOutputStream byteOutStream) {

        final String[] flushOutDatas = new String(byteOutStream.toByteArray(), Charset.forName("UTF-8")).split("\\r*\\n");
        for (int i = 0; i < flushOutDatas.length; i++) {
            assertEquals(expectedResult[i], flushOutDatas[i]);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void doTest(final List<KeyValue<K, V>> inputRecords, final String[] expectedResult) {

        for (KeyValue<K, V> record: inputRecords) {
            printProcessor.process(record.key, record.value);
        }
        printWriter.flush();
        assertFlushData(expectedResult, byteOutStream);
    }
}
