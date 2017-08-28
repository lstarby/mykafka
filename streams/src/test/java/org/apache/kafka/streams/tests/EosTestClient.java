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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EosTestClient extends SmokeTestUtil {

    static final String APP_ID = "EosTest";
    private final String kafka;
    private final File stateDir;
    private final boolean withRepartitioning;

    private KafkaStreams streams;
    private boolean uncaughtException;

    EosTestClient(final String kafka, final File stateDir, final boolean withRepartitioning) {
        super();
        this.kafka = kafka;
        this.stateDir = stateDir;
        this.withRepartitioning = withRepartitioning;
    }

    private boolean isRunning = true;

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                isRunning = false;
                streams.close(5, TimeUnit.SECONDS);
                // do not remove these printouts since they are needed for health scripts
                if (!uncaughtException) {
                    System.out.println("EOS-TEST-CLIENT-CLOSED");
                }
            }
        }));

        while (isRunning) {
            if (streams == null) {
                uncaughtException = false;

                streams = createKafkaStreams(stateDir, kafka);
                streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(final Thread t, final Throwable e) {
                        System.out.println("EOS-TEST-CLIENT-EXCEPTION");
                        e.printStackTrace();
                        uncaughtException = true;
                    }
                });
                streams.start();
            }
            if (uncaughtException) {
                streams.close(5, TimeUnit.SECONDS);
                streams = null;
            }
            sleep(1000);
        }
    }

    private KafkaStreams createKafkaStreams(final File stateDir,
                                            final String kafka) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());


        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> data = builder.stream("data");

        data.to("echo");
        data.process(SmokeTestUtil.printProcessorSupplier("data"));

        final KGroupedStream<String, Integer> groupedData = data.groupByKey();
        // min
        groupedData
            .aggregate(
                new Initializer<Integer>() {
                    @Override
                    public Integer apply() {
                        return Integer.MAX_VALUE;
                    }
                },
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(final String aggKey,
                                         final Integer value,
                                         final Integer aggregate) {
                        return (value < aggregate) ? value : aggregate;
                    }
                },
                intSerde,
                "min")
            .to(stringSerde, intSerde, "min");

        // sum
        groupedData.aggregate(
            new Initializer<Long>() {
                @Override
                public Long apply() {
                    return 0L;
                }
            },
            new Aggregator<String, Integer, Long>() {
                @Override
                public Long apply(final String aggKey,
                                  final Integer value,
                                  final Long aggregate) {
                    return (long) value + aggregate;
                }
            },
            longSerde,
            "sum")
            .to(stringSerde, longSerde, "sum");

        if (withRepartitioning) {
            final KStream<String, Integer> repartitionedData = data.through("repartition");

            repartitionedData.process(SmokeTestUtil.printProcessorSupplier("repartition"));

            final KGroupedStream<String, Integer> groupedDataAfterRepartitioning = repartitionedData.groupByKey();
            // max
            groupedDataAfterRepartitioning
                .aggregate(
                    new Initializer<Integer>() {
                        @Override
                        public Integer apply() {
                            return Integer.MIN_VALUE;
                        }
                    },
                    new Aggregator<String, Integer, Integer>() {
                        @Override
                        public Integer apply(final String aggKey,
                                             final Integer value,
                                             final Integer aggregate) {
                            return (value > aggregate) ? value : aggregate;
                        }
                    },
                    intSerde,
                    "max")
                .to(stringSerde, intSerde, "max");

            // count
            groupedDataAfterRepartitioning.count("cnt")
                .to(stringSerde, longSerde, "cnt");
        }

        return new KafkaStreams(builder.build(), props);
    }

}
