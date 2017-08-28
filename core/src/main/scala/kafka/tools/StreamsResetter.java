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
package kafka.tools;


import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

import kafka.admin.AdminClient;
import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * {@link StreamsResetter} resets the processing state of a Kafka Streams application so that, for example, you can reprocess its input from scratch.
 * <p>
 * <strong>This class is not part of public API. For backward compatibility, use the provided script in "bin/" instead of calling this class directly from your code.</strong>
 * <p>
 * Resetting the processing state of an application includes the following actions:
 * <ol>
 * <li>setting the application's consumer offsets for input and internal topics to zero</li>
 * <li>skip over all intermediate user topics (i.e., "seekToEnd" for consumers of intermediate topics)</li>
 * <li>deleting any topics created internally by Kafka Streams for this application</li>
 * </ol>
 * <p>
 * Do only use this tool if <strong>no</strong> application instance is running. Otherwise, the application will get into an invalid state and crash or produce wrong results.
 * <p>
 * If you run multiple application instances, running this tool once is sufficient.
 * However, you need to call {@code KafkaStreams#cleanUp()} before re-starting any instance (to clean local state store directory).
 * Otherwise, your application is in an invalid state.
 * <p>
 * User output topics will not be deleted or modified by this tool.
 * If downstream applications consume intermediate or output topics, it is the user's responsibility to adjust those applications manually if required.
 */
@InterfaceStability.Unstable
public class StreamsResetter {
    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_ERROR = 1;

    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> zookeeperOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> inputTopicsOption;
    private static OptionSpec<String> intermediateTopicsOption;
    private static OptionSpecBuilder dryRunOption;

    private OptionSet options = null;
    private final Properties consumerConfig = new Properties();
    private final List<String> allTopics = new LinkedList<>();
    private boolean dryRun = false;

    public int run(final String[] args) {
        return run(args, new Properties());
    }

    public int run(final String[] args, final Properties config) {
        consumerConfig.clear();
        consumerConfig.putAll(config);

        int exitCode = EXIT_CODE_SUCCESS;

        AdminClient adminClient = null;
        ZkUtils zkUtils = null;
        try {
            parseArguments(args);
            dryRun = options.has(dryRunOption);

            adminClient = AdminClient.createSimplePlaintext(options.valueOf(bootstrapServerOption));
            final String groupId = options.valueOf(applicationIdOption);


            zkUtils = ZkUtils.apply(options.valueOf(zookeeperOption),
                30000,
                30000,
                JaasUtils.isZkSecurityEnabled());

            allTopics.clear();
            allTopics.addAll(scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics()));


            if (!adminClient.describeConsumerGroup(groupId, 0).consumers().get().isEmpty()) {
                throw new IllegalStateException("Consumer group '" + groupId + "' is still active. " +
                            "Make sure to stop all running application instances before running the reset tool.");
            }

            if (dryRun) {
                System.out.println("----Dry run displays the actions which will be performed when running Streams Reset Tool----");
            }
            maybeResetInputAndSeekToEndIntermediateTopicOffsets();
            maybeDeleteInternalTopics(zkUtils);

        } catch (final Throwable e) {
            exitCode = EXIT_CODE_ERROR;
            System.err.println("ERROR: " + e.getMessage());
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
            if (zkUtils != null) {
                zkUtils.close();
            }
        }

        return exitCode;
    }

    private void parseArguments(final String[] args) throws IOException {

        final OptionParser optionParser = new OptionParser(false);
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id).")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-servers", "Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("urls");
        zookeeperOption = optionParser.accepts("zookeeper", "Zookeeper url with format: HOST:POST")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:2181")
            .describedAs("url");
        inputTopicsOption = optionParser.accepts("input-topics", "Comma-separated list of user input topics. For these topics, the tool will reset the offset to the earliest available offset.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma-separated list of intermediate user topics (topics used in the through() method). For these topics, the tool will skip to the end.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        dryRunOption = optionParser.accepts("dry-run", "Display the actions that would be performed without executing the reset commands.");

        try {
            options = optionParser.parse(args);
        } catch (final OptionException e) {
            printHelp(optionParser);
            throw e;
        }
    }

    private void maybeResetInputAndSeekToEndIntermediateTopicOffsets() {
        final List<String> inputTopics = options.valuesOf(inputTopicsOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);


        final List<String> notFoundInputTopics = new ArrayList<>();
        final List<String> notFoundIntermediateTopics = new ArrayList<>();

        String groupId = options.valueOf(applicationIdOption);

        if (inputTopics.size() == 0 && intermediateTopics.size() == 0) {
            System.out.println("No input or intermediate topics specified. Skipping seek.");
            return;
        }

        if (inputTopics.size() != 0) {
            System.out.println("Seek-to-beginning for input topics " + inputTopics);
        }
        if (intermediateTopics.size() != 0) {
            System.out.println("Seek-to-end for intermediate topics " + intermediateTopics);
        }

        final Set<String> topicsToSubscribe = new HashSet<>(inputTopics.size() + intermediateTopics.size());

        for (final String topic : inputTopics) {
            if (!allTopics.contains(topic)) {
                notFoundInputTopics.add(topic);
            } else {
                topicsToSubscribe.add(topic);
            }
        }
        for (final String topic : intermediateTopics) {
            if (!allTopics.contains(topic)) {
                notFoundIntermediateTopics.add(topic);
            } else {
                topicsToSubscribe.add(topic);
            }
        }

        final Properties config = new Properties();
        config.putAll(consumerConfig);
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            client.subscribe(topicsToSubscribe);
            client.poll(1);

            final Set<TopicPartition> partitions = client.assignment();
            final Set<TopicPartition> inputTopicPartitions = new HashSet<>();
            final Set<TopicPartition> intermediateTopicPartitions = new HashSet<>();

            for (final TopicPartition p : partitions) {
                final String topic = p.topic();
                if (isInputTopic(topic)) {
                    inputTopicPartitions.add(p);
                } else if (isIntermediateTopic(topic)) {
                    intermediateTopicPartitions.add(p);
                } else {
                    System.err.println("Skipping invalid partition: " + p);
                }
            }

            maybeSeekToBeginning(client, inputTopicPartitions);

            maybeSeekToEnd(client, intermediateTopicPartitions);

            if (!dryRun) {
                for (final TopicPartition p : partitions) {
                    client.position(p);
                }
                client.commitSync();
            }

            if (notFoundInputTopics.size() > 0) {
                System.out.println("Following input topics are not found, skipping them");
                for (final String topic : notFoundInputTopics) {
                    System.out.println("Topic: " + topic);
                }
            }

            if (notFoundIntermediateTopics.size() > 0) {
                System.out.println("Following intermediate topics are not found, skipping them");
                for (final String topic : notFoundIntermediateTopics) {
                    System.out.println("Topic:" + topic);
                }
            }

        } catch (final RuntimeException e) {
            System.err.println("ERROR: Resetting offsets failed.");
            throw e;
        }
        System.out.println("Done.");
    }

    private void maybeSeekToEnd(final KafkaConsumer<byte[], byte[]> client, final Set<TopicPartition> intermediateTopicPartitions) {

        final String groupId = options.valueOf(applicationIdOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);

        if (intermediateTopicPartitions.size() > 0) {
            System.out.println("Following intermediate topics offsets will be reset to end (for consumer group " + groupId + ")");
            for (final String topic : intermediateTopics) {
                if (allTopics.contains(topic)) {
                    System.out.println("Topic: " + topic);
                }
            }
            if (!dryRun) {
                client.seekToEnd(intermediateTopicPartitions);
            }
        }
    }

    private void maybeSeekToBeginning(final KafkaConsumer<byte[], byte[]> client,
                                      final Set<TopicPartition> inputTopicPartitions) {

        final List<String> inputTopics = options.valuesOf(inputTopicsOption);
        final String groupId = options.valueOf(applicationIdOption);

        if (inputTopicPartitions.size() > 0) {
            System.out.println("Following input topics offsets will be reset to beginning (for consumer group " + groupId + ")");
            for (final String topic : inputTopics) {
                if (allTopics.contains(topic)) {
                    System.out.println("Topic: " + topic);
                }
            }
            if (!dryRun) {
                client.seekToBeginning(inputTopicPartitions);
            }
        }
    }

    private boolean isInputTopic(final String topic) {
        return options.valuesOf(inputTopicsOption).contains(topic);
    }

    private boolean isIntermediateTopic(final String topic) {
        return options.valuesOf(intermediateTopicsOption).contains(topic);
    }

    private void maybeDeleteInternalTopics(final ZkUtils zkUtils) {

        System.out.println("Deleting all internal/auto-created topics for application " + options.valueOf(applicationIdOption));

        for (final String topic : allTopics) {
            if (isInternalTopic(topic)) {
                try {
                    if (!dryRun) {
                        final TopicCommand.TopicCommandOptions commandOptions = new TopicCommand.TopicCommandOptions(new String[]{
                            "--zookeeper", options.valueOf(zookeeperOption),
                            "--delete", "--topic", topic});
                        TopicCommand.deleteTopic(zkUtils, commandOptions);
                    } else {
                        System.out.println("Topic: " + topic);
                    }
                } catch (final RuntimeException e) {
                    System.err.println("ERROR: Deleting topic " + topic + " failed.");
                    throw e;
                }
            }
        }
        System.out.println("Done.");
    }

    private boolean isInternalTopic(final String topicName) {
        return topicName.startsWith(options.valueOf(applicationIdOption) + "-")
            && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
    }

    private void printHelp(OptionParser parser) throws IOException {
        System.err.println("The Streams Reset Tool allows you to quickly reset an application in order to reprocess "
                + "its data from scratch.\n"
                + "* This tool resets offsets of input topics to the earliest available offset and it skips to the end of "
                + "intermediate topics (topics used in the through() method).\n"
                + "* This tool deletes the internal topics that were created by Kafka Streams (topics starting with "
                + "\"<application.id>-\").\n"
                + "You do not need to specify internal topics because the tool finds them automatically.\n"
                + "* This tool will not delete output topics (if you want to delete them, you need to do it yourself "
                + "with the bin/kafka-topics.sh command).\n"
                + "* This tool will not clean up the local state on the stream application instances (the persisted "
                + "stores used to cache aggregation results).\n"
                + "You need to call KafkaStreams#cleanUp() in your application or manually delete them from the "
                + "directory specified by \"state.dir\" configuration (/tmp/kafka-streams/<application.id> by default).\n\n"
                + "*** Important! You will get wrong output if you don't clean up the local stores after running the "
                + "reset tool!\n\n"
        );
        parser.printHelpOn(System.err);
    }

    public static void main(final String[] args) {
        Exit.exit(new StreamsResetter().run(args));
    }

}
