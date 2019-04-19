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
/*
 * derived from org/apache/kafka/tools/VerifiableConsumer.java
 */

package qlikglue.source.kafka;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * Command line consumer designed for system testing. It outputs consumer events to STDOUT as JSON
 * formatted objects. The "name" field in each JSON event identifies the event type. The following
 * events are currently supported:
 *
 * <ul>
 * <li>partitions_revoked: outputs the partitions revoked through {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}.
 *     See {@link QlikGlueKafkaConsumer.PartitionsRevoked}</li>
 * <li>partitions_assigned: outputs the partitions assigned through {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
 *     See {@link QlikGlueKafkaConsumer.PartitionsAssigned}.</li>
 * <li>records_consumed: contains a summary of records consumed in a single call to {@link KafkaConsumer#poll(Duration)}.
 *     See {@link QlikGlueKafkaConsumer.RecordsConsumed}.</li>
 * <li>record_data: contains the key, value, and offset of an individual consumed record
 *     See {@link QlikGlueKafkaConsumer.RecordData}.</li>
 * <li>offsets_committed: The result of every offset commit (only included if auto-commit is not enabled).
 *     See {@link QlikGlueKafkaConsumer.OffsetsCommitted}</li>
 * <li>shutdown_complete: emitted after the consumer returns from {@link KafkaConsumer#close()}.
 *     See {@link QlikGlueKafkaConsumer.ShutdownComplete}.</li>
 * </ul>
 */
public class QlikGlueKafkaConsumer implements Closeable, OffsetCommitCallback, ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(QlikGlueKafkaConsumer.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final boolean useAutoCommit;
    private final boolean useAsyncCommit;
    private final int maxMessages;
    private int consumedMessages = 0;

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private KafkaJsonHandler recordHandler;


    private QlikGlueKafkaConsumer(KafkaConsumer<String, String> consumer,
                              String topic,
                              int maxMessages,
                              boolean useAutoCommit,
                              boolean useAsyncCommit) {
        this.consumer = consumer;
        this.topic = topic;
        this.maxMessages = maxMessages;
        this.useAutoCommit = useAutoCommit;
        this.useAsyncCommit = useAsyncCommit;
        addTopicSerializerModule();
        init();
    }

    private void init() {
        LOG.info("init(): initializing environment");

        // ... do any additional config that may be needed ...

        recordHandler = new KafkaJsonHandler();

        LOG.debug("init(): returning");
    }

    private void addTopicSerializerModule() {
        SimpleModule kafka = new SimpleModule();
        kafka.addSerializer(TopicPartition.class, new JsonSerializer<TopicPartition>() {
            @Override
            public void serialize(TopicPartition tp, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeStartObject();
                gen.writeObjectField("topic", tp.topic());
                gen.writeObjectField("partition", tp.partition());
                gen.writeEndObject();
            }
        });
        mapper.registerModule(kafka);
    }

    private boolean hasMessageLimit() {
        return maxMessages >= 0;
    }

    private boolean isFinished() {
        return hasMessageLimit() && consumedMessages >= maxMessages;
    }

    private Map<TopicPartition, OffsetAndMetadata> onRecordsReceived(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        List<RecordSetSummary> summaries = new ArrayList<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

            if (hasMessageLimit() && consumedMessages + partitionRecords.size() > maxMessages)
                partitionRecords = partitionRecords.subList(0, maxMessages - consumedMessages);

            if (partitionRecords.isEmpty())
                continue;

            long minOffset = partitionRecords.get(0).offset();
            long maxOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

            offsets.put(tp, new OffsetAndMetadata(maxOffset + 1));
            summaries.add(new RecordSetSummary(tp.topic(), tp.partition(),
                    partitionRecords.size(), minOffset, maxOffset));

                for (ConsumerRecord<String, String> record : partitionRecords) {
                    if (LOG.isTraceEnabled())
                        LOG.trace(logJson(new RecordData(record)));
                    recordHandler.newRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

            consumedMessages += partitionRecords.size();
            if (isFinished())
                break;
        }

        LOG.info(logJson(new RecordsConsumed(records.count(), summaries)));
        return offsets;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        List<CommitData> committedOffsets = new ArrayList<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : offsets.entrySet()) {
            TopicPartition tp = offsetEntry.getKey();
            committedOffsets.add(new CommitData(tp.topic(), tp.partition(), offsetEntry.getValue().offset()));
        }

        boolean success = true;
        String error = null;
        if (exception != null) {
            success = false;
            error = exception.getMessage();
        }
        LOG.info(logJson(new OffsetsCommitted(committedOffsets, error, success)));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info(logJson(new PartitionsAssigned(partitions)));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info(logJson(new PartitionsRevoked(partitions)));
    }

    private String logJson(Object data) {
        String rval;
        try {
            rval = mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOG.error("Bad data can't be written as json:", e.getMessage());
            rval = "Json format error";
        }
        return rval;
    }

    private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            consumer.commitSync(offsets);
            onComplete(offsets, null);
        } catch (WakeupException e) {
            // we only call wakeup() once to close the consumer, so this recursion should be safe
            commitSync(offsets);
            throw e;
        } catch (Exception e) {
            onComplete(offsets, e);
        }
    }

    public void run() {
        try {
            LOG.info(logJson(new StartupComplete()));
            // supports a single hard coded topic
            //consumer.subscribe(Collections.singletonList(topic), this);
            //
            // supports a single regex to get matching topics
            Pattern regex = Pattern.compile(topic);
            consumer.subscribe(regex, this);

            while (!isFinished()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                Map<TopicPartition, OffsetAndMetadata> offsets = onRecordsReceived(records);

                if (!useAutoCommit) {
                    if (useAsyncCommit) {
                        consumer.commitAsync(offsets, this);
                    } else {
                        commitSync(offsets);
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore, we are closing
            if (LOG.isTraceEnabled())
                LOG.trace("Caught WakeupException because consumer is shutdown, ignore and terminate.", e);
        } catch (Throwable t) {
            LOG.error("Error during processing, terminating consumer process: ", t);
        } finally {
            consumer.close();
            LOG.info(logJson(new ShutdownComplete()));
            shutdownLatch.countDown();
        }
    }

    public void close() {
        boolean interrupted = false;
        try {
            consumer.wakeup();
            while (true) {
                try {
                    recordHandler.cleanup();
                    shutdownLatch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @JsonPropertyOrder({ "timestamp", "name" })
    private static abstract class ConsumerEvent {
        private final long timestamp = System.currentTimeMillis();

        @JsonProperty
        public abstract String name();

        @JsonProperty
        public long timestamp() {
            return timestamp;
        }
    }

    private static class StartupComplete extends ConsumerEvent {

        @Override
        public String name() {
            return "startup_complete";
        }
    }

    private static class ShutdownComplete extends ConsumerEvent {

        @Override
        public String name() {
            return "shutdown_complete";
        }
    }

    private static class PartitionsRevoked extends ConsumerEvent {
        private final Collection<TopicPartition> partitions;

        PartitionsRevoked(Collection<TopicPartition> partitions) {
            this.partitions = partitions;
        }

        @JsonProperty
        public Collection<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String name() {
            return "partitions_revoked";
        }
    }

    private static class PartitionsAssigned extends ConsumerEvent {
        private final Collection<TopicPartition> partitions;

        PartitionsAssigned(Collection<TopicPartition> partitions) {
            this.partitions = partitions;
        }

        @JsonProperty
        public Collection<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String name() {
            return "partitions_assigned";
        }
    }

    public static class RecordsConsumed extends ConsumerEvent {
        private final long count;
        private final List<RecordSetSummary> partitionSummaries;

        RecordsConsumed(long count, List<RecordSetSummary> partitionSummaries) {
            this.count = count;
            this.partitionSummaries = partitionSummaries;
        }

        @Override
        public String name() {
            return "records_consumed";
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public List<RecordSetSummary> partitions() {
            return partitionSummaries;
        }
    }

    @JsonPropertyOrder({ "timestamp", "name", "key", "value", "topic", "partition", "offset" })
    public static class RecordData extends ConsumerEvent {

        private final ConsumerRecord<String, String> record;

        RecordData(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public String name() {
            return "record_data";
        }

        @JsonProperty
        public String topic() {
            return record.topic();
        }

        @JsonProperty
        public int partition() {
            return record.partition();
        }

        @JsonProperty
        public String key() {
            return record.key();
        }

        @JsonProperty
        public String value() {
            return record.value();
        }

        @JsonProperty
        public long offset() {
            return record.offset();
        }

    }

    private static class PartitionData {
        private final String topic;
        private final int partition;

        PartitionData(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @JsonProperty
        public String topic() {
            return topic;
        }

        @JsonProperty
        public int partition() {
            return partition;
        }
    }

    private static class OffsetsCommitted extends ConsumerEvent {

        private final List<CommitData> offsets;
        private final String error;
        private final boolean success;

        public OffsetsCommitted(List<CommitData> offsets, String error, boolean success) {
            this.offsets = offsets;
            this.error = error;
            this.success = success;
        }

        @Override
        public String name() {
            return "offsets_committed";
        }

        @JsonProperty
        public List<CommitData> offsets() {
            return offsets;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String error() {
            return error;
        }

        @JsonProperty
        public boolean success() {
            return success;
        }

    }

    private static class CommitData extends PartitionData {
        private final long offset;

        public CommitData(String topic, int partition, long offset) {
            super(topic, partition);
            this.offset = offset;
        }

        @JsonProperty
        public long offset() {
            return offset;
        }
    }

    private static class RecordSetSummary extends PartitionData {
        private final long count;
        private final long minOffset;
        private final long maxOffset;

        public RecordSetSummary(String topic, int partition, long count, long minOffset, long maxOffset) {
            super(topic, partition);
            this.count = count;
            this.minOffset = minOffset;
            this.maxOffset = maxOffset;
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public long minOffset() {
            return minOffset;
        }

        @JsonProperty
        public long maxOffset() {
            return maxOffset;
        }

    }

    public static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("QlikGlueKafkaConsumer")
                .build()
                .defaultHelp(true)
                .description("This tool consumes messages from a specific topic and sends messages downstream");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Consumes messages from this topic. May be a regex.");

        parser.addArgument("--group-id")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("GROUP_ID")
                .dest("groupId")
                .help("The groupId shared among members of the consumer group");

        parser.addArgument("--max-messages")
                .action(store())
                .required(false)
                .type(Integer.class)
                .setDefault(-1)
                .metavar("MAX-MESSAGES")
                .dest("maxMessages")
                .help("Consume this many messages. If -1 (the default), the consumer will consume until the process is killed externally");

        parser.addArgument("--session-timeout")
                .action(store())
                .required(false)
                .setDefault(30000)
                .type(Integer.class)
                .metavar("TIMEOUT_MS")
                .dest("sessionTimeout")
                .help("Set the consumer's session timeout");

        parser.addArgument("--enable-autocommit")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("ENABLE-AUTOCOMMIT")
                .dest("useAutoCommit")
                .help("Enable offset auto-commit on consumer");

        parser.addArgument("--reset-policy")
                .action(store())
                .required(false)
                .setDefault("earliest")
                .type(String.class)
                .dest("resetPolicy")
                .help("Set reset policy (must be either 'earliest', 'latest', or 'none'");

        parser.addArgument("--assignment-strategy")
                .action(store())
                .required(false)
                .setDefault(RangeAssignor.class.getName())
                .type(String.class)
                .dest("assignmentStrategy")
                .help("Set assignment strategy (e.g. " + RoundRobinAssignor.class.getName() + ")");

        parser.addArgument("--consumer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Consumer config properties file (config options shared with command line parameters will be overridden).");

        return parser;
    }

    public static QlikGlueKafkaConsumer createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        String topic = res.getString("topic");
        boolean useAutoCommit = res.getBoolean("useAutoCommit");
        int maxMessages = res.getInt("maxMessages");
        String configFile = res.getString("consumer.config");

        Properties consumerProps = new Properties();
        if (configFile != null) {
            try {
                consumerProps.putAll(Utils.loadProps(configFile));
            } catch (IOException e) {
                throw new ArgumentParserException(e.getMessage(), parser);
            }
        }

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, res.getString("groupId"));
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, useAutoCommit);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, res.getString("resetPolicy"));
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(res.getInt("sessionTimeout")));
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, res.getString("assignmentStrategy"));

        StringDeserializer deserializer = new StringDeserializer();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps, deserializer, deserializer);

        return new QlikGlueKafkaConsumer(
                consumer,
                topic,
                maxMessages,
                useAutoCommit,
                false);
    }


}
