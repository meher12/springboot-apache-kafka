package com.course.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.course.kafka.broker.message.LocationMessage;

@Configuration
public class OperationDemoStreamConfig {

    public static final String SOURCE_TOPIC_BRANCH = "t-demo-stream-branch-source";
    public static final String SINK_TOPIC_BRANCH_GT_100 = "t-demo-stream-branch-sink-gt-100";
    public static final String SINK_TOPIC_BRANCH_GT_20 = "t-demo-stream-branch-sink-gt-20";
    public static final String SINK_TOPIC_BRANCH_GT_10 = "t-demo-stream-branch-sink-gt-10";

    public static final String SOURCE_TOPIC_COGROUP_WEATHER = "t-demo-stream-cogroup-source-weather";
    public static final String SOURCE_TOPIC_COGROUP_TRAFFIC = "t-demo-stream-cogroup-source-traffic";
    public static final String SINK_TOPIC_COGROUP = "t-demo-stream-cogroup-sink";

    public static final String SOURCE_TOPIC_FILTER = "t-demo-stream-filter-source";
    public static final String SINK_TOPIC_FILTER = "t-demo-stream-filter-sink";

    public static final String SOURCE_TOPIC_FILTER_NOT = "t-demo-stream-filter-not-source";
    public static final String SINK_TOPIC_FILTER_NOT = "t-demo-stream-filter-not-sink";

    public static final String SOURCE_TOPIC_FLAT_MAP = "t-demo-stream-flat-map-source";
    public static final String SINK_TOPIC_FLAT_MAP = "t-demo-stream-flat-map-sink";

    public static final String SOURCE_TOPIC_FLAT_MAP_VALUES = "t-demo-stream-flat-map-values-source";
    public static final String SINK_TOPIC_FLAT_MAP_VALUES = "t-demo-stream-flat-map-values-sink";

    public static final String SOURCE_TOPIC_FOR_EACH = "t-demo-stream-for-each-source";

    public static final String SOURCE_TOPIC_GROUP_BY = "t-demo-stream-group-by-source";
    public static final String SINK_TOPIC_GROUP_BY = "t-demo-stream-group-by-sink";

    public static final String SOURCE_TOPIC_GROUP_BY_KEY = "t-demo-stream-group-by-key-source";
    public static final String SINK_TOPIC_GROUP_BY_KEY = "t-demo-stream-group-by-key-sink";

    public static final String SOURCE_TOPIC_MAP = "t-demo-stream-map-source";
    public static final String SINK_TOPIC_MAP = "t-demo-stream-map-sink";

    public static final String SOURCE_TOPIC_MAP_VALUES = "t-demo-stream-map-values-source";
    public static final String SINK_TOPIC_MAP_VALUES = "t-demo-stream-map-values-sink";

    public static final String SOURCE_TOPIC_MERGE_ALPHABET = "t-demo-stream-merge-source-alphabet";
    public static final String SOURCE_TOPIC_MERGE_NAME = "t-demo-stream-merge-source-name";
    public static final String SINK_TOPIC_MERGE = "t-demo-stream-merge-sink";

    public static final String SOURCE_TOPIC_PEEK = "t-demo-stream-peek-source";
    public static final String SINK_TOPIC_PEEK = "t-demo-stream-peek-sink";

    public static final String SOURCE_TOPIC_REPARTITION = "t-demo-stream-repartition-source";
    public static final String SINK_TOPIC_REPARTITION_ONE = "t-demo-stream-repartition-sink-one";
    public static final String SINK_TOPIC_REPARTITION_TWO = "t-demo-stream-repartition-sink-two";

    public static final String SOURCE_TOPIC_SELECT_KEY = "t-demo-stream-select-key-source";
    public static final String SINK_TOPIC_SELECT_KEY = "t-demo-stream-select-key-sink";

    public static final String SOURCE_TOPIC_SPLIT = "t-demo-stream-split-source";
    public static final String SINK_TOPIC_SPLIT_GT_100 = "t-demo-stream-split-sink-gt-100";
    public static final String SINK_TOPIC_SPLIT_GT_20 = "t-demo-stream-split-sink-gt-20";
    public static final String SINK_TOPIC_SPLIT_GT_10 = "t-demo-stream-split-sink-gt-10";

    public static final String SOURCE_TOPIC_THROUGH = "t-demo-stream-through-source";
    public static final String SINK_TOPIC_THROUGH_ONE = "t-demo-stream-through-sink-one";
    public static final String SINK_TOPIC_THROUGH_TWO = "t-demo-stream-through-sink-two";

    public static final String SOURCE_TOPIC_TO_TABLE = "t-demo-stream-to-table-source";
    public static final String SINK_TOPIC_TO_TABLE = "t-demo-stream-to-table-sink";

    public static final Aggregator<String, String, LocationMessage> WEATHER_AGGREGATOR = new Aggregator<String, String, LocationMessage>() {

        @Override
        public LocationMessage apply(String key, String value, LocationMessage aggregate) {
            aggregate.setWeather(value);
            return aggregate;
        }
    };

    public static final Aggregator<String, String, LocationMessage> TRAFFIC_AGGREGATOR = new Aggregator<String, String, LocationMessage>() {

        @Override
        public LocationMessage apply(String key, String value, LocationMessage aggregate) {
            aggregate.setTraffic(value);
            return aggregate;
        }
    };

    public static final Aggregator<String, Long, Long> NUMBER_AGGREGATOR = new Aggregator<String, Long, Long>() {

        @Override
        public Long apply(String key, Long value, Long aggregate) {
            return aggregate * value;
        }
    };

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    ProducerFactory<String, Long> producerFactoryStringLong(SslBundles sslBundles) {
        var properties = kafkaProperties.buildProducerProperties(sslBundles);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<String, Long> kafkaTemplateStringLong(SslBundles sslBundles) {
        return new KafkaTemplate<>(producerFactoryStringLong(sslBundles));
    }

    @Bean
    ProducerFactory<String, String> producerFactoryStringString(SslBundles sslBundles) {
        var properties = kafkaProperties.buildProducerProperties(sslBundles);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateStringString(SslBundles sslBundles) {
        return new KafkaTemplate<>(producerFactoryStringString(sslBundles));
    }

    @Bean
    NewTopic sourceTopicBranch() {
        return TopicBuilder.name(SOURCE_TOPIC_BRANCH).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicBranchGt100() {
        return TopicBuilder.name(SINK_TOPIC_BRANCH_GT_100).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicBranchGt20() {
        return TopicBuilder.name(SINK_TOPIC_BRANCH_GT_20).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicBranchGt10() {
        return TopicBuilder.name(SINK_TOPIC_BRANCH_GT_10).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicCogroupWeather() {
        return TopicBuilder.name(SOURCE_TOPIC_COGROUP_WEATHER).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicCogroupTraffic() {
        return TopicBuilder.name(SOURCE_TOPIC_COGROUP_TRAFFIC).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicCogroup() {
        return TopicBuilder.name(SINK_TOPIC_COGROUP).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicFilter() {
        return TopicBuilder.name(SOURCE_TOPIC_FILTER).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicFilter() {
        return TopicBuilder.name(SINK_TOPIC_FILTER).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicFilterNot() {
        return TopicBuilder.name(SOURCE_TOPIC_FILTER_NOT).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicFilterNot() {
        return TopicBuilder.name(SINK_TOPIC_FILTER_NOT).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicFlatMap() {
        return TopicBuilder.name(SOURCE_TOPIC_FLAT_MAP).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicFlatMap() {
        return TopicBuilder.name(SINK_TOPIC_FLAT_MAP).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicFlatMapValues() {
        return TopicBuilder.name(SOURCE_TOPIC_FLAT_MAP_VALUES).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicFlatMapValues() {
        return TopicBuilder.name(SINK_TOPIC_FLAT_MAP_VALUES).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicForEach() {
        return TopicBuilder.name(SOURCE_TOPIC_FOR_EACH).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicGroupBy() {
        return TopicBuilder.name(SOURCE_TOPIC_GROUP_BY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicGroupBy() {
        return TopicBuilder.name(SINK_TOPIC_GROUP_BY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicGroupByKey() {
        return TopicBuilder.name(SOURCE_TOPIC_GROUP_BY_KEY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicGroupByKey() {
        return TopicBuilder.name(SINK_TOPIC_GROUP_BY_KEY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicMap() {
        return TopicBuilder.name(SOURCE_TOPIC_MAP).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicMap() {
        return TopicBuilder.name(SINK_TOPIC_MAP).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicMapValues() {
        return TopicBuilder.name(SOURCE_TOPIC_MAP_VALUES).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicMapValues() {
        return TopicBuilder.name(SINK_TOPIC_MAP_VALUES).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicMergeAlphabet() {
        return TopicBuilder.name(SOURCE_TOPIC_MERGE_ALPHABET).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicMergeName() {
        return TopicBuilder.name(SOURCE_TOPIC_MERGE_NAME).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicMerge() {
        return TopicBuilder.name(SINK_TOPIC_MERGE).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicPeek() {
        return TopicBuilder.name(SOURCE_TOPIC_PEEK).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicPeek() {
        return TopicBuilder.name(SINK_TOPIC_PEEK).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicRepartition() {
        return TopicBuilder.name(SOURCE_TOPIC_REPARTITION).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicRepartitionOne() {
        return TopicBuilder.name(SINK_TOPIC_REPARTITION_ONE).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicRepartitionTwo() {
        return TopicBuilder.name(SINK_TOPIC_REPARTITION_TWO).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicSelectKey() {
        return TopicBuilder.name(SOURCE_TOPIC_SELECT_KEY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicSelectKey() {
        return TopicBuilder.name(SINK_TOPIC_SELECT_KEY).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicThrough() {
        return TopicBuilder.name(SOURCE_TOPIC_THROUGH).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicThroughOne() {
        return TopicBuilder.name(SINK_TOPIC_THROUGH_ONE).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicThroughTwo() {
        return TopicBuilder.name(SINK_TOPIC_THROUGH_TWO).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sourceTopicToTable() {
        return TopicBuilder.name(SOURCE_TOPIC_TO_TABLE).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sinkTopicToTable() {
        return TopicBuilder.name(SINK_TOPIC_TO_TABLE).partitions(1).replicas(1).build();
    }

}
