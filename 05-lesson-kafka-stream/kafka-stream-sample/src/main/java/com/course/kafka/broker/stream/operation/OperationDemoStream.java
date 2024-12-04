package com.course.kafka.broker.stream.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.LocationMessage;
import com.course.kafka.broker.message.PrimeFactorsMessage;
import com.course.kafka.config.OperationDemoStreamConfig;

/**
 * To use, turn on the @Component and @Autowired annotation for the operation
 * you want.
 * Then, in the KafkaStreamSampleApplication.java file in the main package,
 * call the operationDemoStream.demoXxx() method.
 */
// @Component
public class OperationDemoStream {

    @Autowired
    private KafkaTemplate<String, Long> kafkaTemplateStringLong;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateStringString;

    private static final Logger LOG = LoggerFactory.getLogger(OperationDemoStream.class);

    // sample kafka stream branch operation
    // @Autowired
    @SuppressWarnings({ "deprecation", "unchecked" })
    void branch(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_BRANCH,
                Consumed.with(Serdes.String(), Serdes.Long()));

        Predicate<String, Long> isGreaterThan100 = (key, value) -> value > 100;
        Predicate<String, Long> isGreaterThan20 = (key, value) -> value > 20;
        Predicate<String, Long> isGreaterThan10 = (key, value) -> value > 10;

        var arrayStream = sourceStream.branch(isGreaterThan100, isGreaterThan20,
                isGreaterThan10);

        // print to console
        arrayStream[0].foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_100, key, value));
        arrayStream[1].foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_20, key, value));
        arrayStream[2].foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_10, key, value));

        // send to sink topic
        arrayStream[0].to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_100);
        arrayStream[1].to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_20);
        arrayStream[2].to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_10);
    }

    public void demoBranch() throws InterruptedException {
        for (int i = 0; i < 30; i++) {
            kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_BRANCH, "key-" + i,
                    ThreadLocalRandom.current().nextLong(0, 150));
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    // sample kafka stream cogroup operation
    // @Autowired
    void cogroup(StreamsBuilder builder) {
        var jsonSerde = new JsonSerde<>(LocationMessage.class);
        var weatherStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_WEATHER,
                Consumed.with(Serdes.String(), Serdes.String()));
        var trafficStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_TRAFFIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        var groupedWeather = weatherStream.groupByKey();
        var groupedTraffic = trafficStream.groupByKey();

        var locationsCogroup = groupedWeather.cogroup(OperationDemoStreamConfig.WEATHER_AGGREGATOR)
                .cogroup(groupedTraffic, OperationDemoStreamConfig.TRAFFIC_AGGREGATOR)
                .aggregate(() -> new LocationMessage(), Materialized.with(Serdes.String(), jsonSerde));

        // print to console
        locationsCogroup.toStream()
                .foreach(
                        (key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                                OperationDemoStreamConfig.SINK_TOPIC_COGROUP, key,
                                value));

        // send to sink topic
        locationsCogroup.toStream().to(OperationDemoStreamConfig.SINK_TOPIC_COGROUP,
                Produced.with(Serdes.String(), jsonSerde));
    }

    public void demoCogroup() throws InterruptedException {
        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_WEATHER, "loc-A",
                "sunny");
        TimeUnit.SECONDS.sleep(20);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_TRAFFIC, "loc-A",
                "light");
        TimeUnit.SECONDS.sleep(20);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_TRAFFIC, "loc-B",
                "medium");
        TimeUnit.SECONDS.sleep(20);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_WEATHER, "loc-B",
                "rainy");
        TimeUnit.SECONDS.sleep(20);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_COGROUP_WEATHER, "loc-A",
                "cloudy");
    }

    // sample kafka stream filter operation
    // @Autowired
    void filter(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.filter((key, value) -> value % 2 == 0);

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_FILTER,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_FILTER);
    }

    public void demoFilter() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER, "A", 5L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER, "B", 8L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER, "C", 7L);
    }

    // sample kafka stream filterNot operation
    // @Autowired
    void filterNot(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER_NOT,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.filterNot((key, value) -> value % 2 == 0);

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_FILTER_NOT,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_FILTER_NOT);
    }

    public void demoFilterNot() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER_NOT, "A", 5L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER_NOT, "B", 8L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FILTER_NOT, "C", 7L);
    }

    // sample kafka stream flatMap operation
    // @Autowired
    void flatMap(StreamsBuilder builder) {
        var jsonSerde = new JsonSerde<>(PrimeFactorsMessage.class);
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream
                .flatMap(this::listPrimeFactorsAndAppendKey);

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_FLAT_MAP,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_FLAT_MAP, Produced.with(Serdes.String(), jsonSerde));
    }

    private List<KeyValue<String, PrimeFactorsMessage>> listPrimeFactorsAndAppendKey(String key, Long value) {
        var result = new ArrayList<KeyValue<String, PrimeFactorsMessage>>();

        var primeFactors = new PrimeFactorsMessage();
        primeFactors.setPrimeFactors(primeFactors(value));

        result.add(KeyValue.pair(key + "Z", primeFactors));
        return result;
    }

    private Set<Long> primeFactors(long numbers) {
        long n = numbers;
        var factors = new TreeSet<Long>();
        for (var i = 2L; i <= n / i; i++) {
            while (n % i == 0) {
                factors.add(i);
                n /= i;
            }
        }
        if (n > 1) {
            factors.add(n);
        }
        return factors;
    }

    public void demoFlatMap() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP, "A", 3L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP, "B", 36L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP, "C", 1L);
    }

    // sample kafka stream flatMapValues operation
    // @Autowired
    void flatMapValues(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP_VALUES,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.flatMapValues(listPrimeFactors());

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_FLAT_MAP_VALUES,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_FLAT_MAP_VALUES,
                Produced.with(Serdes.String(), Serdes.Long()));
    }

    private ValueMapper<Long, Set<Long>> listPrimeFactors() {
        return value -> {
            return primeFactors(value);
        };
    }

    public void demoFlatMapValues() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP_VALUES, "A", 40L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP_VALUES, "B", 5L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FLAT_MAP_VALUES, "C", 1L);
    }

    // sample kafka stream forEach operation
    // @Autowired
    void forEach(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_FOR_EACH,
                Consumed.with(Serdes.String(), Serdes.Long()));

        sourceStream.foreach((k, v) -> insertToDatabase(v));
    }

    private void insertToDatabase(Long value) {
        LOG.info("Inserting value {} to database", value);
    }

    public void demoForEach() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FOR_EACH, "A", 6L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FOR_EACH, "B", 4L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_FOR_EACH, "C", 2L);
    }

    // sample kafka stream groupBy operation
    // @Autowired
    void groupBy(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var groupedStream = sourceStream.groupBy((k, v) -> v % 2 == 0 ? "EVEN" : "ODD",
                Grouped.with(Serdes.String(), Serdes.Long()));

        // example : sum the value on the groupedStream based on key, then display them
        groupedStream.reduce(Long::sum).toStream().foreach((k, v) -> LOG.info("[{}] Key: {}, Value: {}",
                OperationDemoStreamConfig.SINK_TOPIC_GROUP_BY, k, v));

        // example : to count how many data in each grouped key
        // groupedStream.count().toStream().foreach((k, v) -> LOG.info("[{}] Key: {},
        // Value: {}",
        // OperationDemoConfig.SINK_TOPIC_GROUP_BY, k, v));

        // example : custom aggregator, multiplying each value with aggregated value
        // initial aggregated value is 1L
        // groupedStream.aggregate(() -> 1L, OperationDemoConfig.NUMBER_AGGREGATOR,
        // Materialized.with(Serdes.String(), Serdes.Long()))
        // .toStream().foreach((k, v) -> LOG.info("[{}] Key: {}, Value: {}",
        // OperationDemoConfig.SINK_TOPIC_GROUP_BY, k, v));
    }

    public void demoGroupBy() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY, "A", 15L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY, "B", 20L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY, "A", 6L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY, "B", 17L);
    }

    // sample kafka stream groupBy operation
    // @Autowired
    void groupByKey(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY_KEY,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var groupedStream = sourceStream.groupByKey();

        // example : sum the value on the groupedStream based on key, then display them
        groupedStream.reduce(Long::sum).toStream().foreach((k, v) -> LOG.info("[{}] Key: {}, Value: {}",
                OperationDemoStreamConfig.SINK_TOPIC_GROUP_BY_KEY, k, v));

        // example : to count how many data in each grouped key
        // groupedStream.count().toStream().foreach((k, v) -> LOG.info("[{}] Key: {},
        // Value: {}",
        // OperationDemoConfig.SINK_TOPIC_GROUP_BY_KEY, k, v));

        // example : custom aggregator, multiplying each value with aggregated value
        // initial aggregated value is 1L
        // groupedStream.aggregate(() -> 1L, OperationDemoConfig.NUMBER_AGGREGATOR,
        // Materialized.with(Serdes.String(), Serdes.Long()))
        // .toStream().foreach((k, v) -> LOG.info("[{}] Key: {}, Value: {}",
        // OperationDemoConfig.SINK_TOPIC_GROUP_BY_KEY, k, v));
    }

    public void demoGroupByKey() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY_KEY, "A", 12L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY_KEY, "B", 14L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY_KEY, "A", 3L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_GROUP_BY_KEY, "B", 19L);
    }

    // sample kafka stream map operation
    // @Autowired
    void map(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_MAP,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.map((k, v) -> KeyValue.pair("X" + k, v * 5));

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_MAP,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_MAP, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public void demoMap() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP, "A", 5L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP, "B", 2L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP, "C", 8L);
    }

    // sample kafka stream mapValues operation
    // @Autowired
    void mapValues(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_MAP_VALUES,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.map((k, v) -> KeyValue.pair("X" + k, v * 5));

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_MAP_VALUES,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_MAP_VALUES,
                Produced.with(Serdes.String(), Serdes.Long()));
    }

    public void demoMapValues() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP_VALUES, "A", 32L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP_VALUES, "B", 6L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_MAP_VALUES, "C", 14L);
    }

    // sample kafka stream merge operation
    // @Autowired
    void merge(StreamsBuilder builder) {
        var sourceStreamAlphabet = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_ALPHABET,
                Consumed.with(Serdes.String(), Serdes.String()));
        var sourceStreamName = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_NAME,
                Consumed.with(Serdes.String(), Serdes.String()));

        var sinkStream = sourceStreamAlphabet.merge(sourceStreamName);

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_MERGE,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_MERGE);
    }

    public void demoMerge() throws InterruptedException {
        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_ALPHABET, "K1", "P");
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_NAME, "K11", "John");
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_ALPHABET, "K2", "Q");
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_NAME, "K22", "Jane");
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringString.send(OperationDemoStreamConfig.SOURCE_TOPIC_MERGE_ALPHABET, "K3", "R");
    }

    // sample kafka stream peek operation
    // @Autowired
    void peek(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_PEEK,
                Consumed.with(Serdes.String(), Serdes.Long()));

        var sinkStream = sourceStream.peek((k, v) -> insertToDatabase(v));

        // continue next processor for sink stream, can be anything
        // e.g filtering, ...
        // then send to sink topic
        sinkStream.filter((k, v) -> v % 2 == 0).to(OperationDemoStreamConfig.SINK_TOPIC_PEEK);
    }

    public void demoPeek() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_PEEK, "A", 8L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_PEEK, "B", 1L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_PEEK, "C", 3L);
    }

    // sample kafka stream repartition operation
    // @Autowired
    void repartition(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_REPARTITION,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // internal topic
        var sinkStream = sourceStream.repartition();

        // continue next processor for sink stream, can be anything
        // e.g filtering, ...
        // then send to sink topic
        sinkStream.filter((k, v) -> v % 2 == 0).to(OperationDemoStreamConfig.SINK_TOPIC_REPARTITION_TWO);
    }

    public void demoRepartition() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_REPARTITION, "A", 6L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_REPARTITION, "B", 2L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_REPARTITION, "C", 9L);
    }

    // sample kafka stream selectKey operation
    // @Autowired
    void selectKey(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_SELECT_KEY,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // internal topic
        var sinkStream = sourceStream.selectKey((k, v) -> "XY" + k);

        // print to console
        sinkStream
                .foreach((key, value) -> LOG.info("[{}] Key: {}, Value: {}",
                        OperationDemoStreamConfig.SINK_TOPIC_SELECT_KEY,
                        key, value));

        // send to sink topic
        sinkStream.to(OperationDemoStreamConfig.SINK_TOPIC_SELECT_KEY);
    }

    public void demoSelectKey() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_SELECT_KEY, "green", 15L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_SELECT_KEY, "yellow", 56L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_SELECT_KEY, "pink", 72L);
    }

    // sample kafka stream split operation
    // @Autowired
    void split(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_SPLIT,
                Consumed.with(Serdes.String(), Serdes.Long()));

        Predicate<String, Long> isGreaterThan100 = (key, value) -> value > 100;
        Predicate<String, Long> isGreaterThan20 = (key, value) -> value > 20;
        Predicate<String, Long> isGreaterThan10 = (key, value) -> value > 10;

        // will send to the topics based on the predicates
        sourceStream.split()
                .branch(isGreaterThan100, Branched.<String, Long>withConsumer(
                        ks -> ks.to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_100)))
                .branch(isGreaterThan20, Branched.<String, Long>withConsumer(
                        ks -> ks.to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_20)))
                .branch(isGreaterThan10, Branched.<String, Long>withConsumer(
                        ks -> ks.to(OperationDemoStreamConfig.SINK_TOPIC_BRANCH_GT_10)));

    }

    public void demoSplit() throws InterruptedException {
        for (int i = 0; i < 30; i++) {
            kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_SPLIT, "key-" + i,
                    ThreadLocalRandom.current().nextLong(0, 150));
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    // sample kafka stream through operation
    @SuppressWarnings("deprecation")
    // @Autowired
    void through(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_THROUGH,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // send everything to this sink topic, as intermediate operation
        var sinkStream = sourceStream.through(OperationDemoStreamConfig.SINK_TOPIC_THROUGH_ONE,
                Produced.with(Serdes.String(), Serdes.Long()));

        // continue next processor for sink stream, can be anything
        // e.g filtering, ...
        // then send to other topic
        sinkStream.filter((k, v) -> v % 2 == 0).to(OperationDemoStreamConfig.SINK_TOPIC_THROUGH_TWO);
    }

    public void demoThrough() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_THROUGH, "A", 6L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_THROUGH, "B", 2L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_THROUGH, "C", 9L);
    }

    // sample kafka stream toTable operation
    // @Autowired
    @SuppressWarnings("unused")
    void toTable(StreamsBuilder builder) {
        var sourceStream = builder.stream(OperationDemoStreamConfig.SOURCE_TOPIC_TO_TABLE,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // producing KTable for further processing
        var sourceTable = sourceStream.toTable();
    }

    public void demoToTable() throws InterruptedException {
        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_TO_TABLE, "A", 20L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_TO_TABLE, "B", 25L);
        TimeUnit.MILLISECONDS.sleep(500);

        kafkaTemplateStringLong.send(OperationDemoStreamConfig.SOURCE_TOPIC_TO_TABLE, "C", 30L);
    }

}
