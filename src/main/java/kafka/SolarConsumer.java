package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class SolarConsumer {

    public static void main(final String[] args) throws Exception {
        runKafkaStreams();
    }

    static final Duration duration = Duration.ofSeconds(3);

    static final TimeWindows timeWindows = TimeWindows.of(duration);

    static final JoinWindows joinWindows = JoinWindows.of(duration);

    static final StreamsBuilder builder = new StreamsBuilder();

    static final Serde<SolarModuleData> solarModuleDataSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleData.class));

    static final Serde<AggregationPerSolarModule> aggregationPowerPerSolarModuleSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarModule.class));

    static final Serde<AggregationPerSolarPanel> aggregationPerSolarPanelSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarPanel.class));

    static final Serde<SolarModuleKey> solarModuleKeySerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleKey.class));

    static final Serde<Aggregation> aggregationSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(Aggregation.class));

    static final Serde<AggregationJoiner> aggregationJoinerSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationJoiner.class));

    static final Serde<PanelJoiner> panelJoinerSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(PanelJoiner.class));

    static final Serde<String> stringSerde = Serdes.String();
    static final StringSerializer stringSerializer = new StringSerializer();
    static final StringDeserializer stringDeserializer = new StringDeserializer();
    static final TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(stringSerializer);
    static final TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(stringDeserializer, timeWindows.size());
    static final Serde<Windowed<String>> windowedStringSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

    static final Serde<Windowed<String>> windowedSolarModuleKeySerde =
            Serdes.serdeFrom(
                    new SessionWindowedSerializer<>(stringSerializer),
                    new SessionWindowedDeserializer<>(stringDeserializer));

    private static void runKafkaStreams() {
        final KStream<SolarModuleKey, SolarModuleData> source = builder
                .stream("my-topic", Consumed.with(stringSerde, solarModuleDataSerde))
                .map((k, v) -> KeyValue.pair(new SolarModuleKey(v.getPanel(), v.getName()), v));

        source.foreach((k, v) -> {
            log.info("NEW DATA: [{}|{}]: {}", k.getPanelName(), k.getModuleName(), v.getPower());
        });

        final KStream<Windowed<SolarModuleKey>, AggregationPerSolarModule> aggPowerPerSolarModuleStream =
                source
                        .groupByKey(Grouped.with(solarModuleKeySerde, solarModuleDataSerde))
                        .windowedBy(timeWindows)
                        .aggregate(AggregationPerSolarModule::new,
                                (modelKey, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(solarModuleKeySerde, aggregationPowerPerSolarModuleSerde))
                        .suppress(Suppressed.untilTimeLimit(duration, Suppressed.BufferConfig.unbounded()))
                        .toStream();
        aggPowerPerSolarModuleStream.foreach(
                (k, v) -> log.info("PerSolarModule: [{}|{}|{}]: {}:{}", k.window().endTime().getEpochSecond(), k.key().getPanelName(), k.key().getModuleName(), v.getSumPower(), v.getCount()));


        final KStream<Windowed<String>, AggregationPerSolarPanel> aggPowerPerSolarPanelStream =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v))
                        .groupByKey(Grouped.with(windowedStringSerde, aggregationPowerPerSolarModuleSerde))
                        .aggregate(AggregationPerSolarPanel::new,
                                (panelKey, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(windowedStringSerde, aggregationPerSolarPanelSerde))
                        .suppress(Suppressed.untilTimeLimit(duration, Suppressed.BufferConfig.unbounded()))
                        .toStream();
        aggPowerPerSolarPanelStream.foreach(
                (k, v) -> log.info("PerSolarPanel: [{}|{}]: {}:{}", k.window().endTime().getEpochSecond(), k.key(), v.getSumPower(), v.getCount()));

//        final KStream<String, Aggregation> aggregationPanels =
//                aggPowerPerSolarPanelStream
//                        .map((k, v) -> KeyValue.pair("Panels", v))
//                        .groupByKey(Grouped.with(stringSerde, aggregationPerSolarPanelSerde))
//                        .windowedBy(timeWindows)
//                        .aggregate(Aggregation::new,
//                                (key, value, aggregation) -> aggregation.updateAvg(value),
//                                Materialized.with(stringSerde, aggregationSerde))
//                        .toStream()
//                        .map((k, v) -> KeyValue.pair(k.key(), v))
//                        .join(aggPowerPerSolarPanelStream.map((k, v) -> KeyValue.pair("Panels", v)),
//                                (agg, panel) -> new AggregationJoiner().updateFrom(agg, panel),
//                                joinWindows,
//                                Joined.with(stringSerde, aggregationSerde, aggregationPerSolarPanelSerde))
//                        .groupByKey(Grouped.with(stringSerde, aggregationJoinerSerde))
//                        .windowedBy(timeWindows)
//                        .aggregate(Aggregation::new,
//                                (key, value, aggregation) -> aggregation.updateDeviance(value),
//                                Materialized.with(stringSerde, aggregationSerde))
//                        .toStream()
//                        .map((k, v) -> KeyValue.pair(k.key(), v));
//
//        aggregationPanels.foreach((k, v) -> System.out.printf("part number 3 %s %s\n", k, v));

        log.info("STARTING");
        final KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
        streams.cleanUp();
        streams.start();
        log.info("STARTED");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }
}