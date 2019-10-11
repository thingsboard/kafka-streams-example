package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

public class SolarConsumer {

    public static void main(final String[] args) throws Exception {
        runKafkaStreams();
    }

    private static void runKafkaStreams() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("solar-out-topic");

        final Duration duration = Duration.ofSeconds(30);

        final TimeWindows timeWindows = TimeWindows.of(duration);

        final JoinWindows joinWindows = JoinWindows.of(duration);

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<SolarModuleData> solarModuleDataSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleData.class));

        final Serde<AggregationPerSolarModule> aggregationPowerPerSolarModuleSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarModule.class));

        final Serde<AggregationPerSolarPanel> aggregationPerSolarPanelSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarPanel.class));

        final Serde<SolarModuleKey> solarModuleKeySerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleKey.class));

        final Serde<Aggregation> aggregationSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(Aggregation.class));

        final Serde<AggregationJoiner> aggregationJoinerSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationJoiner.class));

        final Serde<PanelJoiner> panelJoinerSerde =
                Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(PanelJoiner.class));

        final Serde<String> stringSerde = Serdes.String();

        final Serde<Windowed<SolarModuleKey>> windowedSolarModuleKeySerde =
                Serdes.serdeFrom(
                        new TimeWindowedSerializer<>(new JsonPojoSerializer<>()),
                        new TimeWindowedDeserializer<>(new JsonPojoDeserializer<>(SolarModuleKey.class)));

        final KStream<SolarModuleKey, SolarModuleData> source = builder
                .stream("my-topic", Consumed.with(stringSerde, solarModuleDataSerde))
                .map((k, v) -> KeyValue.pair(new SolarModuleKey(v.getPanel(), v.getName()), v));


        final KStream<SolarModuleKey, AggregationPerSolarModule> aggPowerPerSolarModuleStream =
                source
                        .groupByKey(Grouped.with(solarModuleKeySerde, solarModuleDataSerde))
                        .windowedBy(timeWindows)
                        .aggregate(AggregationPerSolarModule::new,
                                (modelKey, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(solarModuleKeySerde, aggregationPowerPerSolarModuleSerde))
                        .toStream()
                        .map((k, v) -> KeyValue.pair(k.key(), v));
        aggPowerPerSolarModuleStream.foreach((k, v) -> System.out.printf("part number 1 module: %s value: %s\n", k, v));


        final KStream<String, AggregationPerSolarPanel> aggPowerPerSolarPanelStream =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(k.getPanelName(), v))
                        .groupByKey(Grouped.with(stringSerde, aggregationPowerPerSolarModuleSerde))
                        .windowedBy(timeWindows)
                        .aggregate(AggregationPerSolarPanel::new,
                                (panelKey, value, aggregation) -> aggregation.updateFrom(panelKey, value),
                                Materialized.with(stringSerde, aggregationPerSolarPanelSerde))
                        .toStream()
                        .map((k, v) -> KeyValue.pair(k.key(), v));
        aggPowerPerSolarPanelStream.foreach((k, v) -> System.out.printf("part number 2 panel: %s value: %s\n", k, v));


        final KStream<String, Aggregation> aggregationPanels =
                aggPowerPerSolarPanelStream
                        .map((k, v) -> KeyValue.pair("Panels", v))
                        .groupByKey(Grouped.with(stringSerde, aggregationPerSolarPanelSerde))
                        .windowedBy(timeWindows)
                        .aggregate(Aggregation::new,
                                (key, value, aggregation) -> aggregation.updateAvg(value),
                                Materialized.with(stringSerde, aggregationSerde))
                        .toStream()
                        .map((k, v) -> KeyValue.pair(k.key(), v))
                        .join(aggPowerPerSolarPanelStream.map((k, v) -> KeyValue.pair("Panels", v)),
                                (agg, panel) -> new AggregationJoiner().updateFrom(agg, panel),
                                joinWindows,
                                Joined.with(stringSerde, aggregationSerde, aggregationPerSolarPanelSerde))
                        .groupByKey(Grouped.with(stringSerde, aggregationJoinerSerde))
                        .windowedBy(timeWindows)
                        .aggregate(Aggregation::new,
                                (key, value, aggregation) -> aggregation.updateDeviance(value),
                                Materialized.with(stringSerde, aggregationSerde))
                        .toStream()
                        .map((k, v) -> KeyValue.pair(k.key(), v));

        aggregationPanels.foreach((k, v) -> System.out.printf("part number 3 %s %s\n", k, v));

        final KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}