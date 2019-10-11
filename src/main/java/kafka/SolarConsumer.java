package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class SolarConsumer {

    private static final Duration duration = Duration.ofSeconds(10);

    private static final TimeWindows timeWindows = TimeWindows.of(duration);

    private static final JoinWindows joinWindows = JoinWindows.of(duration);

    private static final StreamsBuilder builder = new StreamsBuilder();

    private static final Serde<SolarModuleData> solarModuleDataSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleData.class));

    private static final Serde<AggregationPerSolarModule> aggregationPowerPerSolarModuleSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarModule.class));

    private static final Serde<AggregationPerSolarPanel> aggregationPerSolarPanelSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(AggregationPerSolarPanel.class));

    private static final Serde<SolarModuleKey> solarModuleKeySerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleKey.class));

    private static final Serde<JoinedPanel> joinedPanelSerde =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(JoinedPanel.class));

    private static final Serde<String> stringSerde = Serdes.String();

    private static final Serde<Windowed<String>> windowedStringSerde = Serdes.serdeFrom(
            new TimeWindowedSerializer<>(stringSerde.serializer()),
            new TimeWindowedDeserializer<>(stringSerde.deserializer(), timeWindows.size()));

    private static final double Z = 1;

    public static void main(final String[] args) {
        runKafkaStreams();
    }

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


        //if used for join more than once, the exception "TopologyException: Invalid topology:" will be thrown
        final KStream<Windowed<String>, AggregationPerSolarModule> aggPowerPerSolarModuleForJoinStream =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v));

        final KStream<Windowed<String>, JoinedPanel> joinedAggPanelWithAggModule =
                aggPowerPerSolarPanelStream.join(
                        aggPowerPerSolarModuleForJoinStream,
                        JoinedPanel::new, joinWindows,
                        Joined.with(windowedStringSerde, aggregationPerSolarPanelSerde, aggregationPowerPerSolarModuleSerde)
                );

        final KStream<Windowed<String>, AggregationPerSolarPanel> aggPowerPerSolarPanelFinalStream =
                joinedAggPanelWithAggModule
                        .groupByKey(Grouped.with(windowedStringSerde, joinedPanelSerde))
                        .aggregate(AggregationPerSolarPanel::new,
                                (key, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(windowedStringSerde, aggregationPerSolarPanelSerde))
                        .suppress(Suppressed.untilTimeLimit(duration, Suppressed.BufferConfig.unbounded()))
                        .toStream();

        aggPowerPerSolarPanelFinalStream.foreach(
                (k, v) -> log.info("PerSolarPanelFinal: [{}|{}]: power:{} count:{} squareSum:{} variance:{} deviance:{}",
                        k.window().endTime().getEpochSecond(), k.key(), v.getSumPower(), v.getCount(), v.getSquaresSum(), v.getVariance(), v.getDeviance()));


        final KStream<Windowed<String>, JoinedModule> joinedAggModuleWithAggPanel =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v))
                        .join(
                                aggPowerPerSolarPanelFinalStream,
                                JoinedModule::new, joinWindows,
                                Joined.with(windowedStringSerde, aggregationPowerPerSolarModuleSerde, aggregationPerSolarPanelSerde)
                        );

        joinedAggModuleWithAggPanel.foreach(
                (k, v) -> {
                    if (isAnomalyModule(v)) {
                        log.info("ANOMALY: [{}|{}|{}]: sumPower:{} panelAvg:{} deviance:{}",
                                k.window().endTime().getEpochSecond(), k.key(), v.getModuleName(),
                                v.getSumPower(), v.getAggregationPerSolarPanel().getAvgPower(), v.getAggregationPerSolarPanel().getDeviance());
                    }
                });


        log.info("STARTING");
        final KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
        streams.cleanUp();
        streams.start();
        log.info("STARTED");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isAnomalyModule(JoinedModule module) {
        double currentZ = Math.abs(module.getSumPower() - module.getAggregationPerSolarPanel().getAvgPower()) / module.getAggregationPerSolarPanel().getDeviance();
        return currentZ > Z;
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