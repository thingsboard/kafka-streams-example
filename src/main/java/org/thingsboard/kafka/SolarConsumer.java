/**
 * Copyright © 2016-2019 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class SolarConsumer {

    private static final String IN_TOPIC = "my-topic";

    private static final TopicNameExtractor<String, SolarModuleAggregatorJoiner> OUT_TOPIC =
            new StaticTopicNameExtractor<>("my-topic-output");

    // Time for windowing
    private static final Duration DURATION = Duration.ofSeconds(10);

    private static final TimeWindows TIME_WINDOWS = TimeWindows.of(DURATION);

    private static final JoinWindows JOIN_WINDOWS = JoinWindows.of(DURATION);

    private static final StreamsBuilder builder = new StreamsBuilder();

    // serde - Serializer/Deserializer
    // for custom classes should be custom Serializer/Deserializer
    private static final Serde<SolarModuleData> SOLAR_MODULE_DATA_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleData.class));

    private static final Serde<SolarModuleAggregator> SOLAR_MODULE_AGGREGATOR_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleAggregator.class));

    private static final Serde<SolarPanelAggregator> SOLAR_PANEL_AGGREGATOR_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarPanelAggregator.class));

    private static final Serde<SolarModuleKey> SOLAR_MODULE_KEY_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleKey.class));

    private static final Serde<SolarPanelAggregatorJoiner> SOLAR_PANEL_AGGREGATOR_JOINER_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarPanelAggregatorJoiner.class));

    private static final Serde<SolarModuleAggregatorJoiner> SOLAR_MODULE_AGGREGATOR_JOINER_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(SolarModuleAggregatorJoiner.class));

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final Serde<Windowed<String>> WINDOWED_STRING_SERDE = Serdes.serdeFrom(
            new TimeWindowedSerializer<>(STRING_SERDE.serializer()),
            new TimeWindowedDeserializer<>(STRING_SERDE.deserializer(), TIME_WINDOWS.size()));

    // 1 - sigma
    private static final double Z = 1;

    public static void main(final String[] args) {
        runKafkaStreams();
    }

    private static void runKafkaStreams() {

        // source stream from kafka
        final KStream<SolarModuleKey, SolarModuleData> source =
                builder
                        .stream(IN_TOPIC, Consumed.with(STRING_SERDE, SOLAR_MODULE_DATA_SERDE))
                        .map((k, v) -> KeyValue.pair(new SolarModuleKey(v.getPanel(), v.getName()), v));

        source.foreach((k, v) -> {
            log.info("NEW DATA: [{}|{}]: {}", k.getPanelName(), k.getModuleName(), v.getPower());
        });

        // calculating sum power and average power for modules
        final KStream<Windowed<SolarModuleKey>, SolarModuleAggregator> aggPowerPerSolarModuleStream =
                source
                        .groupByKey(Grouped.with(SOLAR_MODULE_KEY_SERDE, SOLAR_MODULE_DATA_SERDE))
                        .windowedBy(TIME_WINDOWS)
                        .aggregate(SolarModuleAggregator::new,
                                (modelKey, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(SOLAR_MODULE_KEY_SERDE, SOLAR_MODULE_AGGREGATOR_SERDE))
                        .suppress(Suppressed.untilTimeLimit(DURATION, Suppressed.BufferConfig.unbounded()))
                        .toStream();

        aggPowerPerSolarModuleStream.foreach(
                (k, v) -> log.info("PerSolarModule: [{}|{}|{}]: {}:{}",
                        k.window().endTime().getEpochSecond(), k.key().getPanelName(), k.key().getModuleName(), v.getSumPower(), v.getCount()));

        // calculating sum power and average power for panels
        final KStream<Windowed<String>, SolarPanelAggregator> aggPowerPerSolarPanelStream =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v))
                        .groupByKey(Grouped.with(WINDOWED_STRING_SERDE, SOLAR_MODULE_AGGREGATOR_SERDE))
                        .aggregate(SolarPanelAggregator::new,
                                (panelKey, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(WINDOWED_STRING_SERDE, SOLAR_PANEL_AGGREGATOR_SERDE))
                        .suppress(Suppressed.untilTimeLimit(DURATION, Suppressed.BufferConfig.unbounded()))
                        .toStream();
        aggPowerPerSolarPanelStream.foreach(
                (k, v) -> log.info("PerSolarPanel: [{}|{}]: {}:{}",
                        k.window().endTime().getEpochSecond(), k.key(), v.getSumPower(), v.getCount()));

        // if used for join more than once, the exception "TopologyException: Invalid topology:" will be thrown
        final KStream<Windowed<String>, SolarModuleAggregator> aggPowerPerSolarModuleForJoinStream =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v));

        // joining aggregated panels with aggregated modules
        // need for calculating sumSquare and deviance
        final KStream<Windowed<String>, SolarPanelAggregatorJoiner> joinedAggPanelWithAggModule =
                aggPowerPerSolarPanelStream
                        .join(
                                aggPowerPerSolarModuleForJoinStream,
                                SolarPanelAggregatorJoiner::new, JOIN_WINDOWS,
                                Joined.with(WINDOWED_STRING_SERDE, SOLAR_PANEL_AGGREGATOR_SERDE, SOLAR_MODULE_AGGREGATOR_SERDE));

        //calculating sumSquare and deviance
        final KStream<Windowed<String>, SolarPanelAggregator> aggPowerPerSolarPanelFinalStream =
                joinedAggPanelWithAggModule
                        .groupByKey(Grouped.with(WINDOWED_STRING_SERDE, SOLAR_PANEL_AGGREGATOR_JOINER_SERDE))
                        .aggregate(SolarPanelAggregator::new,
                                (key, value, aggregation) -> aggregation.updateFrom(value),
                                Materialized.with(WINDOWED_STRING_SERDE, SOLAR_PANEL_AGGREGATOR_SERDE))
                        .suppress(Suppressed.untilTimeLimit(DURATION, Suppressed.BufferConfig.unbounded()))
                        .toStream();

        aggPowerPerSolarPanelFinalStream.foreach(
                (k, v) -> log.info("PerSolarPanelFinal: [{}|{}]: power:{} count:{} squareSum:{} variance:{} deviance:{}",
                        k.window().endTime().getEpochSecond(), k.key(), v.getSumPower(), v.getCount(), v.getSquaresSum(), v.getVariance(), v.getDeviance()));

        // joining aggregated modules with aggregated panels in which calculated sumSquare and deviance
        // need for check modules with anomaly power value
        final KStream<Windowed<String>, SolarModuleAggregatorJoiner> joinedAggModuleWithAggPanel =
                aggPowerPerSolarModuleStream
                        .map((k, v) -> KeyValue.pair(new Windowed<>(k.key().getPanelName(), k.window()), v))
                        .join(
                                aggPowerPerSolarPanelFinalStream,
                                SolarModuleAggregatorJoiner::new, JOIN_WINDOWS,
                                Joined.with(WINDOWED_STRING_SERDE,
                                        SOLAR_MODULE_AGGREGATOR_SERDE,
                                        SOLAR_PANEL_AGGREGATOR_SERDE));

        joinedAggModuleWithAggPanel.foreach(
                (k, v) -> {
                    if (isAnomalyModule(v)) {
                        log.info("ANOMALY module: [{}|{}|{}]: sumPower:{} panelAvg:{} deviance:{}",
                                k.window().endTime().getEpochSecond(), k.key(), v.getModuleName(),
                                v.getSumPower(), v.getSolarPanelAggregator().getAvgPower(), v.getSolarPanelAggregator().getDeviance());
                    }
                });

        // streaming result data (modules with anomaly power value)
        joinedAggModuleWithAggPanel
                .filter((k, v) -> isAnomalyModule(v))
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .to(OUT_TOPIC, Produced.valueSerde(SOLAR_MODULE_AGGREGATOR_JOINER_SERDE));

        log.info("STARTING");
        final KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
        streams.cleanUp();
        streams.start();
        log.info("STARTED");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isAnomalyModule(SolarModuleAggregatorJoiner module) {
        double currentZ = Math.abs(module.getSumPower() - module.getSolarPanelAggregator().getAvgPower()) / module.getSolarPanelAggregator().getDeviance();
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