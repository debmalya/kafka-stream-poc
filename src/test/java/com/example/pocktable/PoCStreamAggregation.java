package com.example.pocktable;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import com.example.pocktable.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PoCStreamAggregation {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PoCStreamAggregation.class);

	public static void main(String[] args) {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final String schemaRegistryUrl = args.length > 0 ? args[1] : "localhost:8081";

		final KafkaStreams streams = buildFeed(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-stream");
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	static KafkaStreams buildFeed(final String bootstrapServers, final String schemaRegistryUrl,
			final String stateDir) {
		Properties streamsConfiguration = configure(bootstrapServers, stateDir);

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> feeds = builder.stream(Constants.FOREIGN_EXCHANGE_TOPIC_NAME);
		final KTable<String, Long> aggregated = feeds.groupByKey().count();

		aggregated.toStream().foreach((key, value) -> {
			log.info("Key : {} Count :{}", key, value);
		});

		aggregated.toStream().to(Constants.FOREIGN_EXCHANGE_SUMMARY_TOPIC_NAME, Produced.with(stringSerde, longSerde));
		return new KafkaStreams(builder.build(), streamsConfiguration);
	}

	private static Properties configure(final String bootstrapServers, final String stateDir) {
		final Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "poc-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "poc-client");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		streamsConfiguration.put(AbstractKafkaAvroSerDeConfg.SCHEMA_REGISTRY_URL_CONFIG, bootstrapServers);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		return streamsConfiguration;
	}

}
