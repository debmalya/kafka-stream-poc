package com.example.pocktable.processor;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Service;

import com.example.pocktable.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ForeignExchangeProcessor implements Processor<String, String, String, String> {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForeignExchangeProcessor.class);
	private ProcessorContext<String, String> context;
	private KeyValueStore<String, String> kvStore;

	private KafkaProducer<String, String> kafkaProducer;

//	When we schedule a punctuator function, it will return a cancellable object that we can use to stop the schedules function later.
	private Cancellable punctuator;

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext<String, String> context) {
		this.context = context;

		// retrieve the key-value store named "foriegn-exchange-store"
		kvStore = (KeyValueStore<String, String>) context.getStateStore(Constants.FOREIGN_EXCHANGE_STORE_NAME);

		// schedule a punctuate() method every 1000 milliseconds based on stream-time
		this.context.schedule(Duration.ofMillis(1000), PunctuationType.STREAM_TIME, (timestamp) -> {
			KeyValueIterator<String, String> iter = this.kvStore.all();
			while (iter.hasNext()) {
				KeyValue<String, String> entry = iter.next();
				Record<String, String> record = new Record<>(entry.key, entry.value, System.currentTimeMillis());
				context.forward(record);
			}
			iter.close();

			// commit the current processing progress
			context.commit();
			log.info("commit completed");
		});

		// Scheduling punctuator. Wall clock time is local system time, which is
		// advanced during each iteration of consumer poll method. Periodic function
		// will continue to execute regardless of a whether a new message arrive.
		punctuator = this.context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::enforceTtl);
	}

	@Override
	public void process(Record<String, String> record) {
		String key = record.key();
		String value = record.value();
		String existingValue = kvStore.get(record.key());
		if (existingValue != null) {
			log.info("Existing key : {}, value : {}", key, existingValue);
		}

		kvStore.put(key, value);
		existingValue = kvStore.get(record.key());
		log.info("Key : {}, new value : {}", key, existingValue);
	}

	@Override
	public void close() {
		if (kafkaProducer != null) {
			kafkaProducer.flush();
			kafkaProducer.close();
			log.info("~~~~~~~ Closed Kafka producer ~~~~~~~");
		}
		// Cancel the punctuator when processor is closed (e.g. during a clean shutdown
		// of a Kafka cluster.)
		punctuator.cancel();
		log.info("~~~~~~~ Cancelled punctuator ~~~~~~~");

	}

	public void enforceTtl(long timestamp) {
		try (KeyValueIterator<String, String> iterator = kvStore.all()) {
			log.info("~~~~~~~ Printing all entries ~~~~~~~");
			long count = 0;
			while (iterator.hasNext()) {
				KeyValue<String, String> entry = iterator.next();
				count++;
				ProducerRecord<String, String> tombstoneRecord = new ProducerRecord<>(
						Constants.FOREIGN_EXCHANGE_TOPIC_NAME, entry.key, null);
				log.info("{}. Trying to publish tombstone message for key : {}", count, entry.key);
				if (kafkaProducer == null) {
					kafkaProducer = getKafkaProducer();
				}
				try {
					kafkaProducer.send(tombstoneRecord);
					log.info("{}. Published tombstone message for key : {}", count, entry.key);
				} catch (SerializationException se) {
					log.error("Not able to publish tombstone message reason : {}", se.getMessage());
				}

			}

			if (count == 0) {
				log.info("Empty key value store: {}", kvStore.name());
			}
		}
	}

	private KafkaProducer<String, String> getKafkaProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("schema.registry.url", "http://localhost:8081");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
		log.info("~~~~ Created Kafka Producer : {}", kafkaProducer.metrics());
		return kafkaProducer;
	}

}
