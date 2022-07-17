package com.example.pocktable.processor;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import com.example.pocktable.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForeignExchangeProcessor implements Processor<String, String, String, String> {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForeignExchangeProcessor.class);
	private ProcessorContext<String, String> context;
	private KeyValueStore<String, String> kvStore;

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
	}

	@Override
	public void process(Record<String, String> record) {
		String key = record.key();
		String value = record.value();
		String existingValue = kvStore.get(record.key());
		log.info("Key : {}, existing value : {}", key, existingValue);
		kvStore.put(key, value);
		existingValue = kvStore.get(record.key());
		log.info("Key : {}, new value : {}", key, existingValue);
	}

}
