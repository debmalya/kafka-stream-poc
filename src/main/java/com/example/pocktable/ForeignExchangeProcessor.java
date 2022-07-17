package com.example.pocktable;

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
		});
	}

	@Override
	public void process(Record<String, String> record) {
		kvStore.put(record.key(), record.value());
	}

}
