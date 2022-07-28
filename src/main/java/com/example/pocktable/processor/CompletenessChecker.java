package com.example.pocktable.processor;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
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
public class CompletenessChecker implements Processor<String, String, String, Long> {
	private ProcessorContext<String, Long> context;
	private KeyValueStore<String, Long> kvStore;
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CompletenessChecker.class);

	@Override
	public void init(ProcessorContext<String, Long> context) {
		this.context = context;
		kvStore = context.getStateStore(Constants.FILE_ROW_STORE_NAME);
		// schedule a punctuate() method every 1000 milliseconds based on stream-time
		this.context.schedule(Duration.ofMillis(1000), PunctuationType.STREAM_TIME, (timestamp) -> {
			KeyValueIterator<String, Long> iter = this.kvStore.all();
			while (iter.hasNext()) {
				KeyValue<String, Long> entry = iter.next();
				Record<String, Long> record = new Record<>(entry.key, entry.value, System.currentTimeMillis());
				context.forward(record);
			}
			iter.close();

			// commit the current processing progress
			context.commit();
			log.info("fileRowStore commit completed");
		});
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Processor.super.close();
	}

	@Override
	public void process(Record<String, String> record) {
		Headers messageHeader = record.headers();

		String fileName = getHeaderValue(messageHeader, "file.name");
		String fileOffset = getHeaderValue(messageHeader, "file.offset");
		long rowNumber = Long.parseLong(fileOffset);
		log.info("File name : {} row number : {}", fileName, rowNumber);
		if (rowNumber == 1) {
			long totalNumberOfRows = processHeader(record.value());
			kvStore.put(fileName, totalNumberOfRows);
		} else {
			if (isLastRow(fileName, rowNumber)) {
				log.info("Encountered last row of file :{}", fileName);
			}
		}

	}

	private boolean isLastRow(String fileName, long rowNumber) {
		Long totalRows = kvStore.get(fileName);
		if (totalRows != null) {
			return totalRows.longValue() + 1 == rowNumber;
		}

		return false;
	}

	private long processHeader(String headerRow) {
		String[] values = headerRow.split("\\|");
		long totalNumberOfRows = 0;
		if (values.length > 1) {
			totalNumberOfRows = Long.valueOf(values[values.length - 2]);
		}
		return totalNumberOfRows;
	}

	private String getHeaderValue(Headers messageHeader, String headerKey) {
		AtomicReference<String> headerValue = new AtomicReference<>();
		if (Objects.nonNull(headerKey)) {
			Iterable<Header> entries = messageHeader.headers(headerKey);
			entries.forEach(entry -> {
				headerValue.set(new String(entry.value()));
			});
		}
		return headerValue.get();
	}

}
