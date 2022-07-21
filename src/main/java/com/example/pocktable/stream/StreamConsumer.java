package com.example.pocktable.stream;

import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.example.pocktable.processor.ForeignExchangeProcessor;
import com.example.pocktable.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class StreamConsumer {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(StreamConsumer.class);



	@SuppressWarnings("rawtypes")
	@Bean
	public Consumer<KStream<String, String>> process() {
		return input -> input.process(new ProcessorSupplier() {

			public Processor get() {
				return new ForeignExchangeProcessor();
			}
		}, Constants.FOREIGN_EXCHANGE_STORE_NAME);
	}

}
