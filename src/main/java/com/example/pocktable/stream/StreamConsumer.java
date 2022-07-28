package com.example.pocktable.stream;

import java.util.function.Consumer;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
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

	StreamsBuilder streamBuilder = new StreamsBuilder();
//	= streamBuilder.table("global_foreign_exchange", null)
//	KTable<String, String> foreignExchangeTable;
//	GlobalKTable<String,String>  globalKTable = streamBuilder.globalTable("global_foreign_exchange");
//	Kafka Streams binder-based applications can bind to destinations as KTable or GlobalKTable.
//	GlobalKTable is a special table type, where you get data from all partitions of an input topic,
//	regardless of the instance that it is running. 

	
	public Consumer<GlobalKTable<String, String>> process1() {
		return input -> {
//			streamBuilder.
//			KTable<String, String> table = streamBuilder.
			
			// convert a record stream to a changelog stream.
		};
	}

	@Bean
	@SuppressWarnings("rawtypes")
	public Consumer<KStream<String, String>> process() {
		return input -> input.process(new ProcessorSupplier() {

			public Processor get() {
				return new ForeignExchangeProcessor();
			}
		}, Constants.FOREIGN_EXCHANGE_STORE_NAME);
	}

}
