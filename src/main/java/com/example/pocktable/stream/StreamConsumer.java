package com.example.pocktable.stream;



import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class StreamConsumer {
	
	private static final org.slf4j.Logger log = 
		    org.slf4j.LoggerFactory.getLogger(StreamConsumer.class);

	@Bean
	public Consumer<KStream<Object,String>> process(){
		return input -> {
			input.foreach((key,value) -> {
				log.info("Key : {} value :{}",key,value);
				
			});
		};
	}
}
