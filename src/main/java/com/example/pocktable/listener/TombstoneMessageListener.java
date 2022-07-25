package com.example.pocktable.listener;

import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TombstoneMessageListener {
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TombstoneMessageListener.class);

	@StreamListener(Sink.INPUT)
	public void in(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] key, @Payload(required = false) String value) {
		// customer is null if a tombstone record
		log.info("Received tombstone message for key : {}", new String(key));

	}
}
