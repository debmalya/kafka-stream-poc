package com.example.pocktable.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.pocktable.util.Constants;

@Configuration
public class StoreConfiguration {
	@Bean
	public StoreBuilder<KeyValueStore<String,String>> myStore() {
	  return Stores.keyValueStoreBuilder(
	        Stores.persistentKeyValueStore(Constants.FOREIGN_EXCHANGE_STORE_NAME), Serdes.String(),
	        Serdes.String());
	}
	
	@Bean
	public StoreBuilder<KeyValueStore<String,Long>> fileRowStore() {
	  return Stores.keyValueStoreBuilder(
	        Stores.persistentKeyValueStore(Constants.FILE_ROW_STORE_NAME), Serdes.String(),
	        Serdes.Long());
	}
}
