## PoC
* How to create Global KTable.
* How to aggregate Kafka stream.

### Appendix
* How to publish message
* To create kafka topic ``kafka-topics --bootstrap-server localhost:9092 --create --topic foreign_exchange --partitions 4 --replication-factor 1``
* To check created topic ``kafka-topics --bootstrap-server localhost:9092 --describe --topic foreign_exchange``
* To publish messages ``kafka-console-producer --bootstrap-server localhost:9092 --property key.separator=, --property parse.key=true --topic foreign_exchange``
* Then paste ``USD|AUD,1.47`` at end press enter
* Then paste ``USD|SGD,1.40`` at end press enter
* Then paste ``USD|EUR,0.99`` at end press enter
* Then paste ``USD|JPY,138.707`` at end press enter

* kafka-console-consumer --bootstrap-server localhost:9092 --topic foreign_exchange --from-beginning
