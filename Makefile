make:
	docker exec -it kafka kafka-topics --create --topic event-logs --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
