from confluent_kafka import Consumer, KafkaError, KafkaException

def consume_data(consumer, topic):
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Message successfully received
                print('Received message: {}'.format(msg.value().decode('utf-8')))
    finally:
        # Close the consumer when done
        consumer.close()

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change as per your Kafka server configuration
    'group.id': 'my_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading at the earliest available message
}

# Creating a Kafka consumer instance
consumer = Consumer(consumer_config)

# Kafka topic to consume from
topic = 'raw_data'

# Consume data from Kafka topic
consume_data(consumer, topic)
