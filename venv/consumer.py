from confluent_kafka import Consumer, KafkaException
import json
import pymongo
import logging

# MongoDB connection details
mongo_uri = 'mongodb://localhost:27017/'  # Change as per your MongoDB configuration
db_name = 'twitterdata'  # Adjusted database name
collection_name = 'twitterinfo'  # Adjusted collection name

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

def create_consumer(config):
    return Consumer(config)

def consume_messages(consumer, topics):
    try:
        consumer.subscribe(topics)
        logging.info("Consumer subscribed to topics: %s", topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    logging.warning(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Decode the message
                try:
                    message = msg.value().decode('utf-8')
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    logging.error("Error decoding JSON message: %s", e)
                    continue  # Skip processing invalid messages

                # Map JSON fields to MongoDB schema
                document = {
                    'tweet_id': data.get('tweet_id'),
                    'airline': data.get('airline'),
                    'name': data.get('name'),
                    'text': data.get('text'),
                    'tweet_created': data.get('tweet_created'),
                    'tweet_location': data.get('tweet_location'),
                    'user_timezone': data.get('user_timezone'),
                }
                # Insert data into MongoDB collection
                collection.insert_one(document)
                logging.info(f"Inserted message into MongoDB: {document}")
    finally:
        consumer.close()

# Kafka configuration
config = {
    'bootstrap.servers': 'localhost:9092', # Change as per your server configuration
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

# Creating a Kafka consumer instance
consumer = create_consumer(config)

# Kafka topic to consume from
topic = 'raw_data'

# Consume messages from Kafka topic and insert into MongoDB
consume_messages(consumer, [topic])

# Close MongoDB client
client.close()