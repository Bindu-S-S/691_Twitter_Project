from neo4j import GraphDatabase
from confluent_kafka import Producer
import os
import json

# Neo4j connection details
neo4j_uri = "neo4j+s://6b674d80.databases.neo4j.io"  # Replace with your Neo4j URI
neo4j_username = "neo4j"  # Replace with your Neo4j username
neo4j_password = "aYazW7u73HUJz3nYsjXwoi_iQrFQhOyCMtQkpoJo9Bc"  # Replace with your Neo4j password

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Change as per your Kafka server configuration
    # Add any other Kafka configuration parameters here
}

# Cypher query to retrieve data from Neo4j
cypher_query = """
MATCH (n)
RETURN n
LIMIT 50
"""

# Neo4j driver
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))

# Kafka producer
producer = Producer(kafka_config)

def delivery_callback(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}]")

# Retrieve data from Neo4j and send to Kafka topic
with driver.session() as session:
    result = session.run(cypher_query)
    for record in result:
        # Convert Neo4j record to JSON format
        data = dict(record['n'])
        # Send data to Kafka topic
        producer.produce(topic='raw_data', value=json.dumps(data), callback=delivery_callback)

# Wait for all messages to be delivered
producer.flush()

# Close Neo4j driver
driver.close()
